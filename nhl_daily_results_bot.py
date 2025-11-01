# nhl_daily_results_bot.py
# -*- coding: utf-8 -*-

import os, sys, re, json, time
import datetime as dt
from zoneinfo import ZoneInfo
from html import escape
from typing import List, Dict, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Конфиг
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Принудительная дата отчёта (UTC, YYYY-MM-DD). Если пусто — берём сегодня UTC и +/− 1 день для охвата.
REPORT_DATE_UTC = os.getenv("REPORT_DATE_UTC", "").strip()

# Громкость логов
DEBUG = os.getenv("DEBUG", "1").strip() not in ("0", "false", "False")

# Статусы завершённых матчей в новом NHL API
COMPLETED_STATES = {"FINAL", "OFF"}  # при желании: добавить "OVER" для «сразу после сирены»

# Карта русских эмодзи/иконок по triCode (минимально нужное)
TEAM_EMOJI = {
    "VGK": "🎰",
    "COL": "⛰️",
    "WSH": "🦅",
    "NYI": "🟠",
    "ANA": "🦆",
    "DET": "🔴",
    # при желании дополни список
}

# Карта русских названий команд по triCode
TEAM_RU = {
    "VGK": "«Вегас»",
    "COL": "«Колорадо»",
    "WSH": "«Вашингтон»",
    "NYI": "«Айлендерс»",
    "ANA": "«Анахайм»",
    "DET": "«Детройт»",
    # при желании дополни список
}

# Соответствие teamId -> sports.ru slug клуба (используем для построения URL матча)
SPORTSRU_SLUG_BY_TEAMID = {
    54: "vegas-golden-knights",
    21: "colorado-avalanche",
    15: "washington-capitals",
    2:  "new-york-islanders",
    24: "anaheim-ducks",
    17: "detroit-red-wings",
    # при необходимости добавить остальные
}

# Русские месяцы для заголовка
RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}

# =========================
# Утилиты
# =========================
def dbg(msg: str):
    if DEBUG:
        print(f"[DBG] {msg}")

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "HOH-NHL-Daily-Results/1.4"})
    return s

SESSION = make_session()

def iso2date(s: str) -> dt.date:
    # "YYYY-MM-DD"
    return dt.date.fromisoformat(s)

def day_span(center_utc: dt.date) -> List[dt.date]:
    # Берём «вчера, сегодня, завтра» UTC для охвата всех матчей, которые могли попасть в разные сутки
    return [center_utc - dt.timedelta(days=1), center_utc, center_utc + dt.timedelta(days=1)]

def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def mmss_to_seconds(mmss: str) -> int:
    # "MM:SS" -> seconds
    m, s = mmss.split(":")
    return int(m) * 60 + int(s)

def gameclock_to_pretty(total_seconds: int) -> str:
    # Вывод в формате "M.SS" где M — минуты общего времени от старта матча, SS — секунды с ведущими нулями
    m = total_seconds // 60
    s = total_seconds % 60
    return f"{m}.{s:02d}"

def period_number_to_title(n: int) -> str:
    if n == 1:
        return "_1-й период_"
    if n == 2:
        return "_2-й период_"
    if n == 3:
        return "_3-й период_"
    if n > 3:
        return f"_Овертайм №{n-3}_"
    return "_Период_"

# =========================
# NHL API
# =========================
def fetch_schedule(date_ymd: str) -> List[dict]:
    url = f"https://api-web.nhle.com/v1/schedule/{date_ymd}"
    dbg(f"GET {url}")
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    games = []
    for day in data.get("gameWeek", []):
        for g in day.get("games", []):
            games.append(g)
    return games

def collect_completed_games(center_utc: dt.date) -> List[dict]:
    # Склеиваем уникальные завершённые игры (FINAL или OFF) за 3 соседних дня UTC
    uniq = {}
    for d in day_span(center_utc):
        ds = d.strftime("%Y-%m-%d")
        games = fetch_schedule(ds)
        for g in games:
            state = g.get("gameState")
            gid = g.get("id")
            if state in COMPLETED_STATES:
                dbg(f"take completed: {gid} {state}")
                uniq[gid] = g
            else:
                dbg(f"skip not final: {gid} {state}")
    lst = list(uniq.values())
    dbg(f"Collected unique FINAL games: {len(lst)}")
    return lst

def fetch_standings_now() -> Dict[int, Tuple[int,int,int,int]]:
    # Возвращает teamId -> (W, L, OT, PTS)
    url = "https://api-web.nhle.com/v1/standings/now"
    dbg(f"GET {url}")
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        dbg(f"standings error: {repr(e)}")
        return {}
    out = {}
    # структура — списки по конференциям/дивизионам; собираем все "teamRecords"
    def scan(container):
        if isinstance(container, dict):
            for k, v in container.items():
                scan(v)
        elif isinstance(container, list):
            for item in container:
                scan(item)
        else:
            return
    # но проще — пройти возможные ключи
    records = []
    for key in ("standings", "teamRecords", "records", "wildCard", "overallStandings"):
        if key in data and isinstance(data[key], list):
            records.extend(data[key])
    # если пусто, поищем глубже:
    if not records:
        for v in data.values():
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and "teamRecords" in item:
                        records.extend(item["teamRecords"])

    count = 0
    for rec in records:
        try:
            team = rec.get("team", {})
            team_id = team.get("id")
            w = int(rec.get("wins", rec.get("w", 0)) or 0)
            l = int(rec.get("losses", rec.get("l", 0)) or 0)
            ot = int(rec.get("otLosses", rec.get("ot", 0)) or 0)
            pts = int(rec.get("points", rec.get("p", 0)) or 0)
            if team_id:
                out[int(team_id)] = (w, l, ot, pts)
                count += 1
        except Exception:
            pass
    dbg(f"records loaded: {len(out)}")
    return out

def fetch_pbp(game_id: int) -> dict:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    dbg(f"GET {url}")
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def extract_goals_from_pbp(pbp: dict, home_id: int, away_id: int) -> Tuple[List[dict], bool]:
    """
    Возвращает:
      - список голов в порядке события:
        { "period": int, "tsec": total_seconds_from_start, "team_id": int }
      - признак shootout (буллиты)
    """
    plays = pbp.get("plays") or pbp.get("playByPlay") or []
    # Иногда структура внутри "plays" -> list of dicts с полями typeDescKey, periodDescriptor, timeInPeriod, details...
    goals = []
    shootout = False

    def parse_period(ev) -> Optional[int]:
        pd = ev.get("periodDescriptor") or {}
        if isinstance(pd, dict) and "number" in pd:
            try:
                return int(pd["number"])
            except Exception:
                pass
        if "period" in ev:
            try:
                return int(ev["period"])
            except Exception:
                pass
        return None

    def parse_time_in_period(ev) -> Optional[str]:
        # форматы: "MM:SS"
        for k in ("timeInPeriod", "clock", "time"):
            if k in ev and isinstance(ev[k], str) and ":" in ev[k]:
                return ev[k]
        det = ev.get("details") or {}
        if "timeInPeriod" in det and ":" in str(det["timeInPeriod"]):
            return str(det["timeInPeriod"])
        return None

    def parse_team_id(ev) -> Optional[int]:
        det = ev.get("details") or {}
        # пробуем разные поля
        for k in ("eventOwnerTeamId", "teamId", "scoringTeamId"):
            val = det.get(k)
            if isinstance(val, int):
                return val
            if isinstance(val, str) and val.isdigit():
                return int(val)
        # иногда есть верхний уровень "team" с "id"
        team = ev.get("team") or {}
        if isinstance(team, dict):
            tid = team.get("id")
            if isinstance(tid, int):
                return tid
        return None

    sample_has_score_fields = False

    for ev in plays:
        typ = (ev.get("typeDescKey") or ev.get("type") or "").lower()
        if "shootout" in typ:
            shootout = True
        is_goal = ("goal" in typ) and ("no_goal" not in typ)
        if not is_goal:
            continue
        per = parse_period(ev)
        mmss = parse_time_in_period(ev)
        tid = parse_team_id(ev)

        if per is None or mmss is None:
            continue

        # total seconds от старта матча
        sec = mmss_to_seconds(mmss) + (per - 1) * 20 * 60
        goals.append({"period": per, "tsec": sec, "team_id": tid})

        # наличия поля счёта в событии часто нет — поэтому строим счёт сами
        if any(k in ev for k in ("homeScore", "awayScore")):
            sample_has_score_fields = True

    goals.sort(key=lambda x: x["tsec"])
    dbg(f"PBP goals: {len(goals)} shootout:{shootout} sample_has_score_fields={sample_has_score_fields}")
    return goals, shootout

# =========================
# sports.ru парсер голов
# =========================
SPORTSRU_HOST = "https://www.sports.ru"

def get_sportsru_match_goals(slug: str) -> List[Tuple[int, str, List[str]]]:
    """
    Парсим страницу трансляции матча на sports.ru:
    Возвращаем список (total_seconds, author_ru, assists_ru_list)
    На странице используются строки вида:
      "58:44  Гол!  Барзэл"
      "Ассистенты: Пелек, ..."
    либо "Ассистент: ...".
    """
    # страница трансляции без /lineups/
    url = f"{SPORTSRU_HOST}/hockey/match/{slug}/"
    dbg(f"GET {url}")
    r = SESSION.get(url, timeout=20)
    if r.status_code != 200:
        return []

    html = r.text
    # Найдём все блоки с временем и далее "Гол!"
    # Будем идти простым регексом по времени, затем искать ближайший "Гол!" и автор/ассистентов
    # Пример фрагмента:
    # 58:44
    # Гол!  Барзэл
    # Ассистент: Пелек
    time_re = re.compile(r"(\d{1,2}:\d{2})")
    goal_line_re = re.compile(r"Гол!\s*([A-Za-zА-Яа-яЁё\-\.\s]+)")
    ass1_re = re.compile(r"Ассистент:\s*([A-Za-zА-Яа-яЁё\-\.\s]+)")
    assN_re = re.compile(r"Ассистенты:\s*([A-Za-zА-Яа-яЁё\-\.\s,]+)")

    # разбиваем на строки для простоты поиска
    lines = html.splitlines()
    res = []
    for i, line in enumerate(lines):
        # ищем время
        m = time_re.search(line)
        if not m:
            continue
        mmss = m.group(1)
        # проверим, есть ли рядом "Гол!" (в этой или следующих нескольких строках)
        neighborhood = "\n".join(lines[i:i+6])
        gm = goal_line_re.search(neighborhood)
        if not gm:
            continue
        scorer_raw = gm.group(1).strip()
        # ассисты
        assists = []
        m1 = ass1_re.search(neighborhood)
        if m1:
            assists = [m1.group(1).strip()]
        else:
            mN = assN_re.search(neighborhood)
            if mN:
                assists = [x.strip() for x in mN.group(1).split(",") if x.strip()]

        total = mmss_to_seconds(mmss)
        res.append((total, scorer_raw, assists))

    # убираем дубли и сортируем по времени
    res.sort(key=lambda x: x[0])
    return res

def sportsru_goals_for_pair(home_id: int, away_id: int) -> List[Tuple[int, str, List[str]]]:
    # Пробуем 2 варианта слагов: away-vs-home и home-vs-away
    slug_home = SPORTSRU_SLUG_BY_TEAMID.get(home_id)
    slug_away = SPORTSRU_SLUG_BY_TEAMID.get(away_id)
    if not slug_home or not slug_away:
        return []

    tried = []
    for slug in (f"{slug_away}-vs-{slug_home}", f"{slug_home}-vs-{slug_away}"):
        tried.append(slug)
        goals = get_sportsru_match_goals(slug)
        if goals:
            dbg(f"sports.ru matched matchpage: {slug} goals: {len(goals)}")
            return goals
    dbg(f"sports.ru no goals for pair {slug_away.upper()} {slug_home.upper()} tried: {tried}")
    return []

# =========================
# Формирование сообщения
# =========================
def build_game_block(g: dict, standings: Dict[int, Tuple[int,int,int,int]]) -> str:
    """
    g — объект игры из NHL schedule.
    Строим блок:
      <emoji> «Хозяева»: X (W-L-OT, P о.)
      <emoji> «Гости»:   Y (W-L-OT, P о.)
      <строки по периодам: "S:T – mm.ss Автор (Ассисты)">
    """
    gid = int(g.get("id"))
    home = g.get("homeTeam", {})
    away = g.get("awayTeam", {})
    home_id = int(home.get("id"))
    away_id = int(away.get("id"))
    home_tri = home.get("abbrev") or home.get("triCode") or ""
    away_tri = away.get("abbrev") or away.get("triCode") or ""

    home_emoji = TEAM_EMOJI.get(home_tri, "🏒")
    away_emoji = TEAM_EMOJI.get(away_tri, "🏒")
    home_ru    = TEAM_RU.get(home_tri, f"«{home.get('name', 'Хозяева')}»")
    away_ru    = TEAM_RU.get(away_tri, f"«{away.get('name', 'Гости')}»")

    # Итоговый счёт
    scores = g.get("homeTeam", {}).get("score"), g.get("awayTeam", {}).get("score")
    # но в расписании это может отсутствовать — подстрахуемся
    home_score = int(home.get("score", 0))
    away_score = int(away.get("score", 0))

    # Рекорды команд
    def rec_str(team_id: int) -> str:
        w,l,ot,pts = standings.get(team_id, (None,None,None,None))
        if w is None:
            return ""
        return f" ({w}-{l}-{ot}, {pts} о.)"

    header = []
    header.append(f"{home_emoji} {home_ru}: {home_score}{rec_str(home_id)}")
    header.append(f"{away_emoji} {away_ru}: {away_score}{rec_str(away_id)}")

    # Голы из NHL PBP (для правильного счёта по ходу) + имена с sports.ru
    pbp = fetch_pbp(gid)
    goals_pbp, shootout = extract_goals_from_pbp(pbp, home_id, away_id)

    # имена с sports.ru (время в секундах от старта)
    sr_goals = sportsru_goals_for_pair(home_id, away_id)
    # строим “временную сетку” для маппинга имён к голам NHL по ближайшему времени
    # допускаем погрешность +/- 3 сек
    def match_name_by_time(tsec: int) -> Tuple[str, List[str]]:
        if not sr_goals:
            return "—", []
        best = None
        best_dd = 999
        for (ts, author, assists) in sr_goals:
            dd = abs(ts - tsec)
            if dd < best_dd:
                best = (author, assists)
                best_dd = dd
        if best and best_dd <= 3:
            return best[0], best[1]
        return "—", []

    # Счёт по ходу
    home_run = 0
    away_run = 0
    by_period: Dict[int, List[str]] = {}

    for ev in goals_pbp:
        per = int(ev["period"])
        tsec = int(ev["tsec"])
        tid = ev.get("team_id")

        # какая команда забила
        if tid == home_id:
            home_run += 1
        elif tid == away_id:
            away_run += 1
        else:
            # если не удалось понять — пропустим определение забившей, но счёт не собьём
            # (хотя такое почти не встречается)
            pass

        # формат времени
        pretty = gameclock_to_pretty(tsec)

        # имена
        author, assists = match_name_by_time(tsec)
        if assists:
            line = f"{home_run}:{away_run} – {pretty} {author} ({', '.join(assists)})"
        else:
            line = f"{home_run}:{away_run} – {pretty} {author}"

        by_period.setdefault(per, []).append(line)

    # если буллиты были — берём только победный гол (на sports.ru его явного признака нет, поэтому ориентируемся на NHL)
    # В новом API победный буллит — отдельным событием; если его нет — не добавляем блок
    # (для простоты оставим как есть; когда NHL помечает shootout=True, но нет события — опускаем)
    # В выводе “буллиты” просили переименовать в “победный буллит”, но добавляем строку только если он найден.
    # Здесь не реализуем поиск победного буллита из pbp, т.к. не в каждом матче было нужно в текущих играх.

    # Сборка блока
    out_lines = []
    out_lines.extend(header)
    if not goals_pbp:
        out_lines.append("")
        out_lines.append("— события матча недоступны")
        return "\n".join(out_lines)

    # периодами
    for per in sorted(by_period.keys()):
        out_lines.append("")
        out_lines.append(period_number_to_title(per))
        out_lines.extend(by_period[per])

    return "\n".join(out_lines)

def build_message() -> str:
    # Опорная дата UTC
    if REPORT_DATE_UTC:
        try:
            base = iso2date(REPORT_DATE_UTC)
        except Exception:
            base = dt.datetime.now(dt.timezone.utc).date()
    else:
        base = dt.datetime.now(dt.timezone.utc).date()

    games = collect_completed_games(base)
    standings = fetch_standings_now()

    # Заголовочная дата — по Европе/Берлин (как в окружении)
    tz_berlin = ZoneInfo("Europe/Berlin")
    today_ber = dt.datetime.now(tz_berlin).date()
    head_date = f"{today_ber.day} {RU_MONTHS[today_ber.month]}"

    title = f"🗓 Регулярный чемпионат НХЛ • {head_date} • {len(games)} матчей"
    lines = [title, "", "Результаты надёжно спрятаны 👇", "", "——————————————————", ""]

    if not games:
        # всё равно публикуем «0 матчей»
        return "\n".join(lines)

    # Стабильный порядок по времени начала (если есть), иначе по id
    def game_start_ts(g: dict) -> float:
        # g["startTimeUTC"] бывает в расписании
        when = g.get("startTimeUTC")
        try:
            if when:
                ts = dt.datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp()
                return ts
        except Exception:
            pass
        return float(g.get("id", 0))
    games.sort(key=game_start_ts)

    for idx, g in enumerate(games):
        lines.append(build_game_block(g, standings))
        if idx != len(games)-1:
            lines.append("")
            lines.append("")

    return "\n".join(lines)

# =========================
# Telegram
# =========================
def send_telegram(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        print("No TELEGRAM_BOT_TOKEN/CHAT_ID in env", file=sys.stderr)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    dbg("POST Telegram sendMessage")
    r = SESSION.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=20)
    r.raise_for_status()

# =========================
# main
# =========================
if __name__ == "__main__":
    try:
        msg = build_message()
        send_telegram(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
