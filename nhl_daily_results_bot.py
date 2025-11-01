# nhl_daily_results_bot.py
# -*- coding: utf-8 -*-
import os, sys, re, json, time
import datetime as dt
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# =========================
# Конфиг
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
FORCE_MSK_DATE = os.getenv("REPORT_DATE_MSK", "").strip()  # YYYY-MM-DD (опционально для ручного прогона)
DEBUG = True

MSK = ZoneInfo("Europe/Moscow")
PT  = ZoneInfo("America/Los_Angeles")
ET  = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# Эмодзи команд (минимально нужные сейчас)
TEAM_EMOJI = {
    "VGK": "🎰",  # Vegas Golden Knights
    "COL": "⛰️",  # Colorado Avalanche
    "WSH": "🦅",  # Washington Capitals
    "NYI": "🟠",  # New York Islanders
    "ANA": "🦆",  # Anaheim Ducks
    "DET": "🔴",  # Detroit Red Wings
}
# Русские названия команд
TEAM_RU = {
    "VGK": "«Вегас»",
    "COL": "«Колорадо»",
    "WSH": "«Вашингтон»",
    "NYI": "«Айлендерс»",
    "ANA": "«Анахайм»",
    "DET": "«Детройт»",
}

# Сопоставление названий для sports.ru слага
SPORTS_SLUG = {
    "VGK": "vegas-golden-knights",
    "COL": "colorado-avalanche",
    "WSH": "washington-capitals",
    "NYI": "new-york-islanders",
    "ANA": "anaheim-ducks",
    "DET": "detroit-red-wings",
    # при необходимости добавишь остальные 26 команд
}

# =========================
# Утилиты
# =========================
def dbg(*a):
    if DEBUG:
        print("[DBG]", *a, flush=True)

def make_session():
    s = requests.Session()
    r = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": "HOH NHL Daily Results/1.0"})
    return s

S = make_session()

def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_iso_z(s: str) -> dt.datetime:
    # '2025-11-01T05:30:00Z'
    return dt.datetime.fromisoformat(s.replace("Z","+00:00"))

def sec_to_mmss(sec: int) -> str:
    m = sec // 60
    s = sec % 60
    return f"{m}.{s:02d}"

def period_from_abs(sec: int) -> int:
    # 20-минутные периоды
    if sec < 20*60: return 1
    if sec < 40*60: return 2
    if sec < 60*60: return 3
    # дальше ОТ
    ot_index = (sec - 60*60) // (5*60) + 1
    return 3 + ot_index

def period_caption(idx: int) -> str:
    if idx == 1: return "_1-й период_"
    if idx == 2: return "_2-й период_"
    if idx == 3: return "_3-й период_"
    return f"_Овертайм №{idx-3}_"

# =========================
# Шаг 1: Определим отчётную МСК-дату и набор игр
# =========================
def resolve_report_date_msk() -> dt.date:
    if FORCE_MSK_DATE:
        try:
            d = dt.date.fromisoformat(FORCE_MSK_DATE)
            dbg("FORCE_MSK_DATE =", d)
            return d
        except Exception:
            print("ERROR: REPORT_DATE_MSK must be YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    # по умолчанию: сегодняшняя дата по МСК
    now_msk = dt.datetime.now(MSK)
    return now_msk.date()

def fetch_schedule_dates_for_msk(report_d: dt.date):
    """
    Логика набора матчей:
    - Все игры с МСК-датой == report_d
    - Плюс игры с МСК-датой == report_d - 1, которые стартовали после 15:00 МСК
    """
    msk_start = dt.datetime(report_d.year, report_d.month, report_d.day, 0, 0, tzinfo=MSK)
    msk_end   = dt.datetime(report_d.year, report_d.month, report_d.day, 23, 59, tzinfo=MSK)
    prev_msk  = msk_start - dt.timedelta(days=1)
    border    = dt.time(15, 0)  # 15:00 МСК

    # конвертируем в UTC и получим набор UTC дат для запроса NHL schedule
    utc_dates = sorted({ msk_start.astimezone(UTC).date(),
                         msk_end.astimezone(UTC).date(),
                         prev_msk.astimezone(UTC).date() })
    return utc_dates, border

def load_nhl_schedule(utc_date: dt.date):
    url = f"https://api-web.nhle.com/v1/schedule/{ymd(utc_date)}"
    dbg("GET", url)
    r = S.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def collect_games_for_msk_day(report_d: dt.date):
    utc_dates, border = fetch_schedule_dates_for_msk(report_d)
    games = []
    for d in utc_dates:
        j = load_nhl_schedule(d)
        for g in (j.get("gameWeek") or []):
            for day in (g.get("games") or []):
                # по странной структуре иногда gameWeek -> days -> games
                pass
        # упрощённо в более свежих версиях есть ключ "gameWeek", в других — "gameWeek":[{"games":[{...}]}]
        week = j.get("gameWeek") or []
        for day in week:
            for ev in (day.get("games") or []):
                # время старта в UTC
                start_utc = parse_iso_z(ev.get("startTimeUTC"))
                start_msk = start_utc.astimezone(MSK)
                msk_date  = start_msk.date()
                # фильтр по правилам
                take = False
                if msk_date == report_d:
                    take = True
                elif msk_date == (report_d - dt.timedelta(days=1)) and start_msk.time() >= border:
                    take = True
                if not take:
                    continue
                # брать только завершённые
                state = (ev.get("gameState") or "").upper()
                if state not in ("FINAL", "OFF"):
                    dbg("skip not final:", ev.get("gameId"), ev.get("awayTeam",{}).get("abbrev"), ev.get("homeTeam",{}).get("abbrev"), state)
                    continue
                games.append(ev)
    dbg("Collected games:", len(games))
    return games

# =========================
# Шаг 2: PBP из NHL + счёт нарастающим итогом
# =========================
def load_pbp(game_id: int):
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    dbg("GET", url)
    r = S.get(url, timeout=20); r.raise_for_status()
    return r.json()

def extract_goal_events(pbp_json, home_abbr, away_abbr):
    """
    Вернём список событий голов:
      [{"abs_sec": 1234, "period": 1.., "team": "HOME"/"AWAY", "tri": "WSH"/"NYI", "shootout":False}, ...]
    """
    out = []
    for p in pbp_json.get("plays", []):
        t = (p.get("typeDescKey") or "").lower()
        if t != "goal": 
            continue
        # время внутри периода в секундах
        clock = p.get("timeInPeriod") or "00:00"
        mm, ss = [int(x) for x in clock.split(":")]
        per = int(p.get("periodDescriptor", {}).get("number") or 0)
        abs_sec = (per-1)*20*60 + mm*60 + ss
        # чья команда
        tri = (p.get("team", {}) or {}).get("abbrev")
        who = "HOME" if tri == home_abbr else "AWAY"
        out.append({
            "abs_sec": abs_sec,
            "period": per,
            "team": who,
            "tri": tri,
            "shootout": False
        })
    # отметим буллиты (если были)
    if (pbp_json.get("periodDescriptor", {}) or {}).get("periodType") == "SO":
        # в некоторых версиях PBP отдельно помечаются SOG/SO shots, но проще:
        # финальный счёт уже у события goal не меняется; победный буллит добавим отдельно через summary
        pass
    out.sort(key=lambda x: x["abs_sec"])
    # теперь посчитаем счёт
    h = a = 0
    for e in out:
        if e["team"] == "HOME": h += 1
        else: a += 1
        e["score"] = f"{h}:{a}" if e["team"]=="HOME" else f"{a}:{h}"
        e["home_score"] = h
        e["away_score"] = a
    return out

def detect_shootout_winner(pbp_json):
    # попробуем найти победный буллит через gameCenter metadata
    try:
        gd = pbp_json.get("gameState", {})
    except:
        gd = {}
    # fallback: в summary есть shootoutData
    try:
        so = (pbp_json.get("summary", {}) or {}).get("shootout") or {}
        # в некоторых ответах есть winnerName и т.п., оставим без имени — имя возьмём со Sports.ru
        if so.get("isShootout"):
            return True
    except:
        pass
    return False

# =========================
# Шаг 3: Sports.ru — парсим «/lineups/»
# =========================
def sports_slug_for_pair(away_tri, home_tri):
    a = SPORTS_SLUG.get(away_tri)
    h = SPORTS_SLUG.get(home_tri)
    if not a or not h:
        return None, None
    # Пробуем оба порядка
    return f"{a}-vs-{h}", f"{h}-vs-{a}"

def fetch_sports_lineups(slug):
    url = f"https://www.sports.ru/hockey/match/{slug}/lineups/"
    dbg("GET", url)
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return None
    return r.text

GOAL_LINE_RE = re.compile(r"(\d{1,2}:\d{2})\s*([А-ЯЁA-Zа-яё\-ʼ’\.\s]+?)(?:\s*\(([^)]+)\))?(?:\s|$)")

def parse_sports_lineups_goals(html_text):
    """
    Возвращает список событий с русскими фамилиями:
      [{"abs_sec": 1488, "scorer_ru":"Уилсон", "assists_ru":"Чикран, Рой"}]
    """
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text("\n", strip=True)
    # На странице «Составы» дублируются блоки по командам — после get_text() идут нужные пункты со временем.
    # Вытащим все совпадения и дедуплицируем по (time, scorer_ru).
    seen = set()
    results = []
    for m in GOAL_LINE_RE.finditer(text):
        tmm = m.group(1)
        who = (m.group(2) or "").strip()
        ass = (m.group(3) or "").strip()
        # фильтруем мусор: пропустим строки, где нет кириллицы в фамилии автора
        if not re.search(r"[А-ЯЁа-яё]", who):
            continue
        # нормализуем автора: часто на странице уже одна фамилия
        who = re.sub(r"\s+", " ", who)
        # в ассистентах оставляем как есть (кириллица), уберём лишние пробелы
        ass = re.sub(r"\s+", " ", ass)
        mm, ss = [int(x) for x in tmm.split(":")]
        abs_sec = mm*60 + ss
        key = (abs_sec, who)
        if key in seen:
            continue
        seen.add(key)
        results.append({"abs_sec": abs_sec, "scorer_ru": who, "assists_ru": ass})
    results.sort(key=lambda x: x["abs_sec"])
    dbg("sports.ru goals parsed:", len(results))
    return results

def attach_ru_names_to_pbp(pbp_events, ru_events):
    """
    Матчим по ближайшему времени (±2 сек). Если совпадение не найдено — оставляем пусто (и покажем «—»).
    """
    j = 0
    for e in pbp_events:
        best = None
        best_diff = 999
        while j < len(ru_events) and ru_events[j]["abs_sec"] <= e["abs_sec"] + 2:
            diff = abs(ru_events[j]["abs_sec"] - e["abs_sec"])
            if diff < best_diff:
                best = ru_events[j]; best_diff = diff
            j += 1
        if not best:
            # попробуем линейный поиск вокруг
            for r in ru_events:
                d = abs(r["abs_sec"] - e["abs_sec"])
                if d < best_diff:
                    best, best_diff = r, d
        if best and best_diff <= 2:
            e["scorer_ru"] = best["scorer_ru"]
            e["assists_ru"] = best["assists_ru"]
        else:
            e["scorer_ru"] = ""
            e["assists_ru"] = ""

# =========================
# Шаг 4: Формирование текста
# =========================
def fmt_team_line(tri_home, tri_away, home_score, away_score):
    # Победителя выделяем жирным
    home_bold = away_bold = False
    if home_score > away_score: home_bold = True
    elif away_score > home_score: away_bold = True
    eh = TEAM_EMOJI.get(tri_home, "🏒")
    ea = TEAM_EMOJI.get(tri_away, "🏒")
    th = TEAM_RU.get(tri_home, tri_home)
    ta = TEAM_RU.get(tri_away, tri_away)
    sh = f"**{home_score}**" if home_bold else f"{home_score}"
    sa = f"**{away_score}**" if away_bold else f"{away_score}"
    return f"{eh} {th}: {sh}\n{ea} {ta}: {sa}\n"

def build_match_block(game_ev, ru_slug_chosen, pbp_events, has_shootout):
    tri_home = (game_ev.get("homeTeam") or {}).get("abbrev")
    tri_away = (game_ev.get("awayTeam") or {}).get("abbrev")
    home_score = (game_ev.get("homeTeam") or {}).get("score")
    away_score = (game_ev.get("awayTeam") or {}).get("score")

    lines = []
    lines.append(fmt_team_line(tri_home, tri_away, home_score, away_score))

    if not pbp_events:
        lines.append("— события матча недоступны\n")
        return "\n".join(lines)

    # Разбивка по периодам/ОТ
    cur_period = None
    for e in pbp_events:
        p = period_from_abs(e["abs_sec"])
        if p != cur_period:
            cur_period = p
            lines.append(period_caption(p))
        # время
        tmm = sec_to_mmss(e["abs_sec"])
        # счёт после гола: нужен формат "X:Y" в "1:0 – 4.38 Фамилия (ассисты)"
        # кто первый в счёте — берём score из события: он уже для команды-автора "X:Y"
        score = e["score"]
        who = e.get("scorer_ru") or "—"
        ass = e.get("assists_ru") or ""
        a_str = f" ({ass})" if ass else ""
        lines.append(f"{score} – {tmm} {who}{a_str}")

    # Победный буллит (если был)
    if has_shootout:
        lines.append("Победный буллит — (см. Sports.ru)")
        # Нужен автор победного — на Sports.ru в «Статистика матча»/«Трансляция» отмечают, но на /lineups/ не всегда.
        # Чтобы не дергать ещё одну страницу сейчас, краткая пометка. При желании добавим отдельный парсер.

    lines.append("")  # пустая строка после матча
    return "\n".join(lines)

# =========================
# Шаг 5: Основной поток
# =========================
def build_report():
    report_d = resolve_report_date_msk()
    games = collect_games_for_msk_day(report_d)
    if not games:
        header = f"🗓 Регулярный чемпионат НХЛ • {report_d.strftime('%-d %B')} • 0 матчей\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————"
        return header

    header = f"🗓 Регулярный чемпионат НХЛ • {report_d.strftime('%-d %B')} • {len(games)} матчей\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n"
    blocks = [header]

    for ev in games:
        gid = ev.get("gameId")
        tri_home = (ev.get("homeTeam") or {}).get("abbrev")
        tri_away = (ev.get("awayTeam") or {}).get("abbrev")
        dbg(f"Game {gid}: {tri_home} vs {tri_away}")

        # NHL PBP
        try:
            pbp = load_pbp(gid)
        except Exception as e:
            dbg("PBP error:", repr(e))
            blocks.append(fmt_team_line(tri_home, tri_away,
                                        (ev.get("homeTeam") or {}).get("score"),
                                        (ev.get("awayTeam") or {}).get("score")))
            blocks.append("— события матча недоступны\n")
            continue

        # Список голов и счёт
        goals = extract_goal_events(pbp, tri_home, tri_away)
        has_shootout = detect_shootout_winner(pbp)

        # Sports.ru /lineups/
        slug_a, slug_b = sports_slug_for_pair(tri_away, tri_home)
        ru_slug = None
        ru_events = []
        for slug in (slug_a, slug_b):
            if not slug: 
                continue
            try:
                html = fetch_sports_lineups(slug)
                if not html:
                    continue
                tmp = parse_sports_lineups_goals(html)
                if tmp:
                    ru_slug = slug
                    ru_events = tmp
                    break
            except Exception as e:
                dbg("Sports.ru parse error for", slug, ":", repr(e))
                continue

        if not goals:
            dbg("No NHL goals parsed")
        else:
            dbg("NHL goals:", len(goals))
        if ru_slug:
            dbg("Matched sports.ru lineups:", ru_slug, "goals:", len(ru_events))
        else:
            dbg("sports.ru lineups not found for", tri_away, tri_home)

        # Сопоставляем имена
        if goals and ru_events:
            attach_ru_names_to_pbp(goals, ru_events)

        # Блок матча
        blocks.append(build_match_block(ev, ru_slug, goals, has_shootout))

    return "\n".join(blocks).rstrip()

def send_telegram(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        print("No TELEGRAM_BOT_TOKEN/CHAT_ID env", file=sys.stderr)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    dbg("POST Telegram sendMessage")
    r = S.post(url, json=payload, timeout=25)
    r.raise_for_status()
    dbg("Telegram OK")

if __name__ == "__main__":
    try:
        msg = build_report()
        print("\n" + msg + "\n")
        send_telegram(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
