#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram
Голы/ассисты — из api-web.nhle.com (официальный play-by-play),
формат имён — «И. РусскаяФамилия» через sports.ru (fallback — «I. Lastname»),
время голов — в формате MM.SS по ходу ВСЕГО матча (напр., 1.15, 21.45, 45.59, 68.15).
"""

import os
import sys
import re
import time
from datetime import date, datetime, timedelta
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ===================== ПАРАМЕТРЫ =====================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

API_NHL = "https://api-web.nhle.com/v1"
SPORTS_RU_SEARCH = "https://www.sports.ru/search/?q="

RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

# аббревиатура → (русское короткое, эмодзи)
TEAM_RU = {
    "ANA": ("Анахайм", "🦆"), "ARI": ("Аризона", "🤠"), "BOS": ("Бостон", "🐻"), "BUF": ("Баффало", "🦬"),
    "CGY": ("Калгари", "🔥"), "CAR": ("Каролина", "🌪️"), "COL": ("Колорадо", "⛰️"), "CBJ": ("Коламбус", "💣"),
    "DAL": ("Даллас", "⭐"), "DET": ("Детройт", "🔴"), "EDM": ("Эдмонтон", "🛢️"), "FLA": ("Флорида", "🐆"),
    "LAK": ("Лос-Анджелес", "👑"), "MIN": ("Миннесота", "🌲"), "MTL": ("Монреаль", "🇨🇦"), "NSH": ("Нэшвилл", "🐯"),
    "NJD": ("Нью-Джерси", "😈"), "NYI": ("Айлендерс", "🟠"), "NYR": ("Рейнджерс", "🗽"), "OTT": ("Оттава", "🛡"),
    "PHI": ("Филадельфия", "🛩"), "PIT": ("Питтсбург", "🐧"), "SJS": ("Сан-Хосе", "🦈"), "SEA": ("Сиэтл", "🦑"),
    "STL": ("Сент-Луис", "🎵"), "TBL": ("Тампа-Бэй", "⚡"), "TOR": ("Торонто", "🍁"), "VAN": ("Ванкувер", "🐳"),
    "VGK": ("Вегас", "🎰"), "WSH": ("Вашингтон", "🦅"), "WPG": ("Виннипег", "✈️"), "UTA": ("Юта", "🦣"),
    "CHI": ("Чикаго", "🦅"),
}

def log(*a): print(*a, file=sys.stderr)

# HTTP с ретраями
def make_session():
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"], raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-DailyResultsBot/2.3 (+api-web.nhle.com; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

# ===================== ДАТА ИГРОВОГО ДНЯ =====================

def pick_report_date() -> date:
    """
    «Североамериканская» логика: если в Нью-Йорке раньше 07:00 — берём вчера; иначе сегодня (ET).
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 7 else now_et.date()

# ===================== УТИЛИТЫ ВРЕМЕНИ =====================

def parse_time_to_sec_in_period(t: str) -> int:
    """ 'MM:SS' или 'M:SS' → секунды в периоде. """
    try:
        m, s = str(t).split(":")
        return int(m)*60 + int(s)
    except Exception:
        try:
            return int(t)*60
        except Exception:
            return 0

def period_to_index(period_type: str, number: int) -> int:
    """REG: 1..3; OT: 4; SO: 5 (строго после игры)."""
    pt = (period_type or "").upper()
    if pt == "OT": return 4
    if pt == "SO": return 5
    return max(1, int(number or 1))

def abs_seconds(period_index: int, sec_in_period: int) -> int:
    """
    Абсолютные секунды с начала матча, если 20-минутные периоды:
    (period-1)*1200 + sec. Для SO считаем базу 65:00 (3900 сек) + шаг 1 сек.
    """
    if period_index == 5:  # SO
        return 65*60 + sec_in_period  # 65:00 + псевдо-секунды попыток
    if period_index >= 4:  # OT
        return 60*60 + sec_in_period
    return (period_index - 1)*20*60 + sec_in_period

def fmt_mm_ss(total_seconds: int) -> str:
    mm = total_seconds // 60
    ss = total_seconds % 60
    return f"{mm}.{ss:02d}"

# ===================== РАСПИСАНИЕ / ФИНАЛЫ =====================

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

def fetch_games_for_date(day: date) -> list[dict]:
    """
    Возвращает список завершённых матчей на день:
    [{gameId, homeAbbrev, awayAbbrev, homeScore, awayScore, periodType}]
    Проверяем несколько эндпоинтов.
    """
    out = []

    # 1) /v1/schedule/{date}
    j = _get_json(f"{API_NHL}/schedule/{day.isoformat()}")
    for bucket in j.get("gameWeek", []):
        if bucket.get("date") != day.isoformat():
            continue
        for g in bucket.get("games", []):
            st = str(g.get("gameState", "")).upper()
            if st not in {"OFF", "FINAL"}:
                continue
            game_id = g.get("id") or g.get("gameId")
            hm = g.get("homeTeam", {}) or {}
            aw = g.get("awayTeam", {}) or {}
            out.append({
                "gameId": int(game_id),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": (g.get("periodDescriptor") or {}).get("periodType") or "",
            })

    # 2) /v1/score/{date}
    if not out:
        j = _get_json(f"{API_NHL}/score/{day.isoformat()}")
        for g in j.get("games", []):
            st = str(g.get("gameState", "")).upper()
            if st not in {"OFF", "FINAL"}:
                continue
            game_id = g.get("id") or g.get("gameId")
            hm = g.get("homeTeam", {}) or {}
            aw = g.get("awayTeam", {}) or {}
            pd = g.get("periodDescriptor", {}) or {}
            out.append({
                "gameId": int(game_id),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": pd.get("periodType") or "",
            })

    # 3) /v1/scoreboard/{date}
    if not out:
        j = _get_json(f"{API_NHL}/scoreboard/{day.isoformat()}")
        for g in j.get("games", []):
            st = str(g.get("gameState", "")).upper()
            if st not in {"OFF", "FINAL"}:
                continue
            game_id = g.get("id") or g.get("gameId")
            hm = g.get("homeTeam", {}) or {}
            aw = g.get("awayTeam", {}) or {}
            pd = g.get("periodDescriptor", {}) or {}
            out.append({
                "gameId": int(game_id),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": pd.get("periodType") or "",
            })

    return out

# ===================== BOX + PLAY-BY-PLAY =====================

_en_name_cache: dict[int, tuple[str, str]] = {}   # playerId -> (first,last)
_display_cache: dict[int, str]           = {}     # playerId -> красивый латинский вид, если дан (e.g., "C. McDavid")

def _extract_names_from_player_obj(p: dict) -> tuple[str, str, str]:
    """
    Универсальный парсер разных форматов записи имени игрока.
    Возвращает (first, last, display) — display можно использовать как латинский фоллбэк.
    """
    first = ""
    last  = ""
    display = ""

    # 1) явные поля firstName/lastName
    fn = p.get("firstName")
    ln = p.get("lastName")
    if isinstance(fn, dict): fn = fn.get("default") or ""
    if isinstance(ln, dict): ln = ln.get("default") or ""
    if fn: first = str(fn).strip()
    if ln: last  = str(ln).strip()

    # 2) возможные поля с готовым отображением
    for key in ("firstInitialLastName", "playerName", "name", "playerNameWithNumber", "fullName"):
        val = p.get(key)
        if isinstance(val, dict):
            val = val.get("default") or ""
        if val and not display:
            display = str(val).strip()

    # 3) если нет first/last, но есть display → попробуем извлечь
    if (not first or not last) and display:
        # ожидаем варианты: "Connor McDavid" или "C. McDavid" или "C. McDavid #97"
        disp = display.replace("#", " ").strip()
        parts = [x for x in re.split(r"\s+", disp) if x and x != "-"]
        if len(parts) >= 2:
            # последний токен считаем фамилией
            last = last or parts[-1]
            # из первого токена извлечём первую букву как first-initial
            first = first or parts[0].replace(".", "").strip()

    return first, last, display

def fetch_box_map(game_id: int) -> dict[int, dict]:
    """
    Карта playerId -> {firstName, lastName} + заполнение кэшей имён.
    """
    url = f"{API_NHL}/gamecenter/{game_id}/boxscore"
    r = S.get(url, timeout=25); r.raise_for_status()
    data = r.json()
    out: dict[int, dict] = {}

    def eat(team_block: dict):
        for group in ("forwards", "defense", "goalies"):
            for p in team_block.get(group, []) or []:
                pid = p.get("playerId")
                if not pid:
                    continue
                pid = int(pid)
                f, l, d = _extract_names_from_player_obj(p)
                out[pid] = {"firstName": f, "lastName": l}
                if f or l:
                    _en_name_cache[pid] = (f, l)
                if d:
                    _display_cache[pid] = d

    stats = data.get("playerByGameStats", {}) or {}
    eat(stats.get("homeTeam", {}) or {})
    eat(stats.get("awayTeam", {}) or {})
    return out

def fetch_player_en_name(pid: int) -> tuple[str, str]:
    """
    Возвращает (first,last) по playerId, кэширует. Используем landing как запасной вариант.
    """
    if pid in _en_name_cache:
        return _en_name_cache[pid]

    try:
        url = f"{API_NHL}/player/{pid}/landing"
        r = S.get(url, timeout=20)
        if r.status_code == 200:
            j = r.json()
            fn = j.get("firstName"); ln = j.get("lastName")
            if isinstance(fn, dict): fn = fn.get("default") or ""
            if isinstance(ln, dict): ln = ln.get("default") or ""
            fn = (fn or "").strip()
            ln = (ln or "").strip()
            if fn or ln:
                _en_name_cache[pid] = (fn, ln)
                return fn, ln
    except Exception as e:
        log("[landing] fail", pid, e)

    _en_name_cache[pid] = ("", "")
    return "", ""

def fetch_goals(game_id: int) -> list[dict]:
    """
    Возвращает голы с строгой хронологией и абсолютным временем:
    [{period:int, sec:int, totsec:int, minute:int, home:int, away:int,
      scorerId:int|None, a1:int|None, a2:int|None, periodType:str, playersInvolved:list}]
    """
    url = f"{API_NHL}/gamecenter/{game_id}/play-by-play"
    r = S.get(url, timeout=25); r.raise_for_status()
    data = r.json()
    plays = data.get("plays", []) or []
    goals = []

    for ev in plays:
        if ev.get("typeDescKey") != "goal":
            continue
        det = ev.get("details", {}) or {}
        pd = ev.get("periodDescriptor", {}) or {}

        t = str(ev.get("timeInPeriod") or det.get("timeInPeriod") or "0:00")
        sec_in = parse_time_to_sec_in_period(t)
        pidx = period_to_index(pd.get("periodType"), pd.get("number"))
        totsec = abs_seconds(pidx, sec_in)

        hs = int(det.get("homeScore", 0))
        as_ = int(det.get("awayScore", 0))

        sid = det.get("scoringPlayerId")
        a1 = det.get("assist1PlayerId") or det.get("secondaryAssistPlayerId")
        a2 = det.get("assist2PlayerId") or det.get("tertiaryAssistPlayerId")

        players = ev.get("playersInvolved") or []

        # запасной вариант из playersInvolved
        if not sid and players:
            for p in players:
                tpe = (p.get("playerType") or "").lower()
                if tpe == "scorer":
                    sid = p.get("playerId")
                elif tpe == "assist":
                    if not a1: a1 = p.get("playerId")
                    elif not a2: a2 = p.get("playerId")

        goals.append({
            "period": pidx, "sec": sec_in, "totsec": totsec, "minute": sec_in // 60,
            "home": hs, "away": as_,
            "scorerId": int(sid) if sid else None,
            "a1": int(a1) if a1 else None,
            "a2": int(a2) if a2 else None,
            "periodType": (pd.get("periodType") or "").upper(),
            "playersInvolved": players,
        })

    # строгая хронология
    goals.sort(key=lambda x: (x["period"], x["sec"]))
    return goals

# ===================== РУССКИЕ ИМЕНА (ИНИЦИАЛ + ФАМИЛИЯ) =====================

_ru_name_cache: dict[str, str] = {}  # "Connor McDavid" -> "К. Макдэвид" или "C. McDavid"

def _ru_initial_surname_from_profile(url: str) -> str | None:
    """
    Открываем профиль игрока на sports.ru и берём заголовок h1 → "Имя Фамилия".
    Возвращаем "И. Фамилия".
    """
    try:
        r = S.get(url, timeout=25)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1", "h2"])
        if not h:
            return None
        full = " ".join(h.get_text(" ", strip=True).split())
        parts = [p for p in re.split(r"\s+", full) if p]
        if len(parts) >= 2:
            ini = parts[0][0] + "."
            last = parts[-1]
            return f"{ini} {last}"
    except Exception as e:
        log("[sports.ru] profile parse fail:", e)
    return None

def ru_initial_surname_by_en(first: str, last: str, display: str | None = None) -> str:
    """
    «И. РусскаяФамилия» (или «I. Lastname», если профиль не найден).
    Если display уже типа "C. McDavid" — можно вернуть его сразу как fallback.
    """
    # если есть готовая красивая латиница
    if display:
        disp = display.replace("#", " ").strip()
        # фильтруем очень странные штуки
        if 2 <= len(disp) <= 40 and any(c.isalpha() for c in disp):
            # попробуем всё равно получить русскую фамилию — но если не выйдет, вернём display
            pass

    first = (first or "").strip()
    last  = (last  or "").strip()
    key = f"{first} {last}".strip()
    if key in _ru_name_cache:
        return _ru_name_cache[key]

    # 1) поиск sports.ru
    if key:
        try:
            q = quote_plus(key)
            sr = S.get(SPORTS_RU_SEARCH + q, timeout=25)
            if sr.status_code == 200:
                soup = BeautifulSoup(sr.text, "html.parser")
                link = soup.select_one('a[href*="/hockey/person/"]') or soup.select_one('a[href*="/hockey/player/"]')
                if link and link.get("href"):
                    href = link["href"]
                    if href.startswith("/"):
                        href = "https://www.sports.ru" + href
                    ru = _ru_initial_surname_from_profile(href)
                    if ru:
                        _ru_name_cache[key] = ru
                        return ru
        except Exception as e:
            log("[sports.ru] search fail:", key, e)

    # 2) fallback: display (латиница) или склеить I. Lastname
    if display:
        _ru_name_cache[key or display] = display
        return display
    lat = (first[:1] + ". " if first else "") + (last or key or "Неизвестно")
    _ru_name_cache[key or lat] = lat
    return lat

def resolve_player_ru_initial(pid: int, boxmap: dict, players_involved: list) -> str:
    """
    Возвращает «И. РусскаяФамилия» для игрока:
    boxscore → playersInvolved → landing → латиница/ID.
    """
    # 1) boxmap
    if pid and pid in boxmap:
        f = boxmap[pid].get("firstName", "")
        l = boxmap[pid].get("lastName", "")
        disp = _display_cache.get(pid)  # может быть "C. McDavid"
        if f or l or disp:
            return ru_initial_surname_by_en(f, l, disp)

    # 2) playersInvolved
    for p in (players_involved or []):
        if p.get("playerId") == pid:
            f, l, d = _extract_names_from_player_obj(p)
            if f or l or d:
                return ru_initial_surname_by_en(f, l, d)

    # 3) landing
    f, l = fetch_player_en_name(pid)
    if f or l:
        return ru_initial_surname_by_en(f, l)

    # 4) крайний случай — отдаём ID
    return f"#{pid}"

# ===================== СБОРКА МАТЧА =====================

def team_ru_and_emoji(abbr: str) -> tuple[str, str]:
    abbr = (abbr or "").upper()
    if abbr in TEAM_RU:
        return TEAM_RU[abbr]
    return (abbr, "🏒")

def build_game_block(game: dict) -> str:
    game_id = game["gameId"]
    home_ab, away_ab = game["homeAbbrev"], game["awayAbbrev"]
    home_ru, emh = team_ru_and_emoji(home_ab)
    away_ru, ema = team_ru_and_emoji(away_ab)

    # данные
    goals = fetch_goals(game_id)
    box = fetch_box_map(game_id)

    # пометка ОТ/Б по типу последнего события/итогу
    suffix = ""
    last_pt = (goals[-1].get("periodType") if goals else "") or game.get("periodType") or ""
    if last_pt == "OT":
        suffix = " (ОТ)"
    elif last_pt == "SO":
        suffix = " (Б)"

    # строки событий в строгой хронологии
    lines = []
    for g in goals:
        scorer = resolve_player_ru_initial(g["scorerId"], box, g.get("playersInvolved"))
        a1 = resolve_player_ru_initial(g["a1"], box, g.get("playersInvolved")) if g.get("a1") else None
        a2 = resolve_player_ru_initial(g["a2"], box, g.get("playersInvolved")) if g.get("a2") else None

        assists = []
        if a1: assists.append(a1)
        if a2: assists.append(a2)
        ast_txt = f" ({', '.join(assists)})" if assists else ""

        # абсолютное время MM.SS
        t_abs = fmt_mm_ss(g["totsec"])
        lines.append(f"{g['home']}:{g['away']} – {t_abs} {scorer}{ast_txt}")

    head = f"{emh} «{home_ru}»: {game['homeScore']}\n{ema} «{away_ru}»: {game['awayScore']}{suffix}\n\n"
    if not lines:
        lines = ["— подробная запись голов недоступна"]

    return head + "\n".join(lines)

# ===================== ФОРМИРОВАНИЕ ПОСТА =====================

def build_post(day: date) -> str:
    games = fetch_games_for_date(day)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(day)} • {len(games)} {ru_plural(len(games), ('матч', 'матча', 'матчей'))}\n\n"
    title += "Результаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    blocks = []
    for i, g in enumerate(games, 1):
        try:
            blocks.append(build_game_block(g))
        except Exception as e:
            log(f"[WARN] game {g.get('gameId')} failed:", e)
            home_ru, emh = team_ru_and_emoji(g["homeAbbrev"])
            away_ru, ema = team_ru_and_emoji(g["awayAbbrev"])
            blocks.append(
                f"{emh} «{home_ru}»: {g['homeScore']}\n{ema} «{away_ru}»: {g['awayScore']}\n\n— события матча временно недоступны"
            )
        if i < len(games):
            blocks.append("")

    return title + "\n".join(blocks).strip()

# ===================== ОТПРАВКА В TELEGRAM =====================

def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    MAX = 3500
    t = text
    parts = []
    while t:
        if len(t) <= MAX:
            parts.append(t); break
        cut = t.rfind("\n\n", 0, MAX)
        if cut == -1: cut = MAX
        parts.append(t[:cut]); t = t[cut:].lstrip()

    for part in parts:
        resp = S.post(url, json={
            "chat_id": CHAT_ID,
            "text": part,
            "disable_web_page_preview": True,
        }, timeout=25)
        if resp.status_code != 200:
            raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
        time.sleep(0.4)

# ===================== MAIN =====================

if __name__ == "__main__":
    try:
        target = pick_report_date()
        games = fetch_games_for_date(target)
        if not games:
            target = target - timedelta(days=1)  # запасной день назад
        msg = build_post(target)
        tg_send(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
