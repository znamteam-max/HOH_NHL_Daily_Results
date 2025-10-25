#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram

• Голы/ассисты — api-web.nhle.com (официальный play-by-play)
• Имена — «И. Фамилия» по sports.ru:
    1) прямой доступ к /hockey/person/{slug}/
    2) поиск на sports.ru
    3) фоллбэк: транслитерация + справочник исключений под sports.ru
• Время голов — MM.SS по абсолютной шкале матча (напр., 1.15, 21.45, 45.59, 68.15)
"""

import os
import sys
import re
import time
import unicodedata
from datetime import date, datetime, timedelta
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# -------------------- Константы/настройки --------------------

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

API_NHL = "https://api-web.nhle.com/v1"

SPORTS_RU_HOST    = "https://www.sports.ru"
SPORTS_RU_PERSON  = SPORTS_RU_HOST + "/hockey/person/"
SPORTS_RU_PLAYER  = SPORTS_RU_HOST + "/hockey/player/"
SPORTS_RU_SEARCH  = SPORTS_RU_HOST + "/search/?q="  # серверный HTML есть

RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

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

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def log(*a): print(*a, file=sys.stderr)

# -------------------- HTTP с ретраями --------------------

def make_session():
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"], raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-DailyResultsBot/2.5 (+api-web.nhle.com; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

# -------------------- Дата игрового дня --------------------

def pick_report_date() -> date:
    # если в Нью-Йорке раньше 07:00 — считаем, что игровой день ещё «вчера»
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 7 else now_et.date()

# -------------------- Время/периоды --------------------

def parse_time_to_sec_in_period(t: str) -> int:
    try:
        m, s = str(t).split(":")
        return int(m)*60 + int(s)
    except Exception:
        try:
            return int(t)*60
        except Exception:
            return 0

def period_to_index(period_type: str, number: int) -> int:
    pt = (period_type or "").upper()
    if pt == "OT": return 4
    if pt == "SO": return 5
    return max(1, int(number or 1))

def abs_seconds(period_index: int, sec_in_period: int) -> int:
    if period_index == 5:   # SO
        return 65*60 + sec_in_period
    if period_index >= 4:   # OT
        return 60*60 + sec_in_period
    return (period_index - 1)*20*60 + sec_in_period

def fmt_mm_ss(total_seconds: int) -> str:
    mm = total_seconds // 60
    ss = total_seconds % 60
    return f"{mm}.{ss:02d}"

# -------------------- Расписание/финалы --------------------

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

def fetch_games_for_date(day: date) -> list[dict]:
    out = []

    j = _get_json(f"{API_NHL}/schedule/{day.isoformat()}")
    for bucket in j.get("gameWeek", []):
        if bucket.get("date") != day.isoformat():
            continue
        for g in bucket.get("games", []):
            st = str(g.get("gameState", "")).upper()
            if st not in {"OFF", "FINAL"}:
                continue
            hm, aw = g.get("homeTeam", {}) or {}, g.get("awayTeam", {}) or {}
            out.append({
                "gameId": int(g.get("id") or g.get("gameId")),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": (g.get("periodDescriptor") or {}).get("periodType") or "",
            })

    if not out:
        j = _get_json(f"{API_NHL}/score/{day.isoformat()}")
        for g in j.get("games", []):
            st = str(g.get("gameState", "")).upper()
            if st not in {"OFF", "FINAL"}:
                continue
            hm, aw = g.get("homeTeam", {}) or {}, g.get("awayTeam", {}) or {}
            pd = g.get("periodDescriptor", {}) or {}
            out.append({
                "gameId": int(g.get("id") or g.get("gameId")),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": pd.get("periodType") or "",
            })

    if not out:
        j = _get_json(f"{API_NHL}/scoreboard/{day.isoformat()}")
        for g in j.get("games", []):
            st = str(g.get("gameState", "")).upper()
            if st not in {"OFF", "FINAL"}:
                continue
            hm, aw = g.get("homeTeam", {}) or {}, g.get("awayTeam", {}) or {}
            pd = g.get("periodDescriptor", {}) or {}
            out.append({
                "gameId": int(g.get("id") or g.get("gameId")),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": pd.get("periodType") or "",
            })

    return out

# -------------------- BOX + PBP --------------------

_en_name_cache: dict[int, tuple[str, str]] = {}  # playerId -> (first,last)
_display_cache: dict[int, str] = {}              # playerId -> "C. McDavid" и т.п.

def _extract_names_from_player_obj(p: dict) -> tuple[str, str, str]:
    first = ""; last = ""; display = ""

    fn = p.get("firstName"); ln = p.get("lastName")
    if isinstance(fn, dict): fn = fn.get("default") or ""
    if isinstance(ln, dict): ln = ln.get("default") or ""
    if fn: first = str(fn).strip()
    if ln: last  = str(ln).strip()

    for key in ("firstInitialLastName", "playerName", "name", "playerNameWithNumber", "fullName"):
        val = p.get(key)
        if isinstance(val, dict):
            val = val.get("default") or ""
        if val and not display:
            display = str(val).strip()

    # если first/last пусты, попробуем выдернуть из display
    if (not first or not last) and display:
        disp = display.replace("#", " ").strip()
        parts = [x for x in re.split(r"\s+", disp) if x and x != "-"]
        if len(parts) >= 2:
            last = last or parts[-1]
            first = first or parts[0].replace(".", "").strip()

    return first, last, display

def fetch_box_map(game_id: int) -> dict[int, dict]:
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
                if f or l: _en_name_cache[pid] = (f, l)
                if d: _display_cache[pid] = d

    stats = data.get("playerByGameStats", {}) or {}
    eat(stats.get("homeTeam", {}) or {})
    eat(stats.get("awayTeam", {}) or {})
    return out

def fetch_player_en_name(pid: int) -> tuple[str, str]:
    if pid in _en_name_cache:
        return _en_name_cache[pid]
    try:
        url = f"{API_NHL}/player/{pid}/landing"
        r = S.get(url, timeout=20)
        if r.status_code == 200:
            j = r.json()
            fn, ln = j.get("firstName"), j.get("lastName")
            if isinstance(fn, dict): fn = fn.get("default") or ""
            if isinstance(ln, dict): ln = ln.get("default") or ""
            fn, ln = (fn or "").strip(), (ln or "").strip()
            _en_name_cache[pid] = (fn, ln)
            return fn, ln
    except Exception as e:
        log("[landing] fail", pid, e)
    _en_name_cache[pid] = ("", "")
    return "", ""

def fetch_goals(game_id: int) -> list[dict]:
    url = f"{API_NHL}/gamecenter/{game_id}/play-by-play"
    r = S.get(url, timeout=25); r.raise_for_status()
    data = r.json()
    plays = data.get("plays", []) or []
    goals = []

    for ev in plays:
        if ev.get("typeDescKey") != "goal":
            continue
        det = ev.get("details", {}) or {}
        pd  = ev.get("periodDescriptor", {}) or {}

        t = str(ev.get("timeInPeriod") or det.get("timeInPeriod") or "0:00")
        sec_in = parse_time_to_sec_in_period(t)
        pidx = period_to_index(pd.get("periodType"), pd.get("number"))
        totsec = abs_seconds(pidx, sec_in)

        hs = int(det.get("homeScore", 0))
        as_ = int(det.get("awayScore", 0))

        sid = det.get("scoringPlayerId")
        a1  = det.get("assist1PlayerId") or det.get("secondaryAssistPlayerId")
        a2  = det.get("assist2PlayerId") or det.get("tertiaryAssistPlayerId")

        players = ev.get("playersInvolved") or []

        if not sid and players:
            for p in players:
                tpe = (p.get("playerType") or "").lower()
                if tpe == "scorer":
                    sid = p.get("playerId")
                elif tpe == "assist":
                    if not a1: a1 = p.get("playerId")
                    elif not a2: a2 = p.get("playerId")

        goals.append({
            "period": pidx, "sec": sec_in, "totsec": totsec,
            "home": hs, "away": as_,
            "scorerId": int(sid) if sid else None,
            "a1": int(a1) if a1 else None,
            "a2": int(a2) if a2 else None,
            "periodType": (pd.get("periodType") or "").upper(),
            "playersInvolved": players,
        })

    goals.sort(key=lambda x: (x["period"], x["sec"]))
    return goals

# -------------------- Имя по sports.ru --------------------

_ru_name_cache: dict[str, str] = {}   # "Connor McDavid" -> "К. Макдэвид"
_slug_cache   : dict[str, str] = {}   # "Connor McDavid" -> "/hockey/person/connor-mcdavid/"

# популярные исключения под стиль sports.ru (фамилии)
EXCEPT_LAST = {
    "Nylander": "Нюландер",
    "Ekman-Larsson": "Экман-Ларссон",
    "Scheifele": "Шайфли",
    "Iafallo": "Иафалло",
    "Backlund": "Баклунд",
    "Kadri": "Кадри",
    "Toews": "Тэйвс",
    "Morrissey": "Моррисси",
    "Namestnikov": "Наместников",
    "Kulich": "Кулих",
    "Samuelsson": "Самуэльссон",
    "Dahlin": "Далин",
    "Roy": "Руа",
    "Cowan": "Коуэн",
    "Coleman": "Колман",
    "Bahl": "Баль",
    "Parekh": "Парех",
    "DeMelo": "Демело",
    "Vilardi": "Виларди",
    "Hamilton": "Хэмилтон",
    "Hischier": "Хишир",
    "Hughes": "Хьюз",
    "Brown": "Браун",
    "Carlson": "Карлсон",
    "Lapierre": "Лапьер",
    "McMichael": "Макмайкл",
    "Strome": "Строум",
    "Sourdif": "Сурдиф",
    "Leonard": "Леонард",
    "Thompson": "Томпсон",
    "Mateychuk": "Матейчук",
    "Monahan": "Монахан",
    "Frank": "Фрэнк",
    "Protas": "Протас",
    "Matthews": "Мэттьюс",
    "Tavares": "Таварес",
    "Power": "Пауэр",
    "Joshua": "Джошуа",
    "Connor": "Коннор",
    "Backlund": "Баклунд",
    "Honzek": "Гонзек",
}

# грубая транслитерация имени → русская первая буква (для инициалов в фоллбэке)
FIRST_INITIAL_MAP = {
    "a":"А","b":"Б","c":"К","d":"Д","e":"Э","f":"Ф","g":"Г","h":"Х","i":"И","j":"Д",
    "k":"К","l":"Л","m":"М","n":"Н","o":"О","p":"П","q":"К","r":"Р","s":"С","t":"Т",
    "u":"У","v":"В","w":"В","x":"Кс","y":"Й","z":"З"
}

def _slugify_eng_name(first: str, last: str) -> str:
    # connor mcdavid -> connor-mcdavid
    base = f"{first} {last}".strip()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower().strip()
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    return base

def _sportsru_try_profile_by_slug(first: str, last: str) -> str | None:
    slug = _slugify_eng_name(first, last)
    for root in (SPORTS_RU_PERSON, SPORTS_RU_PLAYER):
        url = root + slug + "/"
        r = S.get(url, timeout=15)
        if r.status_code == 200 and "hockey/person" in r.url or "hockey/player" in r.url:
            return url
    return None

def _sportsru_extract_initial_surname_from_profile(url: str) -> str | None:
    try:
        r = S.get(url, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1", "h2"])
        if not h: return None
        full = " ".join(h.get_text(" ", strip=True).split())
        parts = [p for p in re.split(r"\s+", full) if p]
        if len(parts) >= 2:
            ini = parts[0][0] + "."
            last = parts[-1]
            return f"{ini} {last}"
    except Exception as e:
        log("[sports.ru profile parse] fail:", e)
    return None

def _sportsru_search_initial_surname(first: str, last: str) -> str | None:
    try:
        q = quote_plus(f"{first} {last}".strip())
        r = S.get(SPORTS_RU_SEARCH + q, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        # ищем первую ссылку на профиль игрока
        link = soup.select_one('a[href*="/hockey/person/"]') or soup.select_one('a[href*="/hockey/player/"]')
        if not link or not link.get("href"):
            return None
        href = link["href"]
        if href.startswith("/"):
            href = SPORTS_RU_HOST + href
        return _sportsru_extract_initial_surname_from_profile(href)
    except Exception as e:
        log("[sports.ru search] fail:", first, last, e)
    return None

def _fallback_translit_initial_surname(first: str, last: str) -> str:
    # спец-исключения на фамилии под стиль sports.ru
    ru_last = EXCEPT_LAST.get(last, None)
    if not ru_last:
        # простая транслитерация фамилии: заменим дефисы на дефисы и оставим латиницу как есть,
        # чтобы не исказить редкие фамилии: в итогах важнее корректность, чем «плохой» перевод
        ru_last = last

    # русская первая буква имени (грубо)
    ini_src = (first or "A")[:1].lower()
    ru_ini = FIRST_INITIAL_MAP.get(ini_src, ini_src.upper())
    # если получилось «Кс» (X) — оставим «К.», чтобы сохранить формат
    if len(ru_ini) > 1:
        ru_ini = ru_ini[0]
    return f"{ru_ini}. {ru_last}"

def ru_initial_surname_by_en(first: str, last: str, display: str | None = None) -> str:
    first = (first or "").strip()
    last  = (last  or "").strip()
    key = f"{first} {last}".strip() or (display or "").strip()
    if not key:
        return ""

    if key in _ru_name_cache:
        return _ru_name_cache[key]

    # 1) прямой slug
    if first and last:
        url = _sportsru_try_profile_by_slug(first, last)
        if url:
            res = _sportsru_extract_initial_surname_from_profile(url)
            if res:
                _ru_name_cache[key] = res
                _slug_cache[key] = url
                return res

    # 2) поиск
    if first and last:
        res = _sportsru_search_initial_surname(first, last)
        if res:
            _ru_name_cache[key] = res
            return res

    # 3) фоллбэк: если есть красивый латинский вариант — сначала попробуем добыть first/last из него
    if display and not (first and last):
        disp = display.replace("#", " ").strip()
        parts = [x for x in re.split(r"\s+", disp) if x and x != "-"]
        if len(parts) >= 2:
            first = first or parts[0].replace(".", "")
            last  = last  or parts[-1]

    # 4) окончательный фоллбэк — транслитерация + исключения
    fallback = _fallback_translit_initial_surname(first or "", last or (display or ""))
    _ru_name_cache[key] = fallback
    return fallback

def resolve_player_ru_initial(pid: int, boxmap: dict, players_involved: list) -> str:
    # 1) boxscore
    if pid and pid in boxmap:
        f = boxmap[pid].get("firstName", "")
        l = boxmap[pid].get("lastName", "")
        disp = _display_cache.get(pid)
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

    return f"#{pid}"

# -------------------- Сборка блока матча --------------------

def team_ru_and_emoji(abbr: str) -> tuple[str, str]:
    abbr = (abbr or "").upper()
    if abbr in TEAM_RU:
        return TEAM_RU[abbr]
    return (abbr, "🏒")

def build_game_block(game: dict) -> str:
    gid = game["gameId"]
    home_ab, away_ab = game["homeAbbrev"], game["awayAbbrev"]
    home_ru, emh = team_ru_and_emoji(home_ab)
    away_ru, ema = team_ru_and_emoji(away_ab)

    goals = fetch_goals(gid)
    box   = fetch_box_map(gid)

    suffix = ""
    last_pt = (goals[-1].get("periodType") if goals else "") or game.get("periodType") or ""
    if last_pt == "OT": suffix = " (ОТ)"
    elif last_pt == "SO": suffix = " (Б)"

    lines = []
    for g in goals:
        scorer = resolve_player_ru_initial(g["scorerId"], box, g.get("playersInvolved"))
        a1 = resolve_player_ru_initial(g["a1"], box, g.get("playersInvolved")) if g.get("a1") else None
        a2 = resolve_player_ru_initial(g["a2"], box, g.get("playersInvolved")) if g.get("a2") else None

        assists = []
        if a1: assists.append(a1)
        if a2: assists.append(a2)
        ast_txt = f" ({', '.join(assists)})" if assists else ""

        t_abs = fmt_mm_ss(g["totsec"])
        # гарантируем пробел в «J. Kulich» и пр.
        scorer = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", scorer)
        ast_txt = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", ast_txt)

        lines.append(f"{g['home']}:{g['away']} – {t_abs} {scorer}{ast_txt}")

    head = f"{emh} «{home_ru}»: {game['homeScore']}\n{ema} «{away_ru}»: {game['awayScore']}{suffix}\n\n"
    if not lines:
        lines = ["— подробная запись голов недоступна"]

    return head + "\n".join(lines)

# -------------------- Формирование поста --------------------

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

# -------------------- Telegram --------------------

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

# -------------------- Main --------------------

if __name__ == "__main__":
    try:
        target = pick_report_date()
        games = fetch_games_for_date(target)
        if not games:
            target = target - timedelta(days=1)
        msg = build_post(target)
        tg_send(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
