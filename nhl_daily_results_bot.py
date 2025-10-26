#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram (RU)

• Матчи/ивенты: api-web.nhle.com
  - Расписание:   /v1/schedule/YYYY-MM-DD (фоллбеки: /v1/score, /v1/scoreboard)
  - PBP:          /v1/gamecenter/{gameId}/play-by-play
  - Boxscore:     /v1/gamecenter/{gameId}/boxscore
  - Игрок:        /v1/player/{playerId}/landing

• Имена (формат «И. Фамилия» по-русски):
  1) sports.ru/hockey/person|player/{slug}/ → заголовок → последнее слово (фамилия)
  2) поиск sports.ru
  3) словарь исключений
  4) алгоритмическая транслитерация латиницы → кириллица (на случай редких новичков)
  + автокэш ru_map.json / ru_pending.json (как раньше)

• Буллиты: выводим ТОЛЬКО победный буллит (последний гол серии у команды-победителя).
"""

import os
import sys
import re
import json
import time
import unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

API_NHL = "https://api-web.nhle.com/v1"

# --------- HTTP ---------
def make_session():
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.6,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
        raise_on_status=False
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-DailyResultsBot/so-winner-only/1.1",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

# --------- Даты и заголовки ---------
RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def pick_report_date() -> date:
    # на НХЛ ориентируемся на Eastern Time
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 7 else now_et.date()

# --------- Команды (названия + эмодзи) ---------
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

def team_ru_and_emoji(abbr: str) -> tuple[str, str]:
    abbr = (abbr or "").upper()
    return TEAM_RU.get(abbr, (abbr, "🏒"))

# --------- Утилиты времени/периодов ---------
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
        return 65*60 + (sec_in_period or 0)
    if period_index >= 4:   # OT
        return 60*60 + (sec_in_period or 0)
    return (period_index - 1)*20*60 + (sec_in_period or 0)

def fmt_mm_ss(total_seconds: int) -> str:
    mm = total_seconds // 60
    ss = total_seconds % 60
    return f"{mm}.{ss:02d}"

def period_heading(period_index: int) -> str:
    if period_index <= 3:
        return f"<i>{period_index}-й период</i>"
    if period_index == 5:
        return "<i>Буллиты</i>"
    return f"<i>Овертайм №{period_index - 3}</i>"

# --------- Загрузка JSON ---------
def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

# --------- Расписание (финалы) ---------
def fetch_games_for_date(day: date) -> list[dict]:
    out = []
    def eat(bucket_games):
        for g in bucket_games:
            st = str(g.get("gameState", "")).upper()
            if st not in {"OFF", "FINAL"}:
                continue
            hm, aw = g.get("homeTeam", {}) or {}, g.get("awayTeam", {}) or {}
            pd = (g.get("periodDescriptor") or {}) if "periodDescriptor" in g else {}
            out.append({
                "gameId": int(g.get("id") or g.get("gameId")),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": (pd or {}).get("periodType") or (g.get("periodType") or ""),
                "homeId": int(hm.get("id") or hm.get("teamId") or 0),
                "awayId": int(aw.get("id") or aw.get("teamId") or 0),
            })
    j = _get_json(f"{API_NHL}/schedule/{day.isoformat()}")
    for bucket in j.get("gameWeek", []):
        if bucket.get("date") == day.isoformat():
            eat(bucket.get("games") or [])
    if not out:
        j = _get_json(f"{API_NHL}/score/{day.isoformat()}")
        eat(j.get("games") or [])
    if not out:
        j = _get_json(f"{API_NHL}/scoreboard/{day.isoformat()}")
        eat(j.get("games") or [])
    return out

# --------- Boxscore (для имён) ---------
_en_name_cache: dict[int, tuple[str, str]] = {}
_display_cache: dict[int, str] = {}

def _extract_names_from_player_obj(p: dict) -> tuple[str, str, str]:
    first = ""; last = ""; display = ""
    fn = p.get("firstName"); ln = p.get("lastName")
    if isinstance(fn, dict): fn = fn.get("default") or ""
    if isinstance(ln, dict): ln = ln.get("default") or ""
    first = (fn or "").strip(); last = (ln or "").strip()
    for key in ("firstInitialLastName","playerName","name","playerNameWithNumber","fullName"):
        v = p.get(key)
        if isinstance(v, dict): v = v.get("default") or ""
        if v and not display: display = str(v).strip()
    if (not first or not last) and display:
        disp = display.replace("#", " ").strip()
        parts = [x for x in re.split(r"\s+", disp) if x and x != "-"]
        if len(parts) >= 2:
            last = last or parts[-1]
            first = first or parts[0].replace(".", "")
    return first, last, display

def fetch_box_map(game_id: int) -> dict[int, dict]:
    url = f"{API_NHL}/gamecenter/{game_id}/boxscore"
    r = S.get(url, timeout=25); r.raise_for_status()
    data = r.json()
    out: dict[int, dict] = {}
    def eat(team_block: dict):
        for group in ("forwards","defense","goalies"):
            for p in team_block.get(group, []) or []:
                pid = p.get("playerId")
                if not pid: continue
                pid = int(pid)
                f, l, d = _extract_names_from_player_obj(p)
                out[pid] = {"firstName": f, "lastName": l}
                if f or l: _en_name_cache[pid] = (f, l)
                if d: _display_cache[pid] = d
    stats = data.get("playerByGameStats", {}) or {}
    eat(stats.get("homeTeam", {}) or {})
    eat(stats.get("awayTeam", {}) or {})
    return out

def fetch_player_en_name(pid: int) -> tuple[str,str]:
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
    except Exception:
        pass
    _en_name_cache[pid] = ("","")
    return "",""

# --------- PBP (голы + буллиты) ---------
def fetch_goals(game_id: int) -> list[dict]:
    url = f"{API_NHL}/gamecenter/{game_id}/play-by-play"
    r = S.get(url, timeout=25); r.raise_for_status()
    data = r.json()
    plays = data.get("plays", []) or []
    out = []
    for ev in plays:
        if ev.get("typeDescKey") != "goal":
            # в серии буллитов typeDescKey у «голов» тоже "goal"
            continue
        det = ev.get("details", {}) or {}
        pd  = ev.get("periodDescriptor", {}) or {}
        t = str(ev.get("timeInPeriod") or det.get("timeInPeriod") or "0:00")
        sec_in = parse_time_to_sec_in_period(t)
        pidx = period_to_index(pd.get("periodType"), pd.get("number"))
        totsec = abs_seconds(pidx, sec_in)

        sid = det.get("scoringPlayerId")
        a1  = det.get("assist1PlayerId") or det.get("secondaryAssistPlayerId")
        a2  = det.get("assist2PlayerId") or det.get("tertiaryAssistPlayerId")

        # команда, за которую забили (важно для буллитов)
        team_id = det.get("eventOwnerTeamId") or ev.get("teamId") or det.get("teamId")
        try:
            team_id = int(team_id) if team_id is not None else None
        except Exception:
            team_id = None

        players = ev.get("playersInvolved") or []
        if (not sid) and players:
            for p in players:
                tpe = (p.get("playerType") or "").lower()
                if tpe == "scorer":
                    sid = p.get("playerId")
                elif tpe == "assist":
                    if not a1: a1 = p.get("playerId")
                    elif not a2: a2 = p.get("playerId")

        out.append({
            "period": pidx, "sec": sec_in, "totsec": totsec,
            "home": int(det.get("homeScore", 0)), "away": int(det.get("awayScore", 0)),
            "scorerId": int(sid) if sid else None,
            "a1": int(a1) if a1 else None, "a2": int(a2) if a2 else None,
            "periodType": (pd.get("periodType") or "").upper(),
            "playersInvolved": players,
            "teamId": team_id,
        })
    out.sort(key=lambda x: (x["period"], x["sec"]))
    return out

# --------- RU фамилии (инициал + фамилия) ---------
SPORTS_RU_HOST    = "https://www.sports.ru"
SPORTS_RU_PERSON  = SPORTS_RU_HOST + "/hockey/person/"
SPORTS_RU_PLAYER  = SPORTS_RU_HOST + "/hockey/player/"
SPORTS_RU_SEARCH  = SPORTS_RU_HOST + "/search/?q="

RU_MAP_PATH     = "ru_map.json"      # id -> "И. Фамилия"
RU_PENDING_PATH = "ru_pending.json"  # [{id, first, last}]

RU_MAP: dict[str,str] = {}
RU_PENDING: list[dict] = []
_session_pending_ids: set[int] = set()

# исключения (лат. фамилия → русская)
EXCEPT_LAST = {
    "Nylander":"Нюландер","Ekman-Larsson":"Экман-Ларссон","Scheifele":"Шайфли","Iafallo":"Иафалло",
    "Backlund":"Баклунд","Kadri":"Кадри","Toews":"Тэйвс","Morrissey":"Моррисси","Namestnikov":"Наместников",
    "Kulich":"Кулих","Samuelsson":"Самуэльссон","Dahlin":"Далин","Roy":"Руа","Cowan":"Коуэн",
    "Coleman":"Колман","Bahl":"Баль","Parekh":"Парех","DeMelo":"Демело","Vilardi":"Виларди",
    "Hamilton":"Хэмилтон","Hischier":"Хишир","Hughes":"Хьюз","Brown":"Браун","Carlson":"Карлсон",
    "Lapierre":"Лапьер","McMichael":"Макмайкл","Strome":"Строум","Leonard":"Леонард","Thompson":"Томпсон",
    "Matthews":"Мэттьюс","Tavares":"Таварес","Power":"Пауэр","Joshua":"Джошуа","Connor":"Коннор",
    "Byram":"Байрэм","Benson":"Бенсон","Krebs":"Кребс","Carlo":"Карло","Tuch":"Так","McLeod":"Маклауд",
    "Eklund":"Эклунд","Celebrini":"Селебрини","Mercer":"Мерсер","Voronkov":"Воронков","Wilson":"Уилсон",
    "Ovechkin":"Овечкин","Stanley":"Стэнли","Frank":"Фрэнк","Ekholm":"Экхольм","Nurse":"Нерс",
    "Nugent-Hopkins":"Нюджент-Хопкинс","Bouchard":"Бушар","Honzek":"Гонзек","Monahan":"Монахан",
    "Sourdif":"Сурдиф","Mateychuk":"Матейчук","Frost":"Фрост","Protas":"Протас","Cowen":"Коуэн",
    "Lehkonen":"Лехконен","Holmstrom":"Хольмстрём","DeAngelo":"Деанджело","Drysdale":"Драйсдейл",
    "Reinhart":"Райнхарт","Verhaeghe":"Верхаге","Rodrigues":"Родригес","Schmaltz":"Шмалц",
    "Johansson":"Йоханссон","Schwindt":"Швиндт","Gadjovich":"Гаджович","Guenther":"Гюнтер",
    "Sergachev":"Сергачёв","Peterka":"Петерка","Hronek":"Хронек","Matheson":"Матесон",
    "Slafkovský":"Слафковский","Slafkovsky":"Слафковский","Pettersson":"Петтерссон",
    "Kaprizov":"Капризов","Batherson":"Батерсон","Stützle":"Штюцле","Stutzle":"Штюцле",
    "Chabot":"Шабо","Giroux":"Жиру","Cozens":"Коузенс","Cousins":"Кузинс","Kyrou":"Кайру",
    "Neighbours":"Нейборс","Debrincat":"Дебринкэт","DeBrincat":"Дебринкэт","Edvinsson":"Эдвинссон",
    "Letang":"Летанг","Rust":"Раст","Crosby":"Кросби","Fantilli":"Фанти́лли","Marchenko":"Марченко",
    "Maccelli":"Маккелли","Zucker":"Закер","Robertson":"Робертсон","Carlsson":"Карлссон",
    "Guentzel":"Генцел","Hagel":"Хэйгел","Cirelli":"Чирелли","Helleson":"Хеллесон",
}

FIRST_INITIAL_MAP = {
    "a":"А","b":"Б","c":"К","d":"Д","e":"Э","f":"Ф","g":"Г","h":"Х","i":"И","j":"Д",
    "k":"К","l":"Л","m":"М","n":"Н","o":"О","p":"П","q":"К","r":"Р","s":"С","t":"Т",
    "u":"У","v":"В","w":"В","x":"К","y":"Й","z":"З"
}

def _load_json(path: str, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _slugify_eng_name(first: str, last: str) -> str:
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
        if r.status_code == 200 and ("/hockey/person/" in r.url or "/hockey/player/" in r.url):
            return url
    return None

def _sportsru_extract_initial_surname_from_profile(url: str) -> str | None:
    try:
        r = S.get(url, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1","h2"])
        if not h: return None
        full = " ".join(h.get_text(" ", strip=True).split())
        parts = [p for p in re.split(r"\s+", full) if p]
        if len(parts) >= 2:
            ini = parts[0][0] + "."
            last = parts[-1]
            return f"{ini} {last}"
    except Exception:
        return None
    return None

def _sportsru_search_initial_surname(first: str, last: str) -> str | None:
    try:
        q = quote_plus(f"{first} {last}".strip())
        r = S.get(SPORTS_RU_SEARCH + q, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        link = soup.select_one('a[href*="/hockey/person/"]') or soup.select_one('a[href*="/hockey/player/"]')
        if not link or not link.get("href"): return None
        href = link["href"]
        if href.startswith("/"): href = SPORTS_RU_HOST + href
        return _sportsru_extract_initial_surname_from_profile(href)
    except Exception:
        return None

# транслитерация латиницы → кириллица (фамилия)
def _translit_lastname_ru(last: str) -> str:
    s = (last or "").strip()
    if not s: return s
    low = s.lower()

    repl_order = [
        ("shch","щ"), ("sch","ш"), ("ch","ч"), ("sh","ш"), ("zh","ж"),
        ("kh","х"), ("ts","ц"), ("ya","я"), ("yo","ё"), ("yu","ю"),
        ("ye","е"), ("ii","ий"),
        ("ä","я"), ("ö","ё"), ("ø","ё"), ("å","о"), ("é","е"), ("á","а"), ("í","и"), ("ó","о"), ("ú","у"),
        ("ç","с"), ("ñ","нь"), ("ł","л"), ("ž","ж"), ("š","ш"), ("č","ч"),
    ]
    for a,b in repl_order:
        low = low.replace(a,b)

    # односимвольные
    table = {
        "a":"а","b":"б","c":"к","d":"д","e":"е","f":"ф","g":"г","h":"х","i":"и",
        "j":"й","k":"к","l":"л","m":"м","n":"н","o":"о","p":"п","q":"к","r":"р",
        "s":"с","t":"т","u":"у","v":"в","w":"в","x":"кс","y":"и","z":"з",
        "-":"-"," ":" "
    }
    out = []
    i = 0
    while i < len(low):
        ch = low[i]
        out.append(table.get(ch, ch))
        i += 1
    res = "".join(out)

    # Капитализация по частям через дефис/пробел
    parts = [p for p in re.split(r"([- ])", res) if p != ""]
    def cap_ru(word: str) -> str:
        if not word or not re.match(r"[а-яё]", word): return word
        return word[0].upper() + word[1:]
    parts2 = []
    for p in parts:
        if p in {"-"," "}: parts2.append(p)
        else: parts2.append(cap_ru(p))
    return "".join(parts2)

def _queue_pending(pid: int, first: str, last: str):
    if not pid or pid in _session_pending_ids: return
    if str(pid) in RU_MAP: return
    for it in RU_PENDING:
        if it.get("id") == pid: return
    RU_PENDING.append({"id": pid, "first": first or "", "last": last or ""})
    _session_pending_ids.add(pid)

def ru_initial_surname_by_en(first: str, last: str, display: str | None, pid: int | None) -> str:
    # кэш по id
    if pid is not None and str(pid) in RU_MAP:
        return RU_MAP[str(pid)]

    first = (first or "").strip()
    last  = (last  or "").strip()
    key = f"{first} {last}".strip() or (display or "").strip()

    # 1) sports.ru (slug → профиль)
    if first and last:
        url = _sportsru_try_profile_by_slug(first, last)
        if url:
            res = _sportsru_extract_initial_surname_from_profile(url)
            if res:
                return res

    # 2) sports.ru (поиск)
    if first and last:
        res = _sportsru_search_initial_surname(first, last)
        if res:
            return res

    # 3) исключения/транслит
    ru_last = EXCEPT_LAST.get(last) or _translit_lastname_ru(last or (display or ""))
    ini_src = (first or last or "A")[:1].lower()
    ru_ini = FIRST_INITIAL_MAP.get(ini_src, ini_src.upper())
    if len(ru_ini) > 1:  # «Кс» → «К.»
        ru_ini = ru_ini[0]
    name = f"{ru_ini}. {ru_last}".strip()

    if pid: _queue_pending(pid, first, last)
    return name

def resolve_player_ru_initial(pid: int, boxmap: dict, players_involved: list) -> str:
    # boxscore
    if pid and pid in boxmap:
        f = boxmap[pid].get("firstName",""); l = boxmap[pid].get("lastName","")
        d = _display_cache.get(pid)
        if f or l or d:
            return ru_initial_surname_by_en(f, l, d, pid)
    # playersInvolved
    for p in (players_involved or []):
        if p.get("playerId") == pid:
            f, l, d = _extract_names_from_player_obj(p)
            if f or l or d:
                return ru_initial_surname_by_en(f, l, d, pid)
    # landing
    f,l = fetch_player_en_name(pid)
    if f or l:
        return ru_initial_surname_by_en(f, l, None, pid)
    _queue_pending(pid, "", "")
    return f"#{pid}"

# --------- Блок матча ---------
def build_game_block(game: dict) -> str:
    gid = game["gameId"]
    home_ab, away_ab = game["homeAbbrev"], game["awayAbbrev"]
    home_ru, emh = team_ru_and_emoji(home_ab)
    away_ru, ema = team_ru_and_emoji(away_ab)
    home_score, away_score = game["homeScore"], game["awayScore"]

    goals = fetch_goals(gid)
    box   = fetch_box_map(gid)

    last_pt = (goals[-1].get("periodType") if goals else "") or game.get("periodType") or ""
    suffix = " (ОТ)" if last_pt == "OT" else (" (Б)" if last_pt == "SO" else "")

    # Заголовок со счётом (как в ваших примерах)
    head = f"{emh} «{home_ru}»: {home_score}\n{ema} «{away_ru}»: {away_score}{suffix}\n\n"

    if not goals:
        return head + "— подробная запись голов недоступна"

    # Разделим обычные голы и буллиты
    so_goals = [g for g in goals if g["period"] == 5]
    reg_goals = [g for g in goals if g["period"] != 5]

    lines = []
    current_period = None

    # Печатаем все голы в 1-3 периодах и овертаймах
    for g in reg_goals:
        if g["period"] != current_period:
            current_period = g["period"]
            if lines: lines.append("")
            lines.append(period_heading(current_period))

        scorer = resolve_player_ru_initial(g["scorerId"], box, g.get("playersInvolved"))
        a1 = resolve_player_ru_initial(g["a1"], box, g.get("playersInvolved")) if g.get("a1") else None
        a2 = resolve_player_ru_initial(g["a2"], box, g.get("playersInvolved")) if g.get("a2") else None
        assists = []
        if a1: assists.append(a1)
        if a2: assists.append(a2)
        ast_txt = f" ({', '.join(assists)})" if assists else ""

        t_abs = fmt_mm_ss(g["totsec"])
        scorer = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", scorer)
        ast_txt = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", ast_txt)

        lines.append(f"{g['home']}:{g['away']} – {t_abs} {scorer}{ast_txt}")

    # Буллиты: оставить только победный
    if so_goals:
        # победитель по финальному счёту
        winner_team_id = (game["homeId"] if home_score > away_score else
                          game["awayId"] if away_score > home_score else None)
        winning_shot = None
        if winner_team_id:
            # последний забитый буллит команды-победителя
            for g in reversed(so_goals):
                if g.get("teamId") == winner_team_id:
                    winning_shot = g
                    break
        if not winning_shot:
            # фоллбэк — последний буллит с goal
            winning_shot = so_goals[-1]

        # заголовок «Буллиты»
        lines.append("")
        lines.append(period_heading(5))

        scorer = resolve_player_ru_initial(winning_shot.get("scorerId"), box, winning_shot.get("playersInvolved"))
        scorer = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", scorer)
        t_abs = fmt_mm_ss(winning_shot["totsec"])
        # как в ваших примерах: показываем текущий счёт (он 3:3 и т.п.) + время 65.00 + автор
        lines.append(f"{winning_shot['home']}:{winning_shot['away']} – {t_abs} {scorer}")

    return head + "\n".join(lines)

# --------- Пост целиком ---------
def build_post(day: date) -> str:
    games = fetch_games_for_date(day)
    # если вдруг 0 — попробуем день назад
    if not games:
        day2 = day - timedelta(days=1)
        games = fetch_games_for_date(day2)
        if games: day = day2
    # и ещё назад, на всякий
    if not games:
        day3 = day - timedelta(days=2)
        games = fetch_games_for_date(day3)
        if games: day = day3

    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(day)} • {len(games)} {'матч' if len(games)==1 else 'матчей' if 5<=len(games)%100<=20 or len(games)%10 in (0,5,6,7,8,9) else 'матча'}\n\n"
    title += "Результаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    if not games:
        return title.strip()

    blocks = []
    for i, g in enumerate(games, 1):
        try:
            blocks.append(build_game_block(g))
        except Exception as e:
            print("[WARN] game", g.get("gameId"), ":", repr(e), file=sys.stderr)
            home_ru, emh = team_ru_and_emoji(g["homeAbbrev"])
            away_ru, ema = team_ru_and_emoji(g["awayAbbrev"])
            blocks.append(f"{emh} «{home_ru}»: {g['homeScore']}\n{ema} «{away_ru}»: {g['awayScore']}\n\n— события матча временно недоступны")
        if i < len(games):
            blocks.append("")

    return title + "\n".join(blocks).strip()

# --------- Telegram ---------
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
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=25)
        if resp.status_code != 200:
            raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
        time.sleep(0.3)

# --------- Main ---------
if __name__ == "__main__":
    try:
        # загрузим/инициализируем кэши (чтобы файлы точно существовали)
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])
        if isinstance(loaded_map, dict):
            RU_MAP.clear(); RU_MAP.update(loaded_map)
        if isinstance(loaded_pending, list):
            RU_PENDING.clear(); RU_PENDING.extend(loaded_pending)

        day = pick_report_date()
        text = build_post(day)
        tg_send(text)

        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
