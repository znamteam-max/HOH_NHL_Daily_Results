#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL → Telegram (RU)
Берём:
  • матчи и счёт, PBP — из api-web.nhle.com
  • фамилии и время голов — из страницы ТРАНСЛЯЦИИ матча на sports.ru (раздел «Матч окончен»)

Формат строки гола:
<счёт после гола> – <абсолютное время mm.ss> <Фамилия> (<Фамилия>, <Фамилия>)

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  REPORT_DATE=YYYY-MM-DD (необязательно; по умолчанию сегодня по Europe/Moscow)

Зависимости:
  requests==2.32.3
  beautifulsoup4==4.12.3
"""

# вверху файла
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
TZ_PT = ZoneInfo("America/Los_Angeles")

def resolve_pt_report_date():
    s = os.getenv("REPORT_DATE_PT","").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None

import os, sys, re, datetime as dt
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───── Константы
TZ_MSK = ZoneInfo("Europe/Moscow")
API = "https://api-web.nhle.com"
SPORTS_CAL = "https://www.sports.ru/hockey/tournament/nhl/calendar/"
SPORTS_SEARCH = "https://www.sports.ru/search/"
SPORTS_MATCH_BASE = "https://www.sports.ru/hockey/match/"

TEAM_META: Dict[str, Tuple[str, str]] = {
    "NJD": ("😈", "Нью-Джерси"),
    "NYI": ("🟠", "Айлендерс"),
    "NYR": ("🗽", "Рейнджерс"),
    "PHI": ("🛩", "Филадельфия"),
    "PIT": ("🐧", "Питтсбург"),
    "BOS": ("🐻", "Бостон"),
    "BUF": ("🦬", "Баффало"),
    "MTL": ("🇨🇦", "Монреаль"),
    "OTT": ("🛡", "Оттава"),
    "TOR": ("🍁", "Торонто"),
    "CAR": ("🌪️", "Каролина"),
    "FLA": ("🐆", "Флорида"),
    "TBL": ("⚡", "Тампа-Бэй"),
    "WSH": ("🦅", "Вашингтон"),
    "CHI": ("🦅", "Чикаго"),
    "DET": ("🔴", "Детройт"),
    "NSH": ("🐯", "Нэшвилл"),
    "STL": ("🎵", "Сент-Луис"),
    "CGY": ("🔥", "Калгари"),
    "EDM": ("🛢️", "Эдмонтон"),
    "VAN": ("🐳", "Ванкувер"),
    "ANA": ("🦆", "Анахайм"),
    "DAL": ("⭐", "Даллас"),
    "LAK": ("👑", "Лос-Анджелес"),
    "SJS": ("🦈", "Сан-Хосе"),
    "CBJ": ("💣", "Коламбус"),
    "COL": ("⛰️", "Колорадо"),
    "MIN": ("🌲", "Миннесота"),
    "WPG": ("✈️", "Виннипег"),
    "UTA": ("🦣", "Юта"),   # Utah HC
    "ARI": ("🦣", "Юта"),   # на всякий случай
    "SEA": ("🦑", "Сиэтл"),
    "VGK": ("🎰", "Вегас"),
}

# Слаги клубов на sports.ru (для прямого построения URL)
ABBR_TO_SLUG: Dict[str, str] = {
    "NJD": "new-jersey-devils",
    "NYI": "new-york-islanders",
    "NYR": "new-york-rangers",
    "PHI": "philadelphia-flyers",
    "PIT": "pittsburgh-penguins",
    "BOS": "boston-bruins",
    "BUF": "buffalo-sabres",
    "MTL": "montreal-canadiens",
    "OTT": "ottawa-senators",
    "TOR": "toronto-maple-leafs",
    "CAR": "carolina-hurricanes",
    "FLA": "florida-panthers",
    "TBL": "tampa-bay-lightning",
    "WSH": "washington-capitals",
    "CHI": "chicago-blackhawks",
    "DET": "detroit-red-wings",
    "NSH": "nashville-predators",
    "STL": "st-louis-blues",          # без точки в "st"
    "CGY": "calgary-flames",
    "EDM": "edmonton-oilers",
    "VAN": "vancouver-canucks",
    "ANA": "anaheim-ducks",
    "DAL": "dallas-stars",
    "LAK": "los-angeles-kings",
    "SJS": "san-jose-sharks",
    "CBJ": "columbus-blue-jackets",
    "COL": "colorado-avalanche",
    "MIN": "minnesota-wild",
    "WPG": "winnipeg-jets",
    "SEA": "seattle-kraken",
    "VGK": "vegas-golden-knights",
    # Юта: sports.ru использует "utah-hc" (иногда "utah-hockey-club" — попробуем оба)
    "UTA": "utah-hc",
    "ARI": "arizona-coyotes",  # старый клуб на всякий
}

# альтернативные слаги, если основной не открылся
ALT_SLUGS: Dict[str, List[str]] = {
    "STL": ["st-louis-blues", "st.-louis-blues"],
    "UTA": ["utah-hc", "utah-hockey-club", "utah"],
}

RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

# ───── HTTP с ретраями
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.6,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-RU-LiveMerge/1.2",
        "Accept": "text/html,application/json,*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    })
    return s

S = make_session()

def get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code == 200:
        return r.json()
    raise RuntimeError(f"GET {url} -> {r.status_code}")

def get_html(url: str) -> str:
    r = S.get(url, timeout=25)
    if r.status_code == 200:
        return r.text
    raise RuntimeError(f"GET {url} -> {r.status_code}")

# ───── Игровое окно (МСК: вчера 15:00 → сегодня 23:59:59)
def report_date() -> dt.date:
    env = os.getenv("REPORT_DATE","").strip()
    if env:
        return dt.date.fromisoformat(env)
    return dt.datetime.now(TZ_MSK).date()

def window_msk(d: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.combine(d - dt.timedelta(days=1), dt.time(15,0), tzinfo=TZ_MSK)
    end   = dt.datetime.combine(d, dt.time(23,59,59,999000), tzinfo=TZ_MSK)
    return start, end

def to_msk(utc_iso: str) -> dt.datetime:
    return dt.datetime.fromisoformat(utc_iso.replace("Z","+00:00")).astimezone(TZ_MSK)

# ───── NHL schedule + PBP
def pick_games(d: dt.date) -> List[dict]:
    start, end = window_msk(d)
    games: List[dict] = []
    for day in (d - dt.timedelta(days=1), d):
        js = get_json(f"{API}/v1/schedule/{day.isoformat()}")
        lst = js.get("games") or js.get("gameWeek",[{}])[0].get("games",[])
        games.extend(lst)
    picked, seen = [], set()
    for g in games:
        gid = int(g.get("id") or 0)
        if not gid or gid in seen: continue
        seen.add(gid)
        utc = g.get("startTimeUTC") or g.get("startTime")
        if not utc: continue
        msk = to_msk(utc)
        if start <= msk <= end:
            picked.append({
                "id": gid,
                "msk": msk,
                "home": g["homeTeam"]["abbrev"],
                "away": g["awayTeam"]["abbrev"],
            })
    picked.sort(key=lambda x: x["msk"])
    return picked

def nhl_play_by_play(gid: int) -> dict:
    return get_json(f"{API}/v1/gamecenter/{gid}/play-by-play")

# ───── Время
def mmss_to_seconds_any(t: str) -> int:
    t = t.strip().replace(".", ":")
    m = re.match(r"^\s*(\d{1,3})[:.](\d{2})\s*$", t)
    if not m: return 0
    return int(m.group(1))*60 + int(m.group(2))

def period_len(period: int) -> int:
    return 20*60 if period<=3 else 5*60

def to_elapsed_mmss(period: int, time_in_period: Optional[str], time_remaining: Optional[str]) -> str:
    if time_in_period and re.match(r"^\d{1,2}:\d{2}$", time_in_period):
        return time_in_period
    if time_remaining and re.match(r"^\d{1,2}:\d{2}$", time_remaining):
        mm, ss = map(int, time_remaining.split(":"))
        passed = period_len(period) - (mm*60 + ss)
        if passed < 0: passed = 0
        return f"{passed//60}:{passed%60:02d}"
    return (time_in_period or time_remaining or "0:00").replace(".", ":")

def abs_time(period: int, mmss: str) -> str:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", mmss)
    if not m:
        return mmss.replace(":", ".")
    mm, ss = int(m.group(1)), int(m.group(2))
    base = (period-1)*20 if period<=3 else 60 + 5*(period-4)
    return f"{base + mm}.{ss:02d}"

def period_from_abs(abs_sec: int) -> int:
    if abs_sec < 20*60: return 1
    if abs_sec < 40*60: return 2
    if abs_sec < 60*60: return 3
    return 4 + (abs_sec - 60*60)//(5*60)

# ───── Нормализация названий команд для календаря sports.ru
def _norm_team_key(s: str) -> str:
    t = s.lower()
    t = re.sub(r"[^a-zа-яё]+", " ", t)
    t = t.replace(" хк ", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _teams_match(a: str, b: str) -> bool:
    ak = _norm_team_key(a)
    bk = _norm_team_key(b)
    return ak == bk or ak in bk or bk in ak

def _parse_dt_from_td(a_dt_text: str) -> Tuple[Optional[dt.date], Optional[dt.time]]:
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4}).*?(\d{2}):(\d{2})", a_dt_text)
    if not m: return None, None
    d, mth, y, hh, mm = map(int, m.groups())
    try:
        return dt.date(y, mth, d), dt.time(hh, mm)
    except Exception:
        return None, None

# ───── ВАРИАНТЫ КАЛЕНДАРЯ ДЛЯ НУЖНОГО МЕСЯЦА (поддержка ?m= и ?s=…)
def _discover_season_ids_from_html(html: str) -> Set[str]:
    ids = set()
    for m in re.finditer(r"[?&]s=(\d+)", html):
        ids.add(m.group(1))
    return ids

def _calendar_urls_for_month(d_when: dt.datetime) -> List[str]:
    mm = d_when.month
    urls: List[str] = []
    urls.append(SPORTS_CAL)
    urls.append(f"{SPORTS_CAL}?m={mm}")
    try:
        base_html = get_html(SPORTS_CAL)
    except Exception:
        base_html = ""
    try:
        month_html = get_html(f"{SPORTS_CAL}?m={mm}")
    except Exception:
        month_html = ""
    season_ids = _discover_season_ids_from_html(base_html) | _discover_season_ids_from_html(month_html)
    for sid in season_ids:
        urls.append(f"{SPORTS_CAL}?s={sid}")
        urls.append(f"{SPORTS_CAL}?m={mm}&s={sid}")
    mm_prev = 12 if mm == 1 else mm-1
    mm_next = 1 if mm == 12 else mm+1
    urls.append(f"{SPORTS_CAL}?m={mm_prev}")
    urls.append(f"{SPORTS_CAL}?m={mm_next}")
    for sid in season_ids:
        urls.append(f"{SPORTS_CAL}?m={mm_prev}&s={sid}")
        urls.append(f"{SPORTS_CAL}?m={mm_next}&s={sid}")
    seen = set(); compact=[]
    for u in urls:
        if u not in seen:
            seen.add(u); compact.append(u)
    return compact

def find_sportsru_match_url_via_calendar(home_ru: str, away_ru: str, start_msk: dt.datetime) -> Optional[str]:
    candidate_pages = _calendar_urls_for_month(start_msk)
    best: Optional[Tuple[int, str]] = None
    fallback_same_day: List[str] = []

    for cal_url in candidate_pages:
        try:
            html = get_html(cal_url)
        except Exception:
            continue
        soup = BeautifulSoup(html, "html.parser")

        for tr in soup.find_all("tr"):
            td_name = tr.find("td", class_=re.compile(r"name-td"))
            td_home = tr.find("td", class_=re.compile(r"owner-td"))
            td_away = tr.find("td", class_=re.compile(r"guests-td"))
            td_score = tr.find("td", class_=re.compile(r"score-td"))
            if not (td_name and td_home and td_away and td_score):
                continue

            a_dt = td_name.find("a")
            dt_text = a_dt.get_text(" ", strip=True) if a_dt else ""
            row_date, row_time = _parse_dt_from_td(dt_text)
            if row_date is None:  # бывает строка без времени — допустим, но хуже для ранжирования
                row_date = start_msk.date()
                row_time = None

            if abs((row_date - start_msk.date()).days) > 1:
                continue

            a_home = td_home.find("a", class_=re.compile(r"player"))
            a_away = td_away.find("a", class_=re.compile(r"player"))
            home_txt = (a_home.get("title") or a_home.get_text(" ", strip=True)) if a_home else ""
            away_txt = (a_away.get("title") or a_away.get_text(" ", strip=True)) if a_away else ""

            ok_direct = _teams_match(home_txt, home_ru) and _teams_match(away_txt, away_ru)
            ok_swapped = _teams_match(home_txt, away_ru) and _teams_match(away_txt, home_ru)
            if not (ok_direct or ok_swapped):
                continue

            a_score = td_score.find("a", href=True)
            if not a_score:
                continue
            href = a_score["href"]
            if not href.startswith("http"):
                href = "https://www.sports.ru" + href

            if row_time is None:
                # если времени нет, но пара команд совпала — добавим как fallback-кандидат
                fallback_same_day.append(href)
                continue

            row_dt = dt.datetime.combine(row_date, row_time, tzinfo=TZ_MSK)
            diff_min = abs(int((row_dt - start_msk).total_seconds() // 60))
            pair = (diff_min, href)
            if (best is None) or (pair[0] < best[0]):
                best = pair

    if best is not None:
        return best[1]
    # если много кандидатов без времени, попробуем отфильтровать по слагам команд
    if fallback_same_day:
        return fallback_same_day[0]
    return None

# ───── Прямое построение URL по слагам (надёжный план C)
def _slug_variants_for_abbr(abbr: str) -> List[str]:
    base = ABBR_TO_SLUG.get(abbr)
    if not base:
        return []
    alts = ALT_SLUGS.get(abbr, [])
    return [base] + [s for s in alts if s != base]

def try_match_url_by_slugs(home_abbr: str, away_abbr: str) -> Optional[str]:
    """
    Пробуем:
      /hockey/match/<home>-vs-<away>/
      /hockey/match/<away>-vs-<home>/
    с альтернативными слагами.
    Возвращаем первый, который отдаёт 200 и по тексту похож на страницу матча.
    """
    home_slugs = _slug_variants_for_abbr(home_abbr)
    away_slugs = _slug_variants_for_abbr(away_abbr)
    pairs: List[Tuple[str, str]] = []
    for hs in home_slugs:
        for as_ in away_slugs:
            pairs.append((hs, as_))
            pairs.append((as_, hs))  # на всякий — обратный порядок

    seen = set()
    for hs, as_ in pairs:
        url = f"{SPORTS_MATCH_BASE}{hs}-vs-{as_}/"
        if url in seen: continue
        seen.add(url)
        try:
            html = get_html(url)
        except Exception:
            continue
        # грубая проверка, что это страница матча
        if re.search(r"(Трансляция|Матч|Составы|Статистика)", html, re.I):
            return url
    return None

# ───── Запасной поиск sports.ru
def find_sportsru_match_url_via_search(home_ru: str, away_ru: str, d: dt.date) -> Optional[str]:
    query = f"{home_ru} {away_ru} НХЛ {ru_date(d)} {d.year}"
    r = S.get(SPORTS_SEARCH, params={"q": query}, timeout=25)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    cands: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = a.get_text(" ", strip=True)
        if "/hockey/match/" in href and href.endswith(".html"):
            if not href.startswith("http"):
                href = "https://www.sports.ru" + href
            if (home_ru.split()[0] in txt) and (away_ru.split()[0] in txt):
                cands.append(href)
    if not cands:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/hockey/match/" in href and href.endswith(".html"):
                if not href.startswith("http"):
                    href = "https://www.sports.ru" + href
                cands.append(href)
    return cands[0] if cands else None

def find_sportsru_match_url(home_ru: str, away_ru: str, start_msk: dt.datetime, home_abbr: str, away_abbr: str) -> Optional[str]:
    # A: календарь (включая ?m=&s=)
    u = find_sportsru_match_url_via_calendar(home_ru, away_ru, start_msk)
    if u: return u
    # B: поиск
    for delta in (0, -1, 1):
        u = find_sportsru_match_url_via_search(home_ru, away_ru, (start_msk + dt.timedelta(days=delta)).date())
        if u: return u
    # C: прямой слаг
    u = try_match_url_by_slugs(home_abbr, away_abbr)
    if u: return u
    return None

# ───── ТРАНСЛЯЦИЯ sports.ru: извлекаем «Гол!» + ассистентов + абсолютное время (mm:ss)
CYR_WORD = r"[А-ЯЁ][а-яё\-]+"
TIME_RE = re.compile(r"\b(\d{1,3}):([0-5]\d)\b")

def _extract_lastname(s: str) -> Optional[str]:
    tokens = re.findall(CYR_WORD, s)
    return tokens[-1] if tokens else None

def parse_sportsru_live_goals(url: str) -> Tuple[List[dict], Optional[str]]:
    html = get_html(url)
    soup = BeautifulSoup(html, "html.parser")
    lines = [ln.strip() for ln in soup.get_text("\n").split("\n") if ln.strip()]
    goals: List[dict] = []
    so_winner: Optional[str] = None

    for ln in lines:
        m = re.search(r"Победный\s+буллит[:\s–-]+(.+)", ln, re.I)
        if m:
            ln_full = m.group(1).strip()
            last = _extract_lastname(ln_full)
            if last: so_winner = last
            break

    i = 0
    n = len(lines)
    while i < n:
        ln = lines[i]
        if re.search(r"\bГол!\b", ln, re.I):
            ctx = ln + " " + (lines[i+1] if i+1<n else "")
            m_name = re.search(r"Гол!\s*([^,\n\r]+)", ctx, re.I)
            lastname = None
            if m_name:
                lastname = _extract_lastname(m_name.group(1))
            if not lastname:
                lastname = _extract_lastname(lines[i+1]) if i+1<n else None

            assistants: List[str] = []
            for j in range(i, min(i+4, n)):
                m_ass = re.search(r"Ассистент[ы]?:\s*(.+)", lines[j], re.I)
                if m_ass:
                    raw = m_ass.group(1)
                    parts = re.split(r",|\s+и\s+", raw)
                    for p in parts:
                        last = _extract_lastname(p.strip())
                        if last:
                            assistants.append(last)
                    break

            abs_mmss = None
            for j in range(i, min(i+4, n)):
                m_t = TIME_RE.search(lines[j])
                if m_t:
                    abs_mmss = f"{int(m_t.group(1))}:{m_t.group(2)}"
                    break
            if abs_mmss is None:
                for j in range(max(0, i-2), i):
                    m_t = TIME_RE.search(lines[j])
                    if m_t:
                        abs_mmss = f"{int(m_t.group(1))}:{m_t.group(2)}"
                        break

            if lastname and abs_mmss:
                goals.append({"abs": abs_mmss, "who": lastname, "assists": assistants})
            i += 1
        else:
            i += 1

    return goals, so_winner

# ───── Фолбэк-парсер (если live не нашёлся)
GOAL_LINE_RE = re.compile(
    r"(?P<score>\d+:\d+)\s*[—–-]\s*(?P<time>\d{1,2}[:.]\d{2})\s+(?P<who>[А-ЯЁ][^()\n\r]*?)(?:\s*\((?P<ass>[^)]*)\))?(?=\s|$)",
    re.U
)
PERIOD_HEADERS = [
    (re.compile(r"\b1[-–]?й\s+период\b", re.I | re.U), 1),
    (re.compile(r"\b2[-–]?й\s+период\b", re.I | re.U), 2),
    (re.compile(r"\b3[-–]?й\s+период\b", re.I | re_U), 3),
    (re.compile(r"\bОвертайм(?:\s*№\s*(\d+))?\b", re.I | re.U), 4),
]

def ru_initial(full: str) -> str:
    t = re.sub(r"\s+", " ", (full or "").strip())
    if not t: return ""
    parts = t.split(" ")
    if len(parts) == 1: return parts[0]
    return f"{parts[0][0]}. {parts[-1]}"

def parse_sportsru_goals_fallback(url: str) -> Tuple[List[dict], Optional[str]]:
    html = get_html(url)
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text("\n", strip=True)
    txt = txt.replace("—", "–").replace("−", "–").replace("‒", "–")

    start = None
    for m in re.finditer(r"(1[-–]?й\s+период|Голы|Ход матча)", txt, re.I):
        start = m.start(); break
    if start is None: start = 0
    endm = re.search(r"(Буллиты|Серия буллитов|Удаления|Статистика|Составы)", txt, re.I)
    end = endm.start() if endm else len(txt)
    section = txt[start:end]

    period = 1
    goals: List[dict] = []
    lines = [ln.strip() for ln in section.split("\n") if ln.strip()]
    for ln in lines:
        switched = False
        for rx, base in PERIOD_HEADERS:
            m = rx.search(ln)
            if m:
                if base == 4 and m.lastindex == 1 and m.group(1):
                    period = 3 + max(1, int(m.group(1)))
                else:
                    period = base
                switched = True
                break
        if switched: continue

        for m in GOAL_LINE_RE.finditer(ln):
            mmss = m.group("time").replace(".", ":")
            who_full = m.group("who").strip()
            ass = (m.group("ass") or "").strip()
            who = ru_initial(re.split(r"\s+[–-]\s+", who_full)[0].strip())
            assistants: List[str] = []
            if ass:
                for a in ass.split(","):
                    aa = ru_initial(re.split(r"\s+[–-]\s+", a.strip())[0].strip())
                    if aa: assistants.append(aa)
            goals.append({
                "abs": None,
                "who": who,
                "assists": assistants,
                "score_line": m.group("score"),
                "p_rel": mmss,
                "period": period,
            })

    so_winner = None
    m = re.search(r"Победный\s+буллит[:\s–-]+([А-ЯЁ][^,\n\r]+)", txt, re.I)
    if m:
        so_winner = ru_initial(m.group(1).strip())

    return goals, so_winner

# ───── Сопоставление со счётом НХЛ
def attach_scores_from_nhl(nhl_goals: List[dict], ru_goals: List[dict]) -> List[dict]:
    nhl_abs: List[Tuple[int, str]] = []  # (sec, score)
    for ev in nhl_goals:
        abs_s = mmss_to_seconds_any(abs_time(ev["period"], ev["t"]))
        nhl_abs.append((abs_s, ev["score"]))

    used = [False]*len(nhl_abs)
    out: List[dict] = []
    for rg in ru_goals:
        sec = mmss_to_seconds_any(rg["abs"])
        best = None
        for j, (s, score) in enumerate(nhl_abs):
            if used[j]: continue
            diff = abs(s - sec)
            if diff <= 45:
                if (best is None) or diff < best[0]:
                    best = (diff, j)
        if best is None:
            j = next((k for k, u in enumerate(used) if not u), None)
            if j is not None:
                used[j] = True
                s, score = nhl_abs[j]
                out.append({
                    "score": score,
                    "abs": f"{sec//60}.{sec%60:02d}",
                    "period": period_from_abs(sec),
                    "who": rg["who"], "assists": rg["assists"],
                })
            else:
                out.append({
                    "score": "?:?",
                    "abs": f"{sec//60}.{sec%60:02d}",
                    "period": period_from_abs(sec),
                    "who": rg["who"], "assists": rg["assists"],
                })
        else:
            j = best[1]
            used[j] = True
            s, score = nhl_abs[j]
            out.append({
                "score": score,
                "abs": f"{sec//60}.{sec%60:02d}",
                "period": period_from_abs(sec),
                "who": rg["who"], "assists": rg["assists"],
            })
    return out

# ───── Блок матча
def build_match_block(g: dict) -> str:
    gid = g["id"]
    pbp = nhl_play_by_play(gid)

    final_home = pbp.get("homeTeam", {}).get("score", 0)
    final_away = pbp.get("awayTeam", {}).get("score", 0)
    decision = (pbp.get("gameOutcome") or {}).get("lastPeriodType")  # REG/OT/SO

    # Голы NHL (в хронологическом порядке) — счёт из details.*
    nhl_goals: List[dict] = []
    for ev in pbp.get("plays", []):
        if str(ev.get("typeDescKey", "")).lower() != "goal":
            continue
        per = int((ev.get("periodDescriptor") or {}).get("number") or ev.get("period") or 0)
        t_elapsed = to_elapsed_mmss(per, ev.get("timeInPeriod"), ev.get("timeRemaining"))
        det = ev.get("details") or {}
        hs = det.get("homeScore", ev.get("homeScore", 0)) or 0
        as_ = det.get("awayScore", ev.get("awayScore", 0)) or 0
        nhl_goals.append({"period": per, "t": t_elapsed, "score": f"{hs}:{as_}"})

    # Страница матча на sports.ru
    h_emoji, h_ru = TEAM_META.get(g["home"], ("🏒", g["home"]))
    a_emoji, a_ru = TEAM_META.get(g["away"], ("🏒", g["away"]))
    url = find_sportsru_match_url(h_ru, a_ru, g["msk"], g["home"], g["away"])
    if not url:
        raise RuntimeError(f"Не найден матч на sports.ru для {h_ru} — {a_ru} ({g['msk']:%d.%m})")

    ru_live, so_winner = parse_sportsru_live_goals(url)
    if not ru_live:
        ru_fb, so2 = parse_sportsru_goals_fallback(url)
        if ru_fb:
            ru_live = []
            for it in ru_fb:
                if it.get("abs"):
                    abs_txt = it["abs"]
                elif it.get("period") and it.get("p_rel"):
                    abs_txt = abs_time(it["period"], it["p_rel"]).replace(".", ":")
                else:
                    abs_txt = "0:00"
                who = (re.findall(r"[А-ЯЁ][а-яё\-]+", it["who"]) or [it["who"]])[-1]
                ru_live.append({"abs": abs_txt, "who": who, "assists": [ (re.findall(r'[А-ЯЁ][а-яё\-]+', a) or [a])[-1] for a in it["assists"] ]})
        if (not so_winner) and so2:
            so_winner = (re.findall(r"[А-ЯЁ][а-яё\-]+", so2) or [so2])[-1]

    rows = attach_scores_from_nhl(nhl_goals, ru_live) if ru_live else []

    # Заголовок (жирным победителя)
    home_line = f"{h_emoji} «{h_ru}»: {final_home}"
    away_line = f"{a_emoji} «{a_ru}»: {final_away}"
    if final_home > final_away:
        home_line = f"<b>{home_line}</b>"
    elif final_away > final_home:
        away_line = f"<b>{away_line}</b>"
    suffix = " (ОТ)" if decision == "OT" else " (Б)" if decision == "SO" else ""
    parts = [home_line + suffix, away_line, ""]

    # По периодам
    by_p: Dict[int, List[str]] = {}
    for r in rows:
        line = f"{r['score']} – {r['abs']} {r['who']}"
        if r["assists"]:
            line += f" ({', '.join(r['assists'])})"
        by_p.setdefault(r["period"], []).append(line)

    for p in sorted(by_p.keys()):
        parts.append(f"<i>{p}-й период</i>" if p <= 3 else f"<i>Овертайм №{p-3}</i>")
        lines = []
        for ln in by_p[p]:
            ln = re.sub(r"(\d{1,3}):(\d{2})(\s+)", lambda m: f"{int(m.group(1))}.{m.group(2)}{m.group(3)}", ln, count=1)
            lines.append(ln)
        parts.extend(lines)

    if decision == "SO" and so_winner:
        parts.append("Победный буллит")
        parts.append(so_winner)

    return "\n".join(parts)

# ───── Пост целиком
def build_post(d: dt.date) -> str:
    games = pick_games(d)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(d)} • {len(games)} " + \
            ("матч" if len(games)==1 else "матча" if len(games)%10 in (2,3,4) and not 12<=len(games)%100<=14 else "матчей")
    head = f"{title}\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    if not games:
        return head.strip()

    blocks: List[str] = []
    for i, g in enumerate(games, 1):
        blocks.append(build_match_block(g))
        if i < len(games): blocks.append("")
    return head + "\n".join(blocks).strip()

# ───── Telegram
def tg_send(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat  = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat:
        print(text); return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = S.post(url, json={"chat_id": chat, "text": text, "parse_mode":"HTML", "disable_web_page_preview": True}, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:200]}")

# ───── main
if __name__ == "__main__":
    try:
        d = report_date()
        msg = build_post(d)
        tg_send(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
