#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL → Telegram (RU) + подробный DEBUG

Источники:
  • NHL (api-web.nhle.com) — расписание, итоговый счёт, PBP (время и счёт после каждого гола)
  • sports.ru страница матча («Трансляция») — авторы голов и ассистенты (по-русски), время в формате mm:ss
    (если LIVE не нашлась/пуста — фолбэк-парсер текста страницы матча)

Формат строки гола:
<счёт после гола> – <абсолютное время mm.ss> <Фамилия> (<Фамилия>, <Фамилия>)

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  REPORT_DATE=YYYY-MM-DD     — отчётный день по МСК (по умолчанию: сегодня)
  REPORT_DATE_PT=YYYY-MM-DD  — если хотите собирать за PT-день и запуск делать по PT
  DEBUG=1|0                  — включить/отключить подробные логи (по умолчанию 1)
  HTTP_TIMEOUT=12            — таймаут запроса (сек)
  HTTP_RETRIES=3             — число попыток запроса
  HTTP_BACKOFF=0.6           — экспоненциальный backoff между ретраями (сек)
"""

import os, sys, re, time, datetime as dt
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup

# ───── Debug helpers ───────────────────────────────────────────────────────────

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "")
    if v == "": return default
    return v not in ("0", "false", "False", "no", "NO")

DEBUG = _env_bool("DEBUG", True)

def dbg(msg: str):
    if DEBUG:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        print(f"[DBG {ts}] {msg}", flush=True)

def section(title: str):
    dbg("—"*70)
    dbg(title)

# ───── Константы и настройки ──────────────────────────────────────────────────

TZ_MSK = ZoneInfo("Europe/Moscow")
TZ_PT  = ZoneInfo("America/Los_Angeles")
API = "https://api-web.nhle.com"
SPORTS_CAL = "https://www.sports.ru/hockey/tournament/nhl/calendar/"
SPORTS_SEARCH = "https://www.sports.ru/search/"
SPORTS_MATCH_BASE = "https://www.sports.ru/hockey/match/"

HTTP_TIMEOUT  = float(os.getenv("HTTP_TIMEOUT", "12"))
HTTP_RETRIES  = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_BACKOFF  = float(os.getenv("HTTP_BACKOFF", "0.6"))

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
    "UTA": ("🦣", "Юта"),
    "ARI": ("🦣", "Юта"),
    "SEA": ("🦑", "Сиэтл"),
    "VGK": ("🎰", "Вегас"),
}

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
    "STL": "st-louis-blues",
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
    "UTA": "utah-hc",
    "ARI": "arizona-coyotes",
}
ALT_SLUGS: Dict[str, List[str]] = {
    "STL": ["st-louis-blues", "st.-louis-blues"],
    "UTA": ["utah-hc", "utah-hockey-club", "utah"],
}

RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

# ───── HTTP с ручными ретраями + лог ──────────────────────────────────────────

UA = "NHL-RU-LiveMerge/1.4 (debug)"

def http_get(url: str, expect: str = "text") -> str | dict:
    """GET с ретраями и подробным логом."""
    headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    }
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        t0 = time.monotonic()
        try:
            dbg(f"HTTP GET [{attempt}/{HTTP_RETRIES}] {url}")
            r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            elapsed = time.monotonic() - t0
            dbg(f" → status={r.status_code} elapsed={elapsed:.2f}s len={len(r.content)}")
            if r.status_code == 200:
                return r.json() if expect == "json" else r.text
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            elapsed = time.monotonic() - t0
            dbg(f" ! HTTP error attempt {attempt}: {e} (elapsed {elapsed:.2f}s)")
            last_err = e
        if attempt < HTTP_RETRIES:
            back = HTTP_BACKOFF * (2 ** (attempt - 1))
            dbg(f"   retry in {back:.2f}s …")
            time.sleep(back)
    raise RuntimeError(f"GET failed {url} -> {last_err}")

def get_json(url: str) -> dict:
    return http_get(url, expect="json")  # type: ignore[return-value]

def get_html(url: str) -> str:
    return http_get(url, expect="text")  # type: ignore[return-value]

# ───── Дата-окно отчёта ───────────────────────────────────────────────────────

def report_date() -> dt.date:
    # приоритет REPORT_DATE_PT (если приходит из автозапуска по PT-дню)
    s_pt = os.getenv("REPORT_DATE_PT", "").strip()
    if s_pt:
        try:
            d = dt.date.fromisoformat(s_pt)
            dbg(f"REPORT_DATE_PT found: {d}")
            return d  # используем как «дату дня» в заголовке
        except Exception:
            dbg("Invalid REPORT_DATE_PT, ignore")

    s = os.getenv("REPORT_DATE","").strip()
    if s:
        try:
            d = dt.date.fromisoformat(s)
            dbg(f"REPORT_DATE found: {d}")
            return d
        except Exception:
            dbg("Invalid REPORT_DATE, ignore")
    d = dt.datetime.now(TZ_MSK).date()
    dbg(f"No report date in env, using MSK today: {d}")
    return d

def window_msk(d: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.combine(d - dt.timedelta(days=1), dt.time(15,0), tzinfo=TZ_MSK)
    end   = dt.datetime.combine(d, dt.time(23,59,59,999000), tzinfo=TZ_MSK)
    dbg(f"MSK window: {start} .. {end}")
    return start, end

def to_msk(utc_iso: str) -> dt.datetime:
    return dt.datetime.fromisoformat(utc_iso.replace("Z","+00:00")).astimezone(TZ_MSK)

# ───── NHL schedule + PBP ─────────────────────────────────────────────────────

def pick_games(d: dt.date) -> List[dict]:
    """Берём только матчи FINAL/OFF, стартовавшие в МСК-окне."""
    section("NHL: расписание")
    start, end = window_msk(d)
    games: List[dict] = []
    for day in (d - dt.timedelta(days=1), d):
        url = f"{API}/v1/schedule/{day.isoformat()}"
        dbg(f"Fetch schedule {url}")
        js = get_json(url)
        lst = js.get("games")
        if lst is None:
            week = js.get("gameWeek") or []
            lst = []
            for w in week:
                lst.extend(w.get("games") or [])
        dbg(f"  schedule {day}: {len(lst or [])} items")
        games.extend(lst or [])

    def _is_final(g) -> bool:
        st = (g.get("gameState") or "").upper()
        return st in ("FINAL", "OFF")

    picked, seen = [], set()
    future_cnt = 0
    in_window_cnt = 0
    final_cnt = 0

    for g in games:
        gid = int(g.get("id") or 0)
        if not gid or gid in seen:
            continue
        seen.add(gid)
        utc = g.get("startTimeUTC") or g.get("startTime")
        if not utc:
            continue
        msk = to_msk(utc)
        if start <= msk <= end:
            in_window_cnt += 1
            if _is_final(g):
                final_cnt += 1
                picked.append({
                    "id": gid,
                    "msk": msk,
                    "home": g["homeTeam"]["abbrev"],
                    "away": g["awayTeam"]["abbrev"],
                })
            else:
                future_cnt += 1

    picked.sort(key=lambda x: x["msk"])
    dbg(f"in_window={in_window_cnt}, finals={final_cnt}, not_final_yet={future_cnt}")
    dbg(f"Picked FINAL/OFF within MSK window: {len(picked)}")
    for p in picked:
        dbg(f"  game {p['id']}: {p['away']}@{p['home']} start_msk={p['msk']}")
    return picked

def nhl_play_by_play(gid: int) -> dict:
    url = f"{API}/v1/gamecenter/{gid}/play-by-play"
    dbg(f"NHL PBP: {url}")
    js = get_json(url)
    plays = len(js.get("plays", []))
    dbg(f"  PBP plays={plays}, homeScore={js.get('homeTeam',{}).get('score')}, awayScore={js.get('awayTeam',{}).get('score')}")
    return js

# ───── Время ──────────────────────────────────────────────────────────────────

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

# ───── Нормализация команд (sports.ru календарь) ──────────────────────────────

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

# ───── Календарь sports.ru: варианты страниц ──────────────────────────────────

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

    season_ids = set()
    try:
        base_html = get_html(SPORTS_CAL)
        season_ids |= _discover_season_ids_from_html(base_html)
    except Exception as e:
        dbg(f"calendar base fetch failed: {e}")
    try:
        month_html = get_html(f"{SPORTS_CAL}?m={mm}")
        season_ids |= _discover_season_ids_from_html(month_html)
    except Exception as e:
        dbg(f"calendar m={mm} fetch failed: {e}")

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
    dbg(f"calendar candidates: {len(compact)}")
    return compact

def find_sportsru_match_url_via_calendar(home_ru: str, away_ru: str, start_msk: dt.datetime) -> Optional[str]:
    section(f"sports.ru: поиск через календарь — {home_ru} vs {away_ru} @ {start_msk:%d.%m %H:%M} MSK")
    candidate_pages = _calendar_urls_for_month(start_msk)
    best: Optional[Tuple[int, str]] = None
    fallback_same_day: List[str] = []

    for cal_url in candidate_pages:
        dbg(f"calendar page: {cal_url}")
        try:
            html = get_html(cal_url)
        except Exception as e:
            dbg(f"  fetch failed: {e}")
            continue
        soup = BeautifulSoup(html, "html.parser")

        rows = 0; matched = 0
        for tr in soup.find_all("tr"):
            td_name = tr.find("td", class_=re.compile(r"name-td"))
            td_home = tr.find("td", class_=re.compile(r"owner-td"))
            td_away = tr.find("td", class_=re.compile(r"guests-td"))
            td_score = tr.find("td", class_=re.compile(r"score-td"))
            if not (td_name and td_home and td_away and td_score):
                continue
            rows += 1

            a_dt = td_name.find("a")
            dt_text = a_dt.get_text(" ", strip=True) if a_dt else ""
            row_date, row_time = _parse_dt_from_td(dt_text)
            if row_date is None:
                continue
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
            matched += 1

            a_score = td_score.find("a", href=True)
            if not a_score:
                continue
            href = a_score["href"]
            if not href.startswith("http"):
                href = "https://www.sports.ru" + href

            if row_time is None:
                fallback_same_day.append(href)
                continue

            row_dt = dt.datetime.combine(row_date, row_time, tzinfo=TZ_MSK)
            diff_min = abs(int((row_dt - start_msk).total_seconds() // 60))
            pair = (diff_min, href)
            if (best is None) or (pair[0] < best[0]):
                best = pair
        dbg(f"  rows={rows}, matched_by_names={matched}")

    if best is not None:
        dbg(f"calendar matched url: {best[1]} (Δ={best[0]}m)")
        return best[1]
    if len(fallback_same_day) == 1:
        dbg(f"calendar fallback same-day url: {fallback_same_day[0]}")
        return fallback_same_day[0]
    dbg("calendar search: not found")
    return None

# ───── Прямые слаги матча ─────────────────────────────────────────────────────

def _slug_variants_for_abbr(abbr: str) -> List[str]:
    base = ABBR_TO_SLUG.get(abbr)
    if not base:
        return []
    alts = ALT_SLUGS.get(abbr, [])
    return [base] + [s for s in alts if s != base]

def try_match_url_by_slugs(home_abbr: str, away_abbr: str) -> Optional[str]:
    section("sports.ru: пробуем прямые слаги")
    home_slugs = _slug_variants_for_abbr(home_abbr)
    away_slugs = _slug_variants_for_abbr(away_abbr)
    pairs: List[Tuple[str, str]] = []
    for hs in home_slugs:
        for as_ in away_slugs:
            pairs.append((hs, as_))
            pairs.append((as_, hs))
    seen = set()
    for hs, as_ in pairs:
        url = f"{SPORTS_MATCH_BASE}{hs}-vs-{as_}/"
        if url in seen: continue
        seen.add(url)
        try:
            html = get_html(url)
        except Exception as e:
            dbg(f" slug {hs}-vs-{as_}: fetch failed: {e}")
            continue
        if re.search(r"(Трансляция|Матч|Составы|Статистика)", html, re.I):
            dbg(f" slug matched: {url}")
            return url
    dbg(" slug search: not found")
    return None

# ───── Поиск sports.ru через их поиск ─────────────────────────────────────────

def find_sportsru_match_url_via_search(home_ru: str, away_ru: str, d: dt.date) -> Optional[str]:
    section(f"sports.ru: поиск по сайту {ru_date(d)}")
    query = f"{home_ru} {away_ru} НХЛ {ru_date(d)} {d.year}"
    dbg(f"search query: {query}")
    try:
        r = requests.get(SPORTS_SEARCH, params={"q": query}, timeout=HTTP_TIMEOUT, headers={"User-Agent": UA})
        dbg(f" search status={r.status_code} len={len(r.content)} url={r.url}")
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
        if cands:
            dbg(f" search candidate: {cands[0]}")
            return cands[0]
    except Exception as e:
        dbg(f" search failed: {e}")
    dbg(" search: not found")
    return None

def find_sportsru_match_url(home_ru: str, away_ru: str, start_msk: dt.datetime, home_abbr: str, away_abbr: str) -> Optional[str]:
    u = find_sportsru_match_url_via_calendar(home_ru, away_ru, start_msk)
    if u: return u
    for delta in (0, -1, 1):
        u = find_sportsru_match_url_via_search(home_ru, away_ru, (start_msk + dt.timedelta(days=delta)).date())
        if u: return u
    u = try_match_url_by_slugs(home_abbr, away_abbr)
    if u: return u
    return None

# ───── ТРАНСЛЯЦИЯ sports.ru: LIVE-парсер ──────────────────────────────────────

CYR_WORD = r"[А-ЯЁ][а-яё\-]+"
TIME_RE = re.compile(r"\b(\d{1,3}):([0-5]\d)\b")

def _extract_lastname(s: str) -> Optional[str]:
    tokens = re.findall(CYR_WORD, s)
    return tokens[-1] if tokens else None

def parse_sportsru_live_goals(url: str) -> Tuple[List[dict], Optional[str]]:
    section(f"sports.ru LIVE: {url}")
    html = get_html(url)
    soup = BeautifulSoup(html, "html.parser")
    lines = [ln.strip() for ln in soup.get_text("\n").split("\n") if ln.strip()]
    goals: List[dict] = []
    so_winner: Optional[str] = None

    # Победный буллит (если есть)
    for ln in lines:
        m = re.search(r"Победный\s+буллит[:\s–-]+(.+)", ln, re.I)
        if m:
            ln_full = m.group(1).strip()
            last = _extract_lastname(ln_full)
            if last: so_winner = last
            dbg(f" live: победный буллит — {so_winner}")
            break

    found_goals = 0
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
            if not lastname and i+1<n:
                lastname = _extract_lastname(lines[i+1])

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
                found_goals += 1
        i += 1

    dbg(f" live goals parsed: {found_goals}")
    return goals, so_winner

# ───── Фолбэк-парсер блока голов (если LIVE не нашлась) ───────────────────────

GOAL_LINE_RE = re.compile(
    r"(?P<score>\d+:\d+)\s*[—–-]\s*(?P<time>\d{1,2}[:.]\d{2})\s+(?P<who>[А-ЯЁ][^()\n\r]*?)(?:\s*\((?P<ass>[^)]*)\))?(?=\s|$)",
    re.U
)
PERIOD_HEADERS = [
    (re.compile(r"\b1[-–]?й\s+период\b", re.I | re.U), 1),
    (re.compile(r"\b2[-–]?й\s+период\b", re.I | re.U), 2),
    (re.compile(r"\b3[-–]?й\s+период\b", re.I | re.U), 3),
    (re.compile(r"\bОвертайм(?:\s*№\s*(\d+))?\b", re.I | re.U), 4),
]

def ru_initial(full: str) -> str:
    t = re.sub(r"\s+", " ", (full or "").strip())
    if not t: return ""
    parts = t.split(" ")
    if len(parts) == 1: return parts[0]
    return f"{parts[0][0]}. {parts[-1]}"

def parse_sportsru_goals_fallback(url: str) -> Tuple[List[dict], Optional[str]]:
    section(f"sports.ru fallback parser: {url}")
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
    section_txt = txt[start:end]

    period = 1
    goals: List[dict] = []
    lines = [ln.strip() for ln in section_txt.split("\n") if ln.strip()]
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

    dbg(f" fallback goals parsed: {len(goals)}, so_winner={so_winner}")
    return goals, so_winner

# ───── Сопоставление со счётом NHL ────────────────────────────────────────────

def attach_scores_from_nhl(nhl_goals: List[dict], ru_goals: List[dict]) -> List[dict]:
    section("Сопоставление голов (sports.ru ↔ NHL)")
    nhl_abs: List[Tuple[int, str]] = []
    for ev in nhl_goals:
        abs_s = mmss_to_seconds_any(abs_time(ev["period"], ev["t"]))
        nhl_abs.append((abs_s, ev["score"]))
    dbg(f" NHL goals: {len(nhl_abs)}")

    used = [False]*len(nhl_abs)
    out: List[dict] = []
    matched = 0
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
            matched += 1
    dbg(f" matched={matched}/{len(ru_goals)}")
    return out

# ───── Формирование блока матча ───────────────────────────────────────────────

def build_match_block(g: dict) -> str:
    section(f"Матч {g['id']} — {g['away']}@{g['home']} MSK {g['msk']:%Y-%m-%d %H:%M}")
    gid = g["id"]
    pbp = nhl_play_by_play(gid)

    final_home = pbp.get("homeTeam", {}).get("score", 0)
    final_away = pbp.get("awayTeam", {}).get("score", 0)
    decision = (pbp.get("gameOutcome") or {}).get("lastPeriodType")  # REG/OT/SO
    dbg(f" final score: away={final_away} home={final_home} decision={decision}")

    # Список голов NHL (с периодом, временем и счётом)
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
    dbg(f" NHL goals parsed: {len(nhl_goals)}")

    # Поиск страницы матча на sports.ru
    h_emoji, h_ru = TEAM_META.get(g["home"], ("🏒", g["home"]))
    a_emoji, a_ru = TEAM_META.get(g["away"], ("🏒", g["away"]))
    url = find_sportsru_match_url(h_ru, a_ru, g["msk"], g["home"], g["away"])
    dbg(f"sports.ru URL: {url or 'NOT FOUND'}")
    if not url:
        raise RuntimeError(f"Не найден матч на sports.ru для {h_ru} — {a_ru} ({g['msk']:%d.%m})")

    # Парсим LIVE; если пусто — фолбэк
    ru_live, so_winner = parse_sportsru_live_goals(url)
    if not ru_live:
        dbg(" live empty → try fallback parser")
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
    dbg(f" sports.ru goals (live/fallback): {len(ru_live)}, so_winner={so_winner}")
    if ru_live:
        dbg(" first 3 goals sample: " + " | ".join([f"{g['abs']} {g['who']}" for g in ru_live[:3]]))

    rows = attach_scores_from_nhl(nhl_goals, ru_live) if ru_live else []
    if not rows:
        dbg(" sports.ru goals missing → no rows after attach; will show placeholder")

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

    if not by_p:
        parts.append("— события матча недоступны")
        return "\n".join(parts)

    for p in sorted(by_p.keys()):
        parts.append(f"<i>{p}-й период</i>" if p <= 3 else f"<i>Овертайм №{p-3}</i>")
        lines = []
        for ln in by_p[p]:
            ln = re.sub(r"(\d{1,3}):(\d{2})(\s+)", lambda m: f"{int(m.group(1))}.{m.group(2)}{m.group(3)}", ln, count=1)
            lines.append(ln)
        parts.extend(lines)

    if suffix == " (Б)" and so_winner:
        parts.append("Победный буллит")
        parts.append(so_winner)

    return "\n".join(parts)

# ───── Пост целиком ───────────────────────────────────────────────────────────

def build_post(d: dt.date) -> str:
    section(f"Сборка поста за {d.isoformat()}")
    games = pick_games(d)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(d)} • {len(games)} " + \
            ("матч" if len(games)==1 else "матча" if len(games)%10 in (2,3,4) and not 12<=len(games)%100<=14 else "матчей")
    head = f"{title}\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    if not games:
        dbg("Нет матчей в окне — возвращаем только шапку")
        return head.strip()

    blocks: List[str] = []
    for i, g in enumerate(games, 1):
        t0 = time.monotonic()
        try:
            block = build_match_block(g)
        except Exception as e:
            dbg(f"[ERR ] build_match_block {g['id']}: {e}")
            h_emoji, h_ru = TEAM_META.get(g["home"], ("🏒", g["home"]))
            a_emoji, a_ru = TEAM_META.get(g["away"], ("🏒", g["away"]))
            block = f"{h_emoji} «{h_ru}»: 0\n{a_emoji} «{a_ru}»: 0\n\n— события матча недоступны"
        elapsed = time.monotonic() - t0
        dbg(f" match {g['id']} built in {elapsed:.2f}s")
        blocks.append(block)
        if i < len(games): blocks.append("")
    body = "\n".join(blocks).strip()
    return head + body

# ───── Telegram ───────────────────────────────────────────────────────────────

def tg_send(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat  = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat:
        dbg("No TELEGRAM env provided — printing to stdout only")
        print(text)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    dbg(f"Telegram send: {len(text)} chars")
    r = requests.post(url, json={"chat_id": chat, "text": text, "parse_mode":"HTML", "disable_web_page_preview": True},
                      timeout=HTTP_TIMEOUT, headers={"User-Agent": UA})
    dbg(f" Telegram response: {r.status_code} len={len(r.content)}")
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:200]}")

# ───── main ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        d = report_date()
        t0 = time.monotonic()
        msg = build_post(d)
        t1 = time.monotonic()
        dbg(f"Post built in {t1 - t0:.2f}s, sending…")
        tg_send(msg)
        dbg("DONE")
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
