#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, time, textwrap, re, unicodedata
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup

# === SPORTS.RU HELPERS (inline, no extra files) ==============================
# Этот блок правит только слуг Юты (utah-mammoth) и даёт аккуратный список кандидатов URL.
# Остальной пайплайн оставляем без изменений.

import re
import time
import logging
from typing import Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

_SLUG_SAFE = re.compile(r"[^a-z0-9]+")

def _normalize(s: str) -> str:
    return _SLUG_SAFE.sub(" ", s.lower()).strip()

def _slugify_en(s: str) -> str:
    s = s.lower().replace("&", " and ").replace("'", "").replace(".", "")
    s = _SLUG_SAFE.sub("-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
    return s

# Алиасы → целевой sports.ru slug (главное — Юта)
TEAM_SLUG_OVERRIDES: Dict[str, str] = {}
def _add_alias(value: str, *aliases: str) -> None:
    for a in aliases:
        TEAM_SLUG_OVERRIDES[_normalize(a)] = value

# NHL (короткий набор + ключевые кейсы, можно расширять при желании)
_add_alias("montreal-canadiens", "montreal canadiens", "montreal", "mtl")
_add_alias("utah-mammoth", "utah mammoth", "utah", "utah hc", "utah hockey club",
           "utah-hc", "utah-hockey-club", "utah nhl", "utah mammoths", "uta")
_add_alias("san-jose-sharks", "san jose sharks", "san jose", "san-jose", "sj", "sjs")
_add_alias("tampa-bay-lightning", "tampa bay lightning", "tampa bay", "tampa-bay", "tampa", "tbl")
_add_alias("st-louis-blues", "st louis blues", "st. louis blues", "st-louis", "st louis", "st. louis", "stl")
_add_alias("new-york-islanders", "new york islanders", "ny islanders", "islanders", "nyi")
_add_alias("new-york-rangers", "new york rangers", "ny rangers", "rangers", "nyr")

# Фоллбэк «городских» слугов (когда sports.ru даёт короткие урлы)
CITY_SLUG: Dict[str, str] = {}
def _add_city(value: str, *aliases: str) -> None:
    for a in aliases:
        CITY_SLUG[_normalize(a)] = value

_add_city("montreal", "montreal canadiens", "montreal", "mtl")
_add_city("utah", "utah", "utah hc", "utah hockey club", "utah mammoth", "uta")
_add_city("san-jose", "san jose", "san-jose", "san jose sharks", "sj", "sjs")
_add_city("tampa-bay", "tampa", "tampa bay", "tampa-bay", "tampa-bay lightning", "tbl")
_add_city("st-louis", "st louis", "st. louis", "st-louis", "st louis blues", "stl")
_add_city("new-york", "new york", "new-york", "new york rangers", "new york islanders")

def team_slug(name: str) -> str:
    k = _normalize(name)
    if k in TEAM_SLUG_OVERRIDES:
        return TEAM_SLUG_OVERRIDES[k]
    return _slugify_en(name)

def city_slug(name: str) -> str:
    k = _normalize(name)
    if k in CITY_SLUG:
        return CITY_SLUG[k]
    full = team_slug(name)
    parts = full.split("-")
    if parts[:2] in (["new", "york"], ["st", "louis"]):
        return "-".join(parts[:2])
    if len(parts) >= 2 and parts[0] == "tampa" and parts[1] == "bay":
        return "tampa-bay"
    return parts[0]

def _match_paths(home_slug: str, away_slug: str):
    yield f"/hockey/match/{home_slug}-vs-{away_slug}/stat/"
    yield f"/hockey/match/{away_slug}-vs-{home_slug}/stat/"
    yield f"/hockey/match/{home_slug}-vs-{away_slug}/"
    yield f"/hockey/match/{away_slug}-vs-{home_slug}/"

def build_sports_ru_urls(home_team: str, away_team: str, base: str = "https://www.sports.ru") -> List[str]:
    hs, as_ = team_slug(home_team), team_slug(away_team)
    hc, ac = city_slug(home_team), city_slug(away_team)
    tried: List[str] = []

    # полные слуги
    tried.extend(list(_match_paths(hs, as_)))
    # короткие «городские»
    if (hc, ac) != (hs, as_):
        tried.extend(list(_match_paths(hc, ac)))
    # смешанные варианты (на всякий)
    if hs != hc:
        tried.extend(list(_match_paths(hc, as_)))
    if as_ != ac:
        tried.extend(list(_match_paths(hs, ac)))

    # дедуп
    seen, uniq = set(), []
    for p in tried:
        if p not in seen:
            uniq.append(base.rstrip("/") + p)
            seen.add(p)
    return uniq

def _requests_session(timeout: float = 10.0, retries: int = 3, backoff: float = 0.4) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries, connect=retries, read=retries, status=retries,
        backoff_factor=backoff,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False, respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; RNGN-NHL-Bot/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.sports.ru/",
    })
    session.request_timeout = timeout
    return session

def sports_ru_fetch(home_team: str, away_team: str, session: Optional[requests.Session] = None
                   ) -> Tuple[Optional[str], Optional[str], List[str]]:
    """Возвращает (html, used_url, tried_urls). Если html=None — ничего не нашли."""
    own = session is None
    session = session or _requests_session()
    tried_full: List[str] = []

    for url in build_sports_ru_urls(home_team, away_team):
        tried_full.append(url)
        try:
            resp = session.get(url, timeout=getattr(session, "request_timeout", 10.0))
            if resp.status_code == 200 and resp.text:
                return resp.text, url, tried_full
            else:
                log.debug("sports.ru fetch fail %s: HTTPError('%s %s')",
                          url, resp.status_code, resp.reason or "")
        except requests.RequestException as e:
            log.debug("sports.ru fetch fail %s: %s", url, f"{type(e).__name__}('{e}')")
        time.sleep(0.15)

    return None, None, tried_full
# === /SPORTS.RU HELPERS ======================================================

API = "https://api-web.nhle.com"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NHLDailyBot/1.6; +github)",
    "Accept": "application/json, text/plain, */*",
}

SCHEDULE_FMT   = API + "/v1/schedule/{ymd}"
STANDINGS_NOW  = API + "/v1/standings/now"
GAME_PBP_FMT   = API + "/v1/gamecenter/{gamePk}/play-by-play"

REPORT_DATE_LOCAL = (os.getenv("REPORT_DATE_LOCAL") or "").strip()
REPORT_TZ         = (os.getenv("REPORT_TZ") or os.getenv("REPORT_DATE_TZ") or "Europe/Amsterdam").strip()
DRY_RUN           = (os.getenv("DRY_RUN") or "0").strip() == "1"
DEBUG_VERBOSE     = (os.getenv("DEBUG_VERBOSE") or "1").strip() == "1"

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID   = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

def dbg(msg: str):
    if DEBUG_VERBOSE:
        print(f"[DBG] {msg}", flush=True)

# ---------- HTTP ----------
def _get_with_retries(url: str, *, timeout: int = 30, as_text: bool = False) -> Any:
    last = None
    for i in range(3):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text if as_text else r.json()
        except Exception as e:
            last = e
            dbg(f"retry {i+1}/3 for {url}: {repr(e)}")
            time.sleep(0.75 * (i+1))
    if last: raise last

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)

# ---------- normalization helpers ----------
def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        for k in ("default", "en", "eng", "English", "en_US", "en_GB"):
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v
        for v in x.values():
            if isinstance(v, str) and v.strip():
                return v
        return ""
    if isinstance(x, (list, tuple, set)):
        for v in x:
            s = _norm_str(v)
            if s:
                return s
        return ""
    try:
        s = str(x)
        return s if s != "None" else ""
    except Exception:
        return ""

def _asciiize(s: str) -> str:
    # Убираем диакритику: Montréal -> Montreal
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _slugify_en(s: Any) -> str:
    s = _asciiize(_norm_str(s)).strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s

# ---------- dictionaries ----------
TEAM_RU = {
    "ANA":"Анахайм","ARI":"Аризона","BOS":"Бостон","BUF":"Баффало","CGY":"Калгари",
    "CAR":"Каролина","CHI":"Чикаго","COL":"Колорадо","CBJ":"Коламбус","DAL":"Даллас",
    "DET":"Детройт","EDM":"Эдмонтон","FLA":"Флорида","LAK":"Лос-Анджелес","MIN":"Миннесота",
    "MTL":"Монреаль","NSH":"Нэшвилл","NJD":"Нью-Джерси","NYI":"Айлендерс","NYR":"Рейнджерс",
    "OTT":"Оттава","PHI":"Филадельфия","PIT":"Питтсбург","SJS":"Сан-Хосе","SEA":"Сиэтл",
    "STL":"Сент-Луис","TBL":"Тампа-Бэй","TOR":"Торонто","UTA":"Юта","VAN":"Ванкувер",
    "VGK":"Вегас","WPG":"Виннипег","WSH":"Вашингтон"
}
TEAM_EMOJI = {
    "ANA":"🦆","ARI":"🦂","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️",
    "CBJ":"💣","DAL":"⭐️","DET":"🛡️","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦",
    "NSH":"🐯","NJD":"😈","NYI":"🏝️","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈",
    "SEA":"🦑","STL":"🎵","TBL":"⚡","TOR":"🍁","UTA":"🧊","VAN":"🐳","VGK":"🎰","WPG":"✈️","WSH":"🦅",
}

# Полные слаги для sports.ru (основные варианты)
SPORTSRU_TEAM_SLUGS: Dict[str, List[str]] = {
    "ANA": ["anaheim-ducks","anaheim"],
    "ARI": ["arizona-coyotes","arizona"],
    "BOS": ["boston-bruins","boston"],
    "BUF": ["buffalo-sabres","buffalo"],
    "CGY": ["calgary-flames","calgary"],
    "CAR": ["carolina-hurricanes","carolina"],
    "CHI": ["chicago-blackhawks","chicago"],
    "COL": ["colorado-avalanche","colorado"],
    "CBJ": ["columbus-blue-jackets","columbus"],
    "DAL": ["dallas-stars","dallas"],
    "DET": ["detroit-red-wings","detroit"],
    "EDM": ["edmonton-oilers","edmonton"],
    "FLA": ["florida-panthers","florida"],
    "LAK": ["los-angeles-kings","los-angeles","los-angeles-kings"],
    "MIN": ["minnesota-wild","minnesota"],
    "MTL": ["montreal-canadiens","montreal"],  # без акцента
    "NSH": ["nashville-predators","nashville"],
    "NJD": ["new-jersey-devils","new-jersey"],
    "NYI": ["new-york-islanders","new-york-islanders"],
    "NYR": ["new-york-rangers","new-york-rangers"],
    "OTT": ["ottawa-senators","ottawa"],
    "PHI": ["philadelphia-flyers","philadelphia"],
    "PIT": ["pittsburgh-penguins","pittsburgh"],
    "SJS": ["san-jose-sharks","san-jose"],
    "SEA": ["seattle-kraken","seattle"],
    "STL": ["st-louis-blues","st-louis"],
    "TBL": ["tampa-bay-lightning","tampa-bay"],
    "TOR": ["toronto-maple-leafs","toronto"],
    "UTA": ["utah-hockey-club","utah","utah-hc","utah-hc-nhl","utah-hockey-club"],
    "VAN": ["vancouver-canucks","vancouver"],
    "VGK": ["vegas-golden-knights","vegas"],
    "WPG": ["winnipeg-jets","winnipeg"],
    "WSH": ["washington-capitals","washington"],
}

# ---------- sports.ru helpers ----------
def _team_slug_variants_for_sportsru(team: Dict[str,Any]) -> List[str]:
    v: List[str] = []
    abbr = _norm_str(team.get("abbrev") or team.get("triCode") or team.get("teamAbbrev")).upper()
    if abbr in SPORTSRU_TEAM_SLUGS:
        v.extend(SPORTSRU_TEAM_SLUGS[abbr])
    # также генерим составные из place+nick
    place = _slugify_en(team.get("placeName") or team.get("city") or "")
    nick  = _slugify_en(team.get("teamName") or team.get("name") or "")
    if place and nick: v.append(f"{place}-{nick}")
    if nick: v.append(nick)
    if place: v.append(place)
    # уникализируем
    seen=set(); out=[]
    for x in v:
        if x and x not in seen:
            out.append(x); seen.add(x)
    return out

def gen_sportsru_match_urls(home_team: Dict[str,Any], away_team: Dict[str,Any]) -> List[str]:
    base = "https://www.sports.ru/hockey/match"
    hs = _team_slug_variants_for_sportsru(home_team)
    as_ = _team_slug_variants_for_sportsru(away_team)
    urls=[]
    for h in hs:
        for a in as_:
            urls += [
                f"{base}/{h}-vs-{a}/",
                f"{base}/{a}-vs-{h}/",
                f"{base}/{h}-vs-{a}/stat/",
                f"{base}/{a}-vs-{h}/stat/",
            ]
    # уникализируем порядок
    seen=set(); out=[]
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def try_parse_sportsru_names(url: str) -> Dict[str,str]:
    try:
        html = http_get_text(url, timeout=25)
    except Exception as e:
        dbg(f"sports.ru fetch fail {url}: {e!r}")
        return {}
    soup = BeautifulSoup(html, "html.parser")
    ru_names: Dict[str,str] = {}
    for a in soup.select("a[href*='/hockey/players/'], a[href*='/hockey/player/']"):
        txt = (a.get_text(strip=True) or "")
        if not txt: continue
        ru_last = txt.split()[-1]
        # Английская форма
        en = (a.get("title") or a.get("data-name") or a.get("data-player-name") or "").strip()
        if not en:
            href = a.get("href") or ""
            m = re.search(r"/players/([\w-]+)/", href)
            if m: en = m.group(1).replace("-", " ")
        if en:
            en_last = _asciiize(en).split()[-1].title()
            if en_last and ru_last:
                ru_names[en_last] = ru_last
    if ru_names:
        dbg(f"sports.ru names extracted from {url}: {len(ru_names)}")
    return ru_names

def fetch_ru_name_map_for_match(home_team: Dict[str,Any], away_team: Dict[str,Any]) -> Dict[str,str]:
    tried=[]
    for url in gen_sportsru_match_urls(home_team, away_team):
        tried.append(url)
        mp = try_parse_sportsru_names(url)
        if mp:
            dbg(f"sports.ru goals ok for {url}")
            return mp
    dbg("sports.ru tried URLs (no data): " + " | ".join(tried[:8]))
    return {}

# ---------- Telegram ----------
def send_telegram_text(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if DRY_RUN:
        print("[DRY RUN] " + textwrap.shorten(text.replace("\n"," "), 200, placeholder="…"))
        return
    r = requests.post(url, json=data, timeout=30)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        desc = ""
        try:
            desc = r.json().get("description","")
        except Exception:
            desc = r.text
        raise requests.HTTPError(f"{e} | Telegram said: {desc}") from None

# ---------- misc ----------
def parse_ymd_in_tz(ymd: str, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    d = date.fromisoformat(ymd)
    start = datetime.combine(d, dtime(0,0), tzinfo=tz)
    end   = datetime.combine(d, dtime(23,59,59), tzinfo=tz)
    return start, end

def fetch_schedule_day(ymd: date) -> List[Dict[str,Any]]:
    js = http_get_json(SCHEDULE_FMT.format(ymd=ymd.isoformat()))
    games = js.get("games")
    if games is None:
        games=[]
        for w in (js.get("gameWeek") or []):
            games.extend(w.get("games") or [])
    return games or []

def start_dt_in_tz(g: Dict[str,Any], tz: ZoneInfo) -> Optional[datetime]:
    utc = g.get("startTimeUTC") or g.get("startTime") or g.get("gameDate")
    if not utc: return None
    try:
        return datetime.fromisoformat((_norm_str(utc) or "").replace("Z","+00:00")).astimezone(tz)
    except Exception:
        return None

def is_final(g: Dict[str,Any]) -> bool:
    st = (_norm_str(g.get("gameState") or g.get("gameStatus"))).upper()
    return st in ("FINAL","OFF")

def team_block(g: Dict[str,Any], side: str) -> Dict[str,Any]:
    t = (g.get(f"{side}Team") or {})
    ab = t.get("abbrev") or t.get("triCode") or t.get("teamAbbrev")
    t["abbrev"] = _norm_str(ab).upper()
    return t

def fetch_standings_map() -> Tuple[Dict[str,Tuple[int,int,int]], bool]:
    try:
        js = http_get_json(STANDINGS_NOW)
    except Exception as e:
        dbg(f"standings fetch failed: {e!r}")
        return {}, False
    out={}
    for conf in js.get("standings", []):
        for div in conf.get("divisions", []):
            for team in div.get("teams", []):
                ab = _norm_str(team.get("teamAbbrev") or team.get("abbrev")).upper()
                rec = team.get("record") or {}
                out[ab] = (rec.get("wins",0), rec.get("losses",0), rec.get("ot",0))
    dbg(f"standings map built: {len(out)}")
    return out, bool(out)

def fmt_record(rec: Tuple[int,int,int], have_standings: bool) -> str:
    if not have_standings:
        return "—"
    return f"{rec[0]}-{rec[1]}-{rec[2]}"

def mmss_to_ru(mmss: str) -> str:
    return (mmss or "00:00").replace(":", ".")

# ---------- PBP robust parsing ----------
def _extract_period(ev: Dict[str,Any]) -> int:
    return (
        (ev.get("periodDescriptor") or {}).get("number")
        or (ev.get("period") or {}).get("number")
        or (ev.get("about")  or {}).get("periodNumber")
        or (ev.get("about")  or {}).get("period")
        or 0
    ) or 0

def _extract_time(ev: Dict[str,Any]) -> str:
    return (
        ev.get("timeInPeriod")
        or (ev.get("about") or {}).get("periodTime")
        or (ev.get("clock") or {}).get("timeRemaining")
        or "00:00"
    )

def _extract_team_abbrev(ev: Dict[str,Any]) -> str:
    # пробуем разные места
    return (
        (ev.get("details") or {}).get("eventOwnerTeamAbbrev")
        or (ev.get("details") or {}).get("scoringTeamAbbrev")
        or ev.get("teamAbbrev")
        or (ev.get("team") or {}).get("abbrev")
        or (ev.get("about") or {}).get("team", {}).get("abbrev")
        or ""
    )

def _last_name(s: str) -> str:
    s = _asciiize(_norm_str(s)).strip()
    if not s: return ""
    return s.split()[-1].title()

def _extract_scorer_last(ev: Dict[str,Any]) -> str:
    det = ev.get("details") or {}
    for k in ("scorerName","scoringPlayerName","shootoutShooterName","secondaryEventName"):
        nm = _norm_str(det.get(k))
        if nm: return _last_name(nm)
    sc = ev.get("scorer") or {}
    nm = _norm_str(sc.get("lastName") or sc.get("name") or sc.get("fullName"))
    if nm: return _last_name(nm)
    for p in ev.get("players") or []:
        t = _norm_str(p.get("type") or p.get("playerType")).lower()
        if t in ("scorer","shooter"):
            nm = _norm_str(p.get("lastName") or p.get("name") or p.get("fullName"))
            if nm: return _last_name(nm)
    return ""

def _extract_assists_last_list(ev: Dict[str,Any]) -> List[str]:
    out=[]
    det = ev.get("details") or {}
    for k in ("assist1Name","assist2Name"):
        nm = _norm_str(det.get(k))
        if nm: out.append(_last_name(nm))
    if out: return out
    for a in ev.get("assists") or []:
        nm = _norm_str(a.get("lastName") or a.get("name") or a.get("fullName"))
        if nm: out.append(_last_name(nm))
    if out: return out
    for p in ev.get("players") or []:
        t = _norm_str(p.get("type") or p.get("playerType")).lower()
        if t.startswith("assist"):
            nm = _norm_str(p.get("lastName") or p.get("name") or p.get("fullName"))
            if nm: out.append(_last_name(nm))
    return out

def _is_goal_event(ev: Dict[str,Any]) -> bool:
    keys = [
        _norm_str(ev.get("typeDescKey")).lower(),
        _norm_str(ev.get("type")).lower(),
        _norm_str((ev.get("result") or {}).get("eventTypeId")).lower(),
        _norm_str((ev.get("result") or {}).get("event")).lower(),
    ]
    joined = " ".join([k for k in keys if k])
    if not joined: return False
    if "goal" in joined and "no_goal" not in joined:
        return True
    return False

def _is_shootout_event(ev: Dict[str,Any]) -> bool:
    per = _extract_period(ev)
    if per >= 5: return True
    keys = [
        _norm_str(ev.get("typeDescKey")).lower(),
        _norm_str(ev.get("type")).lower(),
        _norm_str((ev.get("result") or {}).get("eventTypeId")).lower(),
    ]
    txt = " ".join([k for k in keys if k])
    return ("shootout" in txt) or ("so" == txt)

def load_pbp_data(game_pk: int) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    js = http_get_json(GAME_PBP_FMT.format(gamePk=game_pk))

    # Унифицируем контейнер
    plays_obj: Dict[str,Any] = {"scoringPlays": [], "allPlays": [], "shootoutPlays": []}
    if isinstance(js, dict):
        raw = js.get("plays")
        if isinstance(raw, dict):
            plays_obj["scoringPlays"]  = raw.get("scoringPlays") or []
            plays_obj["allPlays"]      = raw.get("allPlays") or []
            plays_obj["shootoutPlays"] = raw.get("shootoutPlays") or []
        elif isinstance(raw, list):
            plays_obj["allPlays"] = raw
        else:
            # иногда бывает js["allPlays"]
            plays_obj["allPlays"] = js.get("allPlays") or []
    elif isinstance(js, list):
        plays_obj["allPlays"] = js

    scoring = plays_obj.get("scoringPlays") or []
    allplays = plays_obj.get("allPlays") or []
    shootout_src = plays_obj.get("shootoutPlays") or []

    # Фолбэк: если scoring пуст — собрать из allPlays по типу 'goal'
    if not scoring and allplays:
        for ev in allplays:
            if _is_goal_event(ev):
                scoring.append(ev)

    goals: List[Dict[str,Any]] = []
    for ev in scoring:
        per = int(_extract_period(ev))
        tm  = _extract_time(ev)
        owner = _extract_team_abbrev(ev).upper()
        scorer = _extract_scorer_last(ev).title()
        assists = [a.title() for a in _extract_assists_last_list(ev)]
        goals.append({
            "period": per, "time": tm, "teamAbbrev": owner,
            "scorer": scorer, "assists": assists
        })

    # Собираем SO, если отдельного списка нет
    if not shootout_src and allplays:
        for ev in allplays:
            if _is_shootout_event(ev):
                shootout_src.append(ev)

    shootout: List[Dict[str,Any]] = []
    rnd = 0
    for ev in shootout_src:
        team = _extract_team_abbrev(ev).upper()
        shooter = _extract_scorer_last(ev).title()
        det = ev.get("details") or {}
        is_goal = bool(det.get("isGoal"))
        # эвристика по текстовому типу
        tdk = _norm_str(ev.get("typeDescKey")).lower()
        if "goal" in tdk: is_goal = True
        if "miss" in tdk or "no_goal" in tdk: is_goal = False
        round_no = det.get("shootoutRound") or det.get("round") or rnd + 1
        rnd = int(round_no)
        shootout.append({
            "round": rnd,
            "teamAbbrev": team,
            "shooter": shooter,
            "result": "goal" if is_goal else "miss",
        })

    dbg(f"parsed PBP: goals={len(goals)} shootout={len(shootout)} for game {game_pk}")
    return goals, shootout

# ---------- name mapping ----------
def ru_last_or_keep(en_last: str, ru_map: Dict[str,str]) -> str:
    if not en_last:
        return ""
    return ru_map.get(en_last, en_last)

# ---------- render ----------
def render_game_block(g: Dict[str,Any], standings_map: Dict[str,Tuple[int,int,int]], have_standings: bool) -> str:
    home = team_block(g, "home")
    away = team_block(g, "away")
    h_ab, a_ab = home["abbrev"], away["abbrev"]
    h_emoji, a_emoji = TEAM_EMOJI.get(h_ab, "•"), TEAM_EMOJI.get(a_ab, "•")
    h_name, a_name   = TEAM_RU.get(h_ab, h_ab), TEAM_RU.get(a_ab, a_ab)

    h_score = int((g.get("homeTeam") or {}).get("score", 0))
    a_score = int((g.get("awayTeam") or {}).get("score", 0))
    h_rec = fmt_record(standings_map.get(h_ab, (0,0,0)), have_standings)
    a_rec = fmt_record(standings_map.get(a_ab, (0,0,0)), have_standings)

    goals, shootout = load_pbp_data(g["id"])
    ru_map = fetch_ru_name_map_for_match(home, away)

    # Шапка: эмодзи — название — счёт — рекорд
    header = [
        f"{h_emoji} «{h_name}» — {h_score} ({h_rec})",
        f"{a_emoji} «{a_name}» — {a_score} ({a_rec})",
        "",
    ]

    per_goals: Dict[int, List[str]] = {1:[],2:[],3:[]}
    ot_goals: List[str] = []
    h_c, a_c = 0, 0
    for ev in goals:
        per = int(ev.get("period",0) or 0)
        tm  = mmss_to_ru(ev.get("time"))
        owner = (ev.get("teamAbbrev") or "").upper()
        if owner == h_ab: h_c += 1
        elif owner == a_ab: a_c += 1
        scorer = ru_last_or_keep((ev.get("scorer") or "").title(), ru_map)
        assists = [ru_last_or_keep(x.title(), ru_map) for x in (ev.get("assists") or [])]
        who = f"{scorer} ({', '.join(assists)})" if assists else (scorer or "—")
        line = f"{h_c}:{a_c} – {tm} {who}"
        if per in (1,2,3):
            per_goals[per].append(line)
        elif per == 4:
            ot_goals.append(line)

    # Буллиты — одна строка, без автора, только итоговый счёт
    so_lines: List[str] = []
    if shootout:
        so_lines.append(f"буллит — {h_score}:{a_score}")

    def add_period(title: str, arr: List[str], out: List[str]):
        out.append(title)
        if arr: out.extend(arr)
        else:   out.append("Голов не было")
        out.append("")

    body: List[str] = []
    add_period("<i>1-й период</i>", per_goals[1], body)
    add_period("<i>2-й период</i>", per_goals[2], body)
    add_period("<i>3-й период</i>", per_goals[3], body)
    if ot_goals:
        add_period("<i>Овертайм</i>", ot_goals, body)
    if so_lines:
        add_period("<i>Буллиты</i>", so_lines, body)

    full = []
    full.append("<tg-spoiler>")
    full.extend(header)
    full.extend(body)
    full.append("</tg-spoiler>")
    txt = "\n".join(full).replace("\n\n\n","\n\n").strip()
    return txt

# ---------- day render & safe split by WHOLE BLOCKS ----------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def month_ru(m: int) -> str: return RU_MONTHS.get(m, "")

def build_day_text(ymd: str, tz: str) -> List[str]:
    tzinfo = ZoneInfo(tz)
    if not ymd:
        base_local = datetime.now(tzinfo).date()
        ymd = base_local.isoformat()
    else:
        base_local = date.fromisoformat(ymd)

    dbg(f"Daily summary for {ymd} in {tz}")
    start = datetime.combine(base_local, dtime(0,0), tzinfo=tzinfo)
    end   = datetime.combine(base_local, dtime(23,59,59), tzinfo=tzinfo)

    raw = fetch_schedule_day(base_local - timedelta(days=1)) \
        + fetch_schedule_day(base_local) \
        + fetch_schedule_day(base_local + timedelta(days=1))

    games = []
    for g in raw:
        dt = start_dt_in_tz(g, tzinfo)
        if not dt: continue
        if start <= dt <= end and is_final(g):
            games.append(g)
    dbg(f"Collected FINAL games: {len(games)}")

    standings_map, have_standings = fetch_standings_map()

    if not games:
        return [f"🗓 Регулярный чемпионат НХЛ • {base_local.day} {month_ru(base_local.month)} • матчей нет"]

    head = f"🗓 Регулярный чемпионат НХЛ • {base_local.day} {month_ru(base_local.month)} • {len(games)} матчей\n\nРезультаты надёжно спрятаны 👇"
    sep = "—" * 66

    # Собираем ЦЕЛЫЕ блоки, чтобы не рвать <tg-spoiler>
    block_texts: List[str] = []
    for g in games:
        block_texts.append(sep + "\n" + render_game_block(g, standings_map, have_standings) + "\n")

    # Безопасная нарезка частей по 3500 символов
    parts: List[str] = []
    cur = head + "\n"
    for blk in block_texts:
        if len(cur) + len(blk) > 3500:
            parts.append(cur.rstrip())
            cur = blk
        else:
            cur += blk
    if cur.strip():
        parts.append(cur.rstrip())

    dbg(f"Telegram parts: {len(parts)}")
    return parts

def main():
    parts = build_day_text(REPORT_DATE_LOCAL, REPORT_TZ)
    total = len(parts)
    for i, part in enumerate(parts, 1):
        if i == 1:
            send_telegram_text(part)
        else:
            send_telegram_text(f"…продолжение (часть {i}/{total})\n\n{part}")

if __name__ == "__main__":
    main()
