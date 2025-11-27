#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, time, re, textwrap
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, timedelta
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
    "User-Agent": "Mozilla/5.0 (compatible; NHLSingleBot/1.4; +github)",
    "Accept": "application/json, text/plain, */*",
}

SCHEDULE_FMT     = API + "/v1/schedule/{ymd}"
GAME_SUMMARY_FMT = API + "/v1/gamecenter/{gamePk}/game-summary"
GAME_PBP_FMT     = API + "/v1/gamecenter/{gamePk}/play-by-play"
GAME_BOX_FMT     = API + "/v1/gamecenter/{gamePk}/boxscore"

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID   = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
DEBUG_VERBOSE = (os.getenv("DEBUG_VERBOSE") or "1").strip() == "1"

GAME_PK_ENV = (os.getenv("GAME_PK") or "").strip()
GAME_QUERY  = (os.getenv("GAME_QUERY") or "").strip()

def dbg(msg: str):
    if DEBUG_VERBOSE:
        print(f"[DBG] {msg}", flush=True)

def _get_with_retries(url: str, *, timeout: int = 30, as_text: bool = False) -> Any:
    last=None
    for i in range(3):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text if as_text else r.json()
        except Exception as e:
            last = e
            dbg(f"retry {i+1}/3 for {url} after {0.75*(i+1):.2f}s: {repr(e)}")
            time.sleep(0.75*(i+1))
    if last: raise last

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)

# ---------- normalization ----------
def _norm_str(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, str): return x
    if isinstance(x, dict):
        for k in ("default","en","eng","English"):
            v = x.get(k)
            if isinstance(v,str) and v.strip(): return v
        for v in x.values():
            if isinstance(v,str) and v.strip(): return v
        return ""
    if isinstance(x, (list,tuple,set)):
        for v in x:
            s = _norm_str(v)
            if s: return s
        return ""
    try:
        s = str(x)
        return s if s != "None" else ""
    except Exception:
        return ""

def _slugify_en(s: Any) -> str:
    s = _norm_str(s).strip().lower()
    s = re.sub(r"[^\w\s-]", "", s); s = re.sub(r"\s+", "-", s); s = re.sub(r"-+", "-", s)
    return s

TEAM_RU = {
    "EDM":"Эдмонтон","DAL":"Даллас","DET":"Детройт","NSH":"Нэшвилл",
    "TBL":"Тампа-Бэй","CGY":"Калгари","FLA":"Флорида","PHI":"Филадельфия",
    "NJD":"Нью-Джерси","STL":"Сент-Луис","NYI":"Айлендерс","BOS":"Бостон",
    "PIT":"Питтсбург","BUF":"Баффало","WSH":"Вашингтон","WPG":"Виннипег",
    "CAR":"Каролина","NYR":"Рейнджерс","CBJ":"Коламбус","TOR":"Торонто",
    "CHI":"Чикаго","MIN":"Миннесота","COL":"Колорадо","SJS":"Сан-Хосе",
    "UTA":"Юта","MTL":"Монреаль","VGK":"Вегас","OTT":"Оттава",
    "ANA":"Анахайм","VAN":"Ванкувер","SEA":"Сиэтл","LAK":"Лос-Анджелес",
}
TEAM_EMOJI = {
    "EDM":"🛢️","DAL":"⭐️","DET":"🛡️","NSH":"🐯","TBL":"⚡","CGY":"🔥",
    "FLA":"🐆","PHI":"🛩","NJD":"😈","STL":"🎵","NYI":"🏝️","BOS":"🐻",
    "PIT":"🐧","BUF":"🦬","WSH":"🦅","WPG":"✈️","CAR":"🌪️","NYR":"🗽",
    "CBJ":"💣","TOR":"🍁","CHI":"🦅","MIN":"🌲","COL":"⛰️","SJS":"🦈",
    "UTA":"🧊","MTL":"🇨🇦","VGK":"🎰","OTT":"🛡","ANA":"🦆","VAN":"🐳",
    "SEA":"🦑","LAK":"👑",
}

SPORTSRU_TEAM_SLUGS = {
    "VGK": ["vegas","vegas-golden-knights"],
    "UTA": ["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
    "UTH": ["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
    "UTAH":["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
}

def _team_slug_variants_for_sportsru(team: Dict[str,Any]) -> List[str]:
    v=[]; abbr=_norm_str(team.get("abbrev") or team.get("triCode") or team.get("teamAbbrev")).upper()
    if abbr in SPORTSRU_TEAM_SLUGS: v += SPORTSRU_TEAM_SLUGS[abbr]
    place=_slugify_en(team.get("placeName") or team.get("city") or "")
    nick=_slugify_en(team.get("teamName") or team.get("name") or "")
    if place and nick: v.append(f"{place}-{nick}")
    if nick: v.append(nick)
    if place and place not in v: v.append(place)
    seen=set(); out=[]
    for x in v:
        if x and x not in seen: out.append(x); seen.add(x)
    return out

def gen_sportsru_match_urls(home_team: Dict[str,Any], away_team: Dict[str,Any]) -> List[str]:
    base="https://www.sports.ru/hockey/match"
    hs=_team_slug_variants_for_sportsru(home_team)
    as_=_team_slug_variants_for_sportsru(away_team)
    urls=[]
    for h in hs:
        for a in as_:
            urls += [f"{base}/{h}-vs-{a}/", f"{base}/{a}-vs-{h}/",
                     f"{base}/{h}-vs-{a}/stat/", f"{base}/{a}-vs-{h}/stat/"]
    seen=set(); out=[]
    for u in urls:
        if u not in seen: out.append(u); seen.add(u)
    return out

def try_parse_sportsru_names(url: str) -> Dict[str,str]:
    try:
        html = http_get_text(url, timeout=25)
    except Exception as e:
        dbg(f"sports.ru fetch fail {url}: {e!r}")
        return {}
    soup = BeautifulSoup(html, "html.parser")
    ru: Dict[str,str] = {}
    for a in soup.select("a[href*='/hockey/players/'], a[href*='/hockey/player/']"):
        txt = (a.get_text(strip=True) or "")
        if not txt: continue
        ru_last = txt.split()[-1]
        en = (a.get("title") or a.get("data-name") or a.get("data-player-name") or "").strip()
        if not en:
            href = a.get("href") or ""
            m = re.search(r"/players/([\w-]+)/", href)
            if m: en = m.group(1).replace("-", " ")
        if en:
            en_last = en.split()[-1].title()
            if en_last and ru_last: ru[en_last] = ru_last
    if ru: dbg(f"sports.ru names extracted from {url}: {len(ru)}")
    return ru

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

def resolve_game_pk_from_query(q: str) -> Optional[int]:
    try:
        ymd, pair = q.split(" ", 1)
        base = date.fromisoformat(ymd)
    except Exception:
        return None
    home, away = "", ""
    if "@" in pair:
        a, h = pair.split("@", 1); away, home = a.strip().upper(), h.strip().upper()
    elif "-" in pair:
        left, right = pair.split("-", 1); home, away = left.strip().upper(), right.strip().upper()
    else:
        return None

    def find_on_day(d: date) -> Optional[int]:
        js = http_get_json(SCHEDULE_FMT.format(ymd=d.isoformat()))
        games = js.get("games")
        if games is None:
            games=[]
            for w in (js.get("gameWeek") or []):
                games.extend(w.get("games") or [])
        for g in games:
            h = _norm_str((g.get("homeTeam") or {}).get("abbrev")).upper()
            a = _norm_str((g.get("awayTeam") or {}).get("abbrev")).upper()
            if h == home and a == away:
                return g.get("id")
        return None

    for d in (base - timedelta(days=1), base, base + timedelta(days=1)):
        gid = find_on_day(d)
        if gid:
            dbg(f"Resolved GAME_PK={gid} for {q}")
            return gid
    return None

def mmss_to_ru(mmss: str) -> str:
    return (mmss or "00:00").replace(":", ".")

def load_pbp_data(game_pk: int):
    js_any = http_get_json(GAME_PBP_FMT.format(gamePk=game_pk))
    plays_obj = {"scoringPlays": [], "allPlays": [], "shootoutPlays": []}
    if isinstance(js_any, dict):
        raw = js_any.get("plays")
        if isinstance(raw, dict):
            plays_obj = {"scoringPlays": raw.get("scoringPlays") or [],
                         "allPlays":     raw.get("allPlays")     or [],
                         "shootoutPlays":raw.get("shootoutPlays") or []}
        elif isinstance(raw, list):
            plays_obj["allPlays"] = raw
        else:
            plays_obj["allPlays"] = js_any.get("allPlays") or []
    elif isinstance(js_any, list):
        plays_obj["allPlays"] = js_any

    scoring = plays_obj.get("scoringPlays") or []
    allplays = plays_obj.get("allPlays") or []
    raw_so = plays_obj.get("shootoutPlays") or []

    goals=[]
    for ev in scoring:
        per = (
            (ev.get("periodDescriptor") or {}).get("number")
            or (ev.get("period") or {}).get("number")
            or (ev.get("about")  or {}).get("periodNumber")
            or 0
        ) or 0
        tm  = ev.get("timeInPeriod") or (ev.get("about") or {}).get("periodTime") or "00:00"
        owner = ( (ev.get("details") or {}).get("eventOwnerTeamAbbrev")
                  or ev.get("teamAbbrev")
                  or (ev.get("team") or {}).get("abbrev") or "" )
        def _scorer(ev):
            sc = ev.get("scorer")
            if isinstance(sc, dict):
                nm = _norm_str(sc.get("lastName") or sc.get("name") or sc.get("fullName"))
                if nm: return nm.split()[-1]
            for p in ev.get("players") or []:
                if (_norm_str(p.get("type") or p.get("playerType")).lower() in ("scorer","shooter")):
                    nm = _norm_str(p.get("lastName") or p.get("name") or p.get("fullName"))
                    if nm: return nm.split()[-1]
            det = ev.get("details") or {}
            for k in ("shootoutShooterName","scoringPlayerName","scorerName"):
                nm = _norm_str(det.get(k))
                if nm: return nm.split()[-1]
            return ""
        def _assists(ev):
            out=[]
            for a in ev.get("assists") or []:
                nm = _norm_str(a.get("lastName") or a.get("name") or a.get("fullName"))
                if nm: out.append(nm.split()[-1])
            if out: return out
            for p in ev.get("players") or []:
                if _norm_str(p.get("type") or p.get("playerType")).lower().startswith("assist"):
                    nm = _norm_str(p.get("lastName") or p.get("name") or p.get("fullName"))
                    if nm: out.append(nm.split()[-1])
            return out
        goals.append({"period":per,"time":tm,"teamAbbrev":owner,"scorer":_scorer(ev).title(),"assists":[x.title() for x in _assists(ev)]})

    if not raw_so:
        for ev in allplays:
            per = (
                (ev.get("periodDescriptor") or {}).get("number")
                or (ev.get("period") or {}).get("number")
                or (ev.get("about")  or {}).get("periodNumber")
                or 0
            ) or 0
            ptype = (ev.get("periodDescriptor") or {}).get("periodType") \
                    or (ev.get("about") or {}).get("ordinalNum") or ""
            if per >= 5 or str(ptype).upper() == "SO":
                raw_so.append(ev)

    shootout=[]
    rnd=0
    for ev in raw_so:
        team = (_norm_str((ev.get("details") or {}).get("eventOwnerTeamAbbrev"))
                or _norm_str(ev.get("teamAbbrev"))
                or _norm_str((ev.get("team") or {}).get("abbrev"))).upper()
        sc=""
        scd=ev.get("scorer")
        if isinstance(scd, dict):
            nm=_norm_str(scd.get("lastName") or scd.get("name") or scd.get("fullName"))
            if nm: sc = nm.split()[-1]
        if not sc:
            for p in ev.get("players") or []:
                if _norm_str(p.get("type") or p.get("playerType")).lower() in ("scorer","shooter"):
                    nm=_norm_str(p.get("lastName") or p.get("name") or p.get("fullName"))
                    if nm: sc = nm.split()[-1]; break
        if not sc:
            det = ev.get("details") or {}
            for k in ("shootoutShooterName","scoringPlayerName","scorerName"):
                nm = _norm_str(det.get(k))
                if nm: sc = nm.split()[-1]; break
        tdk = _norm_str(ev.get("typeDescKey")).lower()
        det = ev.get("details") or {}
        is_goal = bool(det.get("isGoal"))
        if "goal" in tdk: is_goal = True
        if "miss" in tdk or "no_goal" in tdk: is_goal = False
        round_no = det.get("shootoutRound") or det.get("round") or rnd + 1
        rnd = int(round_no)
        shootout.append({"round":rnd,"teamAbbrev":team,"shooter":(sc.title() if sc else ""), "result":"goal" if is_goal else "miss"})

    return goals, shootout, (js_any.get("gameInfo") if isinstance(js_any, dict) else {})

def send_telegram_text(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = requests.post(url, json=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram error: {js}")

def render_single(game_pk: int) -> str:
    sumjs = fetch_json_safe(GAME_SUMMARY_FMT.format(gamePk=game_pk))
    boxjs = None if sumjs else fetch_json_safe(GAME_BOX_FMT.format(gamePk=game_pk))
    goals, shootout, game_info = load_pbp_data(game_pk)

    if sumjs:
        g = sumjs.get("game", {})
        home, away = (g.get("homeTeam") or {}), (g.get("awayTeam") or {})
        hscore, ascore = int(home.get("score",0)), int(away.get("score",0))
    elif boxjs:
        home, away = (boxjs.get("homeTeam") or {}).get("team", {}) or {}, (boxjs.get("awayTeam") or {}).get("team", {}) or {}
        hscore = int((boxjs.get("homeTeam") or {}).get("score",0))
        ascore = int((boxjs.get("awayTeam") or {}).get("score",0))
    else:
        home = (game_info.get("homeTeam") or {})
        away = (game_info.get("awayTeam") or {})
        h_ab = _norm_str(home.get("abbrev")).upper()
        a_ab = _norm_str(away.get("abbrev")).upper()
        hscore = sum(1 for ev in goals if _norm_str(ev.get("teamAbbrev")).upper()==h_ab)
        ascore = sum(1 for ev in goals if _norm_str(ev.get("teamAbbrev")).upper()==a_ab)

    h_ab = _norm_str(home.get("abbrev") or home.get("triCode")).upper()
    a_ab = _norm_str(away.get("abbrev") or away.get("triCode")).upper()
    h_name, a_name = TEAM_RU.get(h_ab, h_ab), TEAM_RU.get(a_ab, a_ab)
    h_emoji, a_emoji = TEAM_EMOJI.get(h_ab, "•"), TEAM_EMOJI.get(a_ab, "•")

    def record_of(t: Dict[str,Any]) -> str:
        r = (t.get("record") or {})
        return f"{r.get('wins',0)}-{r.get('losses',0)}-{r.get('ot',0)}" if r else "—"

    h_rec = record_of(home)
    a_rec = record_of(away)

    ru_map = fetch_ru_name_map_for_match(home, away)

    header = [
        f"{h_emoji} «{h_name}» — {hscore} ({h_rec})",
        f"{a_emoji} «{a_name}» — {ascore} ({a_rec})",
        "",
    ]

    per_goals = {1:[],2:[],3:[]}
    ot_goals: List[str] = []
    so_lines: List[str] = []

    h_c = a_c = 0
    for ev in goals:
        per = ( (ev.get("period") if isinstance(ev.get("period"), int) else 0)
                or (ev.get("about") or {}).get("periodNumber") or 0 )
        tm  = mmss_to_ru(ev.get("time") if isinstance(ev.get("time"), str) else ev.get("time","00:00"))
        owner = _norm_str(ev.get("teamAbbrev")).upper()
        if owner == h_ab: h_c += 1
        elif owner == a_ab: a_c += 1
        scorer = ru_last_or_keep(_norm_str(ev.get("scorer")).title(), ru_map)
        assists = [ru_last_or_keep(_norm_str(x).title(), ru_map) for x in (ev.get("assists") or [])]
        who = f"{scorer} ({', '.join(assists)})" if assists else (scorer or "—")
        line = f"{h_c}:{a_c} – {tm} {who}"
        if per in (1,2,3): per_goals[per].append(line)
        elif per == 4:     ot_goals.append(line)

    if shootout:
        so_h = so_a = 0
        for ev in shootout:
            team = ev["teamAbbrev"]
            shooter = ru_last_or_keep(_norm_str(ev["shooter"]).title(), ru_map)
            res = ev["result"]
            if res == "goal":
                if team == h_ab: so_h += 1
                elif team == a_ab: so_a += 1
            word = "гол" if res == "goal" else "мимо"
            so_lines.append(f"Раунд {ev['round']} — {shooter or '—'} — {word} (SO {so_h}:{so_a})")

    def per_block(title: str, arr: List[str]) -> List[str]:
        out=[title]
        out += (arr or ["Голов не было"])
        out.append("")
        return out

    body=[]
    body += per_block("<i>1-й период</i>", per_goals[1])
    body += per_block("<i>2-й период</i>", per_goals[2])
    body += per_block("<i>3-й период</i>", per_goals[3])
    if ot_goals: body += per_block("<i>Овертайм</i>", ot_goals)
    if so_lines: body += per_block("<i>Буллиты</i>", so_lines)

    txt = []
    txt.append("<tg-spoiler>")
    txt.extend(header)
    txt.extend(body)
    txt.append("</tg-spoiler>")
    return "\n".join(txt).replace("\n\n\n","\n\n").strip()

def send_telegram_text(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = requests.post(url, json=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram error: {js}")

def fetch_json_safe(url: str) -> Optional[Dict[str,Any]]:
    try:
        js = http_get_json(url)
        if isinstance(js, dict): return js
    except Exception as e:
        dbg(f"fetch fail {url}: {e!r}")
    return None

def send(gid: int):
    text = render_single(gid)
    dbg("Single match preview:\n" + text[:500] + ("…" if len(text) > 500 else ""))
    send_telegram_text(text)
    print("OK (posted 1)")

def main():
    gid = None
    if GAME_PK_ENV:
        try: gid = int(GAME_PK_ENV)
        except Exception: gid = None
    if not gid and GAME_QUERY:
        gid = resolve_game_pk_from_query(GAME_QUERY)
    if not gid:
        print("[ERR] provide GAME_PK or GAME_QUERY", file=sys.stderr)
        return
    send(gid)

if __name__ == "__main__":
    main()
