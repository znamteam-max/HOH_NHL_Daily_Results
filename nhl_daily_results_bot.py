#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Daily Results Bot — daily summary with RU scorers, spoilers, SO winner line.

ENV (from workflows):
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
- REPORT_DATE_LOCAL   (YYYY-MM-DD)   — optional; if empty: auto (yesterday in REPORT_DATE_TZ)
- REPORT_DATE_TZ      (IANA TZ)      — default Europe/Amsterdam
- DRY_RUN             (0/1)

Notes:
- Hides everything except team names + emojis under <tg-spoiler>.
- Period headers are italic. "Голов не было" is plain.
- Overtime shown as "Овертайм"; "№N" only if multiple OTs.
- Shootout: only the winning attempt is printed as:
      Победный буллит
      {FINAL_HOME}:{FINAL_AWAY} – {Scorer}
- Team records are "(W-L-OT)" only (no points).
- Robust HTTP with retries; auto-fallback to api-web if statsapi fails.
"""

from __future__ import annotations
import os, re, json, time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date, time as dtime

import requests

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None

TG_API      = "https://api.telegram.org"
NHLE_BASE   = "https://api-web.nhle.com/v1"
APIWEB_BASE = "https://api-web.nhle.com/v1"
STATS_BASE  = "https://statsapi.web.nhl.com/api/v1"

# ---------- ENV ----------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

DRY_RUN = _env_bool("DRY_RUN", False)

# ---------- RU ----------
MONTHS_RU = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}

def ru_plural(n: int, one: str, few: str, many: str) -> str:
    n10 = n % 10
    n100 = n % 100
    if n10 == 1 and n100 != 11:
        return one
    if 2 <= n10 <= 4 and not (12 <= n100 <= 14):
        return few
    return many

TEAM_RU = {
    "ANA":"Анахайм","ARI":"Аризона","BOS":"Бостон","BUF":"Баффало","CGY":"Калгари","CAR":"Каролина",
    "CHI":"Чикаго","COL":"Колорадо","CBJ":"Коламбус","DAL":"Даллас","DET":"Детройт","EDM":"Эдмонтон",
    "FLA":"Флорида","LAK":"Лос-Анджелес","MIN":"Миннесота","MTL":"Монреаль","NSH":"Нэшвилл",
    "NJD":"Нью-Джерси","NYI":"Айлендерс","NYR":"Рейнджерс","OTT":"Оттава","PHI":"Филадельфия",
    "PIT":"Питтсбург","SJS":"Сан-Хосе","SEA":"Сиэтл","STL":"Сент-Луис","TBL":"Тампа-Бэй",
    "TOR":"Торонто","VAN":"Ванкувер","VGK":"Вегас","WSH":"Вашингтон","WPG":"Виннипег",
    "UTA":"UTA",
}

TEAM_EMOJI = {
    "ANA":"🦆","ARI":"🦂","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️","CBJ":"💣",
    "DAL":"⭐️","DET":"🛡️","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦","NSH":"🐯",
    "NJD":"😈","NYI":"🏝️","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈","SEA":"🦑","STL":"🎵",
    "TBL":"⚡","TOR":"🍁","VAN":"🐳","VGK":"🎰","WSH":"🦅","WPG":"✈️","UTA":"🧊",
}

SPORTSRU_SLUG = {
    "ANA":"anaheim-ducks","ARI":"arizona-coyotes","BOS":"boston-bruins","BUF":"buffalo-sabres",
    "CGY":"calgary-flames","CAR":"carolina-hurricanes","CHI":"chicago-blackhawks",
    "COL":"colorado-avalanche","CBJ":"columbus-blue-jackets","DAL":"dallas-stars",
    "DET":"detroit-red-wings","EDM":"edmonton-oilers","FLA":"florida-panthers",
    "LAK":"los-angeles-kings","MIN":"minnesota-wild","MTL":"montreal-canadiens",
    "NSH":"nashville-predators","NJD":"new-jersey-devils","NYI":"new-york-islanders",
    "NYR":"new-york-rangers","OTT":"ottawa-senators","PHI":"philadelphia-flyers",
    "PIT":"pittsburgh-penguins","SJS":"san-jose-sharks","SEA":"seattle-kraken",
    "STL":"st-louis-blues","TBL":"tampa-bay-lightning","TOR":"toronto-maple-leafs",
    "VAN":"vancouver-canucks","VGK":"vegas-golden-knights","WSH":"washington-capitals",
    "WPG":"winnipeg-jets","UTA":"utah-hockey-club",
}

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.8",
}

# ---------- HTTP with retry ----------
def http_get_json(url: str, timeout: int = 25, attempts: int = 4, backoff: float = 1.7) -> Any:
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, timeout=timeout, headers=UA_HEADERS if "sports.ru" in url else None)
            r.raise_for_status()
            try:
                return r.json()
            except Exception:
                return json.loads(r.text or "{}")
        except Exception as e:
            last = e
            if i < attempts-1:
                time.sleep(backoff ** i)
            else:
                raise last

def http_get_text(url: str, timeout: int = 25, attempts: int = 4, backoff: float = 1.7) -> str:
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            last = e
            if i < attempts-1:
                time.sleep(backoff ** i)
            else:
                raise last

# ---------- Structures ----------
@dataclass
class TeamRecord:
    wins: int
    losses: int
    ot: int
    def as_str(self) -> str:
        return f"{self.wins}-{self.losses}-{self.ot}"

@dataclass
class GameMeta:
    gamePk: int
    home_tri: str
    away_tri: str
    home_score: int
    away_score: int
    game_state: str

@dataclass
class ScoringEvent:
    period: int
    period_type: str
    time: str          # "MM.SS"
    team_for: str      # triCode
    home_goals: int
    away_goals: int
    scorer: str        # RU/EN
    assists: List[str] = field(default_factory=list)

# ---------- Helpers ----------
TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")

def _mmss(s: str) -> str:
    s = (s or "00:00").strip().replace(" ", "")
    if ":" in s:
        m, ss = s.split(":")
        return f"{int(m):02d}.{ss}"
    if "." in s:
        m, ss = s.split(".")
        return f"{int(m):02d}.{ss}"
    return "00.00"

def sanitize_name(x: Optional[str]) -> str:
    if not x: return ""
    t = re.sub(r"^[\(\s]+|[\)\s]+$", "", x.strip())  # снимем лишние внешние скобки/пробелы
    t = re.sub(r"\s+", " ", t)
    return t

def sanitize_names(lst: List[str]) -> List[str]:
    out = []
    for x in lst or []:
        t = sanitize_name(x)
        if t: out.append(t)
    return out

# ---------- Standings ----------
def fetch_standings_map() -> Dict[str, TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"
    data = http_get_json(url)
    teams: Dict[str, TeamRecord] = {}
    nodes = []
    if isinstance(data, dict):
        if isinstance(data.get("standings"), list): nodes = data["standings"]
        elif isinstance(data.get("records"), list): nodes = data["records"]
        elif isinstance(data.get("standings"), dict):
            nodes = data["standings"].get("overallRecords", []) or []
    elif isinstance(data, list):
        nodes = data
    for r in nodes:
        abbr = ""
        ta = r.get("teamAbbrev")
        if isinstance(ta, str): abbr = ta.upper()
        elif isinstance(ta, dict): abbr = (ta.get("default") or ta.get("tricode") or "").upper()
        if not abbr:
            abbr = (r.get("teamAbbrevTricode") or r.get("teamTriCode") or "").upper()

        rec = r.get("record") or r.get("overallRecord") or {}
        wins = int(rec.get("wins") or rec.get("gamesPlayedWins") or 0)
        losses = int(rec.get("losses") or rec.get("gamesPlayedLosses") or 0)
        ot = int(rec.get("ot") or rec.get("overtimeLosses") or 0)
        if abbr:
            teams[abbr] = TeamRecord(wins, losses, ot)
    return teams

# ---------- Official scoring (statsapi -> fallback api-web) ----------
def fetch_game_meta(gamePk: int) -> GameMeta:
    # Try statsapi
    url_stats = f"{STATS_BASE}/game/{gamePk}/feed/live"
    try:
        data = http_get_json(url_stats)
        g = data.get("gameData", {})
        teams = g.get("teams", {})
        htri = (teams.get("home", {}) or {}).get("abbreviation") or (teams.get("home", {}) or {}).get("triCode") or ""
        atri = (teams.get("away", {}) or {}).get("abbreviation") or (teams.get("away", {}) or {}).get("triCode") or ""
        linescore = data.get("liveData", {}).get("linescore", {}) or {}
        hs = int((linescore.get("teams", {}) or {}).get("home", {}).get("goals") or 0)
        as_ = int((linescore.get("teams", {}) or {}).get("away", {}).get("goals") or 0)
        state = (g.get("status", {}) or {}).get("detailedState") or (data.get("gameData", {}).get("status", {}) or {}).get("abstractGameState") or ""
        return GameMeta(gamePk, htri.upper(), atri.upper(), hs, as_, state)
    except Exception:
        # Fallback api-web
        url_web = f"{APIWEB_BASE}/gamecenter/{gamePk}/game-summary"
        js = http_get_json(url_web)
        home = js.get("homeTeam", {}) or {}
        away = js.get("awayTeam", {}) or {}
        htri = (home.get("abbrev") or "").upper()
        atri = (away.get("abbrev") or "").upper()
        hs = int(home.get("score") or 0)
        as_ = int(away.get("score") or 0)
        state = (js.get("gameState") or js.get("gameStatus") or "").upper()
        return GameMeta(gamePk, htri, atri, hs, as_, state)

def fetch_scoring_official(gamePk: int, home_tri: str, away_tri: str) -> Tuple[List[ScoringEvent], Optional[str]]:
    """
    Returns events and shootout winner (if detected) separately (winner name or None).
    """
    # Try statsapi
    url_stats = f"{STATS_BASE}/game/{gamePk}/feed/live"
    try:
        data = http_get_json(url_stats)
        plays = (data.get("liveData", {}).get("plays", {}) or {})
        allPlays = plays.get("allPlays", []) or []
        idxs = plays.get("scoringPlays", []) or []
        events: List[ScoringEvent] = []
        h = a = 0
        so_goals: List[str] = []  # collect shootout goal scorers (EN)
        for i in idxs:
            if not (0 <= i < len(allPlays)): continue
            p = allPlays[i]
            res = p.get("result", {}) or {}
            if (res.get("eventTypeId") or "").upper() != "GOAL":
                continue
            about = p.get("about", {}) or {}
            period = int(about.get("period") or 0)
            ptype  = (about.get("periodType") or "REGULAR").upper()
            t = _mmss(about.get("periodTime") or "00:00")
            team = ((p.get("team", {}) or {}).get("triCode") or "").upper()

            h_goals = about.get("goals", {}).get("home")
            a_goals = about.get("goals", {}).get("away")
            if isinstance(h_goals, int) and isinstance(a_goals, int):
                h, a = h_goals, a_goals
            else:
                if team == home_tri: h += 1
                elif team == away_tri: a += 1

            scorer = ""
            assists: List[str] = []
            for pp in p.get("players", []) or []:
                role = (pp.get("playerType") or "").upper()
                name = (pp.get("player", {}) or {}).get("fullName") or ""
                if role == "SCORER": scorer = name
                elif role == "ASSIST": assists.append(name)

            events.append(ScoringEvent(period, ptype, t, team, h, a, scorer, assists))

            if ptype == "SHOOTOUT":
                if scorer:
                    so_goals.append(scorer)

        so_winner = so_goals[-1] if so_goals else None
        return events, so_winner

    except Exception:
        # Fallback api-web
        url_pb = f"{APIWEB_BASE}/gamecenter/{gamePk}/play-by-play"
        js = http_get_json(url_pb)
        plist = js.get("plays") or js.get("gameCenter", {}).get("plays") or []
        events: List[ScoringEvent] = []
        h=a=0
        so_goals: List[str] = []
        for p in plist:
            tkey = (p.get("typeDescKey") or p.get("typeCode") or "").lower()
            if tkey != "goal":
                continue
            pd = p.get("periodDescriptor", {}) or {}
            pnum = int(pd.get("number") or p.get("period", 0) or 0)
            ptype = (pd.get("periodType") or "REGULAR").upper()
            t = _mmss(p.get("timeInPeriod") or p.get("clock", ""))

            team = (p.get("teamAbbrev") or p.get("team", {}).get("abbrev") or "").upper()
            if team == home_tri: h += 1
            elif team == away_tri: a += 1

            det = p.get("details", {}) or {}
            scorer = det.get("scoringPlayerName") or det.get("scorerName") or det.get("goalPlayerName") or ""
            assists = [det.get("assist1PlayerName"), det.get("assist2PlayerName")]
            assists = [x for x in assists if x]

            events.append(ScoringEvent(pnum, ptype, t, team, h, a, scorer, assists))
            if ptype == "SHOOTOUT" and scorer:
                so_goals.append(scorer)
        so_winner = so_goals[-1] if so_goals else None
        return events, so_winner

# ---------- sports.ru parsing ----------
def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or "")
    if not m: return None
    return f"{int(m.group(1)):02d}.{m.group(2)}"

def parse_sportsru_goals_html(html: str, side: str) -> List[Tuple[Optional[str], Optional[str], List[str]]]:
    """
    Returns list of tuples: (time_ru, scorer_ru, assists_ru[])
    side: 'home' or 'away'
    """
    results: List[Tuple[Optional[str], Optional[str], List[str]]] = []

    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or \
             soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors = [a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru = anchors[0] if anchors else None
                assists_ru = anchors[1:] if len(anchors) > 1 else []
                raw_text = li.get_text(" ", strip=True)
                time_ru = _extract_time(raw_text)
                results.append((time_ru, scorer_ru, assists_ru))
        return results

    # regex fallback
    ul_pat = re.compile(
        r'<ul[^>]*class="[^"]*match-summary__goals-list[^"]*--%s[^"]*"[^>]*>(.*?)</ul>' % side,
        re.S | re.I
    )
    li_pat = re.compile(r"<li\b[^>]*>(.*?)</li>", re.S|re.I)
    a_pat  = re.compile(r"<a\b[^>]*>(.*?)</a>", re.S|re.I)

    m = ul_pat.search(html)
    if not m: return results
    ul_html = m.group(1)
    for li_html in li_pat.findall(ul_html):
        text = re.sub(r"<[^>]+>", " ", li_html)
        time_ru = _extract_time(text)
        names = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", w)).strip()
                 for w in a_pat.findall(li_html)]
        scorer_ru = names[0] if names else None
        assists_ru = names[1:] if len(names) > 1 else []
        results.append((time_ru, scorer_ru, assists_ru))
    return results

def fetch_sportsru_goals(home_tri: str, away_tri: str) -> Tuple[List[Tuple[Optional[str], Optional[str], List[str]]], List[Tuple[Optional[str], Optional[str], List[str]]]]:
    home_slug = SPORTSRU_SLUG.get(home_tri)
    away_slug = SPORTSRU_SLUG.get(away_tri)
    if not home_slug or not away_slug:
        return [], []
    tried = [
        (home_slug, away_slug),
        (away_slug, home_slug),
    ]
    for hs, asg in tried:
        url = f"https://www.sports.ru/hockey/match/{hs}-vs-{asg}/"
        try:
            html = http_get_text(url, timeout=20)
        except Exception:
            continue
        home_side = "home" if hs == home_slug else "away"
        away_side = "away" if home_side == "home" else "home"
        h = parse_sportsru_goals_html(html, home_side)
        a = parse_sportsru_goals_html(html, away_side)
        if h or a:
            return h, a
    return [], []

def merge_with_sportsru(evs: List[ScoringEvent],
                        sru_home: List[Tuple[Optional[str], Optional[str], List[str]]],
                        sru_away: List[Tuple[Optional[str], Optional[str], List[str]]],
                        home_tri: str, away_tri: str) -> List[ScoringEvent]:
    hi = ai = 0
    out: List[ScoringEvent] = []
    for ev in evs:
        if ev.team_for == home_tri and hi < len(sru_home):
            _, scorer_ru, assists_ru = sru_home[hi]; hi += 1
            ev.scorer = sanitize_name(scorer_ru) or ev.scorer
            ev.assists = sanitize_names(assists_ru) or ev.assists
        elif ev.team_for == away_tri and ai < len(sru_away):
            _, scorer_ru, assists_ru = sru_away[ai]; ai += 1
            ev.scorer = sanitize_name(scorer_ru) or ev.scorer
            ev.assists = sanitize_names(assists_ru) or ev.assists
        out.append(ev)
    return out

# ---------- Day games ----------
def resolve_local_day_games(local_ymd: str, tz_name: str) -> List[Dict[str, Any]]:
    """
    Returns schedule games (raw nodes) whose start time falls within [00:00, 23:59:59] of local day in given TZ.
    """
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(tz_name)
    base = date.fromisoformat(local_ymd)
    start = datetime.combine(base, dtime(0,0), tzinfo=tz)
    end   = datetime.combine(base, dtime(23,59,59), tzinfo=tz)

    def fetch_sched(d: date) -> List[Dict[str, Any]]:
        url = f"{NHLE_BASE}/schedule/{d.isoformat()}"
        js = http_get_json(url)
        games = js.get("games")
        if games is None:
            games = []
            for w in js.get("gameWeek") or []:
                games.extend(w.get("games") or [])
        return games or []

    raw: List[Dict[str, Any]] = []
    for d in (base - timedelta(days=1), base, base + timedelta(days=1)):
        raw.extend(fetch_sched(d))

    def get_start_dt(g) -> Optional[datetime]:
        utc = g.get("startTimeUTC") or g.get("gameDate") or g.get("startTime")
        if not utc: return None
        try:
            return datetime.fromisoformat(utc.replace("Z","+00:00")).astimezone(tz)
        except Exception:
            return None

    filtered = []
    for g in raw:
        dt = get_start_dt(g)
        if dt and start <= dt <= end:
            filtered.append(g)
    return filtered

def filter_final_games(raw_games: List[Dict[str, Any]]) -> List[int]:
    ids: List[int] = []
    for g in raw_games:
        st = (g.get("gameState") or g.get("gameStatus") or "").upper()
        if st in ("FINAL","OFF"):
            gid = int(g.get("id") or g.get("gameId") or g.get("gamePk") or 0)
            if gid: ids.append(gid)
    return sorted(set(ids))

# ---------- Formatting ----------
def period_title(ptype: str, num: int, ot_index: int, total_ot: int) -> str:
    t = (ptype or "REGULAR").upper()
    if t == "REGULAR":
        return f"<i>{num}-й период</i>"
    if t == "OVERTIME":
        if total_ot <= 1:
            return "<i>Овертайм</i>"
        else:
            return f"<i>Овертайм №{ot_index}</i>"
    if t == "SHOOTOUT":
        return "<i>Буллиты</i>"
    return f"<i>Период {num}</i>"

def line_goal(ev: ScoringEvent) -> str:
    score = f"{ev.home_goals}:{ev.away_goals}"
    who = sanitize_name(ev.scorer) or "—"
    assists = sanitize_names(ev.assists)
    assist_txt = f" ({', '.join(assists)})" if assists else ""
    return f"{score} – {ev.time} {who}{assist_txt}"

def build_match_block(meta: GameMeta, standings: Dict[str, TeamRecord], events: List[ScoringEvent], so_winner: Optional[str]) -> str:
    hn = TEAM_RU.get(meta.home_tri, meta.home_tri)
    an = TEAM_RU.get(meta.away_tri, meta.away_tri)
    hrec = standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "0-0-0"
    arec = standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "0-0-0"

    head_inside = f"<b>«{hn}»: {meta.home_score}</b> ({hrec})\n<b>«{an}»: {meta.away_score}</b> ({arec})"

    # group by (period, type)
    groups: Dict[Tuple[int,str], List[ScoringEvent]] = {}
    for ev in events:
        groups.setdefault((ev.period, ev.period_type.upper()), []).append(ev)

    # determine number of OTs
    ot_keys = [k for k in groups.keys() if k[1] == "OVERTIME"]
    total_ot = len(ot_keys)

    # order keys: REGULAR periods by num, then OTs by num, then SHOOTOUT
    def key_order(k: Tuple[int,str]) -> Tuple[int,int]:
        pnum, ptype = k
        order = {"REGULAR":0, "OVERTIME":1, "SHOOTOUT":2}.get(ptype, 9)
        return (order, pnum)
    ordered = sorted(groups.keys(), key=key_order)

    body: List[str] = []
    ot_counter = 0
    for pnum, ptype in ordered:
        if ptype == "OVERTIME":
            ot_counter += 1
            title = period_title(ptype, pnum, ot_counter, total_ot)
        else:
            title = period_title(ptype, pnum, 0, total_ot)

        body.append("")  # blank line before each period block
        body.append(title)

        if ptype == "SHOOTOUT":
            # Only winning attempt (if any)
            if so_winner:
                body.append("Победный буллит")
                body.append(f"{meta.home_score}:{meta.away_score} – {sanitize_name(so_winner)}")
            else:
                body.append("Победный буллит")
                body.append(f"{meta.home_score}:{meta.away_score} – —")
            continue

        evs = groups[(pnum, ptype)]
        if not evs:
            body.append("Голов не было")
        else:
            for ev in evs:
                body.append(line_goal(ev))

    return head_inside + "\n" + "\n".join(body).strip()

def build_visible_header(meta: GameMeta) -> str:
    he = TEAM_EMOJI.get(meta.home_tri, "")
    ae = TEAM_EMOJI.get(meta.away_tri, "")
    hn = TEAM_RU.get(meta.home_tri, meta.home_tri)
    an = TEAM_RU.get(meta.away_tri, meta.away_tri)
    # Only names+emojis visible (no score)
    return f"{he} «{hn}»\n{ae} «{an}»"

# ---------- Telegram ----------
def send_telegram_text(text: str) -> None:
    token   = _env_str("TELEGRAM_BOT_TOKEN","").strip()
    chat_id = _env_str("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat_id:
        print("[ERR] Telegram token/chat_id not set"); return
    url = f"{TG_API}/bot{token}/sendMessage"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload = {
        "chat_id": int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "disable_notification": False,
        "parse_mode": "HTML",
    }
    if DRY_RUN:
        print("[DRY RUN]\n" + text)
        return
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    try:
        data = resp.json()
    except Exception:
        data = {"ok":None,"raw":resp.text}
    print(f"[DBG] TG HTTP={resp.status_code} JSON keys={list(data.keys())}")

# ---------- MAIN ----------
def main():
    # Ignore GAME_PK if somehow set in env (this is daily, not single)
    if os.getenv("GAME_PK"):
        print("[DBG] Ignoring GAME_PK for daily summary")
        os.environ.pop("GAME_PK", None)

    # Resolve day
    ymd = _env_str("REPORT_DATE_LOCAL","").strip()
    tz  = _env_str("REPORT_DATE_TZ","Europe/Amsterdam").strip()
    if not ymd:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo(tz))
        ymd = (now.date() - timedelta(days=1)).isoformat()
    print(f"[DBG] Daily summary for local day {ymd} in {tz}")

    raw = resolve_local_day_games(ymd, tz)
    final_ids = filter_final_games(raw)
    n = len(final_ids)
    if n == 0:
        print("OK (no FINAL games for that day)")
        return

    standings = fetch_standings_map()

    # Header
    dt = date.fromisoformat(ymd)
    header = f"🗓 Регулярный чемпионат НХЛ • {dt.day} {MONTHS_RU[dt.month]} • {n} {ru_plural(n,'матч','матча','матчей')}\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————"

    blocks: List[str] = [header]
    for gid in final_ids:
        meta = fetch_game_meta(gid)
        events, so_winner = fetch_scoring_official(gid, meta.home_tri, meta.away_tri)
        # Merge with sports.ru RU names
        sh, sa = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        merged = merge_with_sportsru(events, sh, sa, meta.home_tri, meta.away_tri)

        visible = build_visible_header(meta)
        inside  = build_match_block(meta, standings, merged, so_winner)
        block = f"{visible}\n\n<tg-spoiler>{inside}</tg-spoiler>\n\n——————————————————"
        blocks.append(block)

    text = "\n".join(blocks).rstrip()
    print("[DBG] Preview 500:\n" + text[:500].replace("\n","¶") + "…")
    send_telegram_text(text)
    print("OK")

if __name__ == "__main__":
    main()
