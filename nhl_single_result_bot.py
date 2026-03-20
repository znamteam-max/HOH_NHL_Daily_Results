#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — per-game posts & autopost (no repeats)
"""

from __future__ import annotations
import os, re, json, time, textwrap, pathlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

try:
    from bs4 import BeautifulSoup as BS  # type: ignore
    HAS_BS = True
except Exception:
    HAS_BS = False

TG_API     = "https://api.telegram.org"
NHLE_BASE  = "https://api-web.nhle.com/v1"
PBP_FMT    = NHLE_BASE + "/gamecenter/{gamePk}/play-by-play"
SCHED_FMT  = NHLE_BASE + "/schedule/{ymd}"

PT_TZ = ZoneInfo("America/Los_Angeles")

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except:
        return default

DRY_RUN = _env_bool("DRY_RUN", False)
DEBUG_VERBOSE = _env_bool("DEBUG_VERBOSE", False)
STATE_PATH = _env_str("STATE_PATH", "state/posted_games.json").strip() or "state/posted_games.json"

TEAM_RU = {
    "ANA":"Анахайм","ARI":"Аризона","BOS":"Бостон","BUF":"Баффало","CGY":"Калгари","CAR":"Каролина",
    "CHI":"Чикаго","COL":"Колорадо","CBJ":"Коламбус","DAL":"Даллас","DET":"Детройт","EDM":"Эдмонтон",
    "FLA":"Флорида","LAK":"Лос-Анджелес","MIN":"Миннесота","MTL":"Монреаль","NSH":"Нэшвилл",
    "NJD":"Нью-Джерси","NYI":"Айлендерс","NYR":"Рейнджерс","OTT":"Оттава","PHI":"Филадельфия",
    "PIT":"Питтсбург","SJS":"Сан-Хосе","SEA":"Сиэтл","STL":"Сент-Луис","TBL":"Тампа-Бэй",
    "TOR":"Торонто","VAN":"Ванкувер","VGK":"Вегас","WSH":"Вашингтон","WPG":"Виннипег","UTA":"Юта",
}
TEAM_EMOJI = {
    "ANA":"🦆","ARI":"🦂","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️","CBJ":"💣",
    "DAL":"⭐️","DET":"🛡️","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦","NSH":"🐯",
    "NJD":"😈","NYI":"🏝️","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈","SEA":"🦑","STL":"🎵",
    "TBL":"⚡","TOR":"🍁","VAN":"🐳","VGK":"🎰","WSH":"🦅","WPG":"✈️","UTA":"🧊",
}

SPORTSRU_SLUGS = {
    "ANA":["anaheim-ducks"],
    "ARI":["arizona-coyotes"],
    "BOS":["boston-bruins"],
    "BUF":["buffalo-sabres"],
    "CGY":["calgary-flames"],
    "CAR":["carolina-hurricanes"],
    "CHI":["chicago-blackhawks"],
    "COL":["colorado-avalanche"],
    "CBJ":["columbus-blue-jackets"],
    "DAL":["dallas-stars"],
    "DET":["detroit-red-wings"],
    "EDM":["edmonton-oilers"],
    "FLA":["florida-panthers"],
    "LAK":["los-angeles-kings","la-kings"],
    "MIN":["minnesota-wild"],
    "MTL":["montreal-canadiens"],
    "NSH":["nashville-predators"],
    "NJD":["new-jersey-devils"],
    "NYI":["new-york-islanders"],
    "NYR":["new-york-rangers"],
    "OTT":["ottawa-senators"],
    "PHI":["philadelphia-flyers"],
    "PIT":["pittsburgh-penguins"],
    "SJS":["san-jose-sharks"],
    "SEA":["seattle-kraken"],
    "STL":["st-louis-blues","saint-louis-blues","stlouis-blues"],
    "TBL":["tampa-bay-lightning"],
    "TOR":["toronto-maple-leafs"],
    "VAN":["vancouver-canucks"],
    "VGK":["vegas","vegas-golden-knights","vegas-knights","vgk"],
    "WSH":["washington-capitals"],
    "WPG":["winnipeg-jets"],
    "UTA":["utah-mammoth","utah","utah-hockey-club","utah-hc","utah-hc-nhl","utah-mammoths"],
}

UA_HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language":"ru,en;q=0.8",
}

def _get_with_retries(url: str, timeout: int = 30, tries: int = 3, backoff: float = 0.75, as_text: bool = False):
    last = None
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
            r.raise_for_status()
            if as_text:
                r.encoding = r.apparent_encoding or "utf-8"
                return r.text
            return r.json()
        except Exception as e:
            last = e
            if attempt < tries:
                sleep_s = backoff * (2 ** (attempt - 1))
                print(f"[DBG] retry {attempt}/{tries} for {url} after {sleep_s:.2f}s: {repr(e)}")
                time.sleep(sleep_s)
            else:
                raise
    raise last

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)

# --- Models ---
@dataclass
class TeamRecord:
    wins: int
    losses: int
    ot: int
    points: int
    def as_str(self) -> str:
        return f"{self.wins}-{self.losses}-{self.ot}"

@dataclass
class GameMeta:
    gamePk: int
    gameDateUTC: datetime
    state: str
    home_tri: str
    away_tri: str
    home_score: int
    away_score: int

@dataclass
class ScoringEvent:
    period: int
    period_type: str
    time: str
    team_for: str
    home_goals: int
    away_goals: int
    scorer: str
    assists: List[str] = field(default_factory=list)
    is_shootout_winner: bool = False

@dataclass
class SRUGoal:
    time: Optional[str]
    scorer_ru: Optional[str]
    assists_ru: List[str]

# --- Helpers ---
def _upper_str(x: Any) -> str:
    try:
        return str(x or "").upper()
    except:
        return ""

def _first_int(*vals) -> int:
    for v in vals:
        if v is None:
            continue
        try:
            s = str(v).strip()
            if s == "":
                continue
            return int(float(s))
        except:
            continue
    return 0

def _extract_name(obj_or_str: Any) -> Optional[str]:
    if not obj_or_str:
        return None
    if isinstance(obj_or_str, str):
        return obj_or_str.strip() or None
    if isinstance(obj_or_str, dict):
        for k in ("name","default","fullName","firstLastName","lastFirstName","shortName"):
            v = obj_or_str.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None

def _clean_person_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^\(+", "", s)
    s = re.sub(r"\)+$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _clean_assists(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for a in items or []:
        aa = _clean_person_name(a)
        if not aa:
            continue
        if aa not in seen:
            seen.add(aa)
            out.append(aa)
    return out

def _is_final_state(state: str) -> bool:
    return _upper_str(state) in ("FINAL", "OFF")

def _is_not_started_state(state: str) -> bool:
    return _upper_str(state) in ("PRE", "FUT", "SCHEDULED")

def _is_liveish_state(state: str) -> bool:
    return _upper_str(state) in ("LIVE", "CRIT")

def _current_hockey_day_pt() -> str:
    now_pt = datetime.now(PT_TZ)
    hockey_day = now_pt.date() if now_pt.hour >= 6 else (now_pt.date() - timedelta(days=1))
    return hockey_day.isoformat()

# --- Standings / schedule / PBP / sports.ru ---
def fetch_standings_map() -> Dict[str,TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"
    data = http_get_json(url)
    teams: Dict[str,TeamRecord] = {}
    nodes = []
    if isinstance(data,dict):
        if isinstance(data.get("standings"),list):
            nodes = data["standings"]
        elif isinstance(data.get("records"),list):
            nodes = data["records"]
        elif isinstance(data.get("standings"),dict):
            nodes = data["standings"].get("overallRecords",[]) or []
    elif isinstance(data,list):
        nodes = data
    for r in nodes:
        abbr = ""
        ta = r.get("teamAbbrev")
        if isinstance(ta,str):
            abbr = ta.upper()
        elif isinstance(ta,dict):
            abbr = _upper_str(ta.get("default") or ta.get("tricode"))
        if not abbr:
            abbr = _upper_str(r.get("teamAbbrevTricode") or r.get("teamTriCode") or r.get("team"))
        rec = r.get("record") or r.get("overallRecord") or r.get("overallRecords") or {}
        wins = _first_int(rec.get("wins"),r.get("wins"),rec.get("gamesPlayedWins"))
        losses = _first_int(rec.get("losses"),r.get("losses"),rec.get("gamesPlayedLosses"),rec.get("regulationLosses"),r.get("regulationLosses"))
        ot = _first_int(rec.get("ot"),r.get("ot"),rec.get("otLosses"),r.get("otLosses"),rec.get("overtimeLosses"),r.get("overtimeLosses"))
        pts = _first_int(r.get("points"),rec.get("points"),r.get("pts"),r.get("teamPoints"))
        if abbr:
            teams[abbr] = TeamRecord(wins,losses,ot,pts)
    return teams

def _iter_dates_around_today(num_back: int = 2, num_fwd: int = 2) -> List[str]:
    now = datetime.now(timezone.utc).date()
    return [(now + timedelta(days=off)).isoformat() for off in range(-num_back, num_fwd+1)]

def _list_games_for_dates(dates: List[str]) -> List[dict]:
    raw = []
    for day in dates:
        js = http_get_json(SCHED_FMT.format(ymd=day))
        games = js.get("games")
        if games is None:
            weeks = js.get("gameWeek") or []
            games = []
            for w in weeks:
                games.extend(w.get("games") or [])
        raw.extend(games or [])
    return raw

def _game_to_meta(g: dict) -> Optional[GameMeta]:
    gid = _first_int(g.get("id"),g.get("gameId"),g.get("gamePk"))
    if gid == 0:
        return None
    state = _upper_str(g.get("gameState") or g.get("gameStatus"))
    gd = g.get("startTimeUTC") or g.get("gameDate") or ""
    try:
        gdt = datetime.fromisoformat(str(gd).replace("Z","+00:00"))
    except:
        gdt = datetime.now(timezone.utc)
    home = g.get("homeTeam",{}) or {}
    away = g.get("awayTeam",{}) or {}
    htri = _upper_str(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
    atri = _upper_str(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
    hscore = _first_int(home.get("score"))
    ascore = _first_int(away.get("score"))
    return GameMeta(gid,gdt,state,htri,atri,hscore,ascore)

def resolve_game_by_query(q: str) -> Optional[GameMeta]:
    q = q.strip()
    if not q:
        return None
    try:
        date_part, rest = q.split(" ", 1)
        y,m,d = map(int, date_part.split("-"))
    except Exception:
        print(f"[DBG] GAME_QUERY bad format: {q}")
        return None
    rest = rest.strip().upper().replace(" ", "")
    home = away = ""
    if "@" in rest:
        away, home = rest.split("@",1)
    elif "-" in rest:
        left, right = rest.split("-",1)
        home, away = left, right
    else:
        return None

    js_for_day = _list_games_for_dates([f"{y:04d}-{m:02d}-{d:02d}"])
    if d > 1:
        js_for_day += _list_games_for_dates([f"{y:04d}-{m:02d}-{d-1:02d}"])
    metas = [_game_to_meta(g) for g in js_for_day]
    metas = [m for m in metas if m]
    for m in metas:
        if m.home_tri == home and m.away_tri == away:
            print(f"[DBG] Resolved GAME_PK={m.gamePk} for {q}")
            return m
    print(f"[DBG] Unable to resolve GAME_PK for {q}")
    return None

_SO_TYPES_GOAL = {"GOAL","SHOT"}
_ASSIST_KEYS = (
    "assist1PlayerName","assist2PlayerName","assist3PlayerName",
    "assist1","assist2","assist3",
    "primaryAssist","secondaryAssist","tertiaryAssist",
)
_SCORER_KEYS = (
    "scoringPlayerName","scorerName","shootingPlayerName","scoringPlayer",
    "goalScorer","primaryScorer","playerName","player",
    "shooterName","shootoutShooterName","shooter","byPlayerName",
)

def _normalize_period_type(t: str) -> str:
    t = _upper_str(t)
    if t in ("","REG"):
        return "REGULAR"
    if t == "OT":
        return "OVERTIME"
    if t == "SO":
        return "SHOOTOUT"
    return t

def _is_shootout_goal(type_key: str, details: dict, period_type: str) -> bool:
    if period_type != "SHOOTOUT":
        return False
    if type_key not in _SO_TYPES_GOAL:
        return False
    for k in ("wasGoal","shotWasGoal","isGoal","isScored","scored"):
        v = details.get(k)
        if isinstance(v, bool) and v:
            return True
        if isinstance(v, str) and v.strip().lower() in ("1","true","yes"):
            return True
    return type_key == "GOAL"

def _players_fallback_names(p: dict) -> Tuple[str, List[str]]:
    scorer = ""
    assists = []
    try:
        for pl in p.get("players") or []:
            pt = (_upper_str(pl.get("playerType")) or _upper_str(pl.get("type"))).strip()
            nm = _extract_name(pl.get("player") or pl.get("playerName") or pl.get("name"))
            if pt in ("SCORER","SHOOTOUTSCORER","SHOOTER","GOALSCORER"):
                if nm:
                    scorer = nm
            elif pt in ("ASSIST","PRIMARYASSIST","SECONDARYASSIST","TERTIARYASSIST"):
                if nm:
                    assists.append(nm)
    except Exception:
        pass
    return scorer, assists

def _is_deciding_shootout_goal(play: dict, details: dict) -> bool:
    for key in (
        "isGameWinningGoal",
        "isWinningGoal",
        "isGameDecidingGoal",
        "gameWinningGoal",
        "decidingGoal",
    ):
        val = details.get(key)
        if isinstance(val, bool) and val:
            return True
        if isinstance(val, str) and val.strip().lower() in ("1", "true", "yes"):
            return True

    desc = str(play.get("typeDescKey") or "").upper()
    if desc in ("SHOOTOUT_COMPLETE",):
        return False

    return False

def fetch_scoring_official(gamePk: int, home_tri: str, away_tri: str) -> List[ScoringEvent]:
    data = http_get_json(PBP_FMT.format(gamePk=gamePk))
    plays = data.get("plays",[]) or []
    events: List[ScoringEvent] = []
    prev_h = prev_a = 0

    for p in plays:
        type_key = _upper_str(p.get("typeDescKey"))
        pd = p.get("periodDescriptor",{}) or {}
        period = _first_int(pd.get("number") or p.get("period"))
        ptype = _normalize_period_type(pd.get("periodType") or "REG")
        det = p.get("details",{}) or {}
        t = str(p.get("timeInPeriod") or "00:00").replace(":", ".")

        is_goal = (type_key == "GOAL") or _is_shootout_goal(type_key, det, ptype)
        if not is_goal:
            continue

        h = det.get("homeScore")
        a = det.get("awayScore")
        if not (isinstance(h,int) and isinstance(a,int)):
            sc = p.get("score",{}) or {}
            if isinstance(sc.get("home"),int) and isinstance(sc.get("away"),int):
                h,a = sc["home"],sc["away"]
            else:
                h,a = prev_h,prev_a

        team = home_tri if h > prev_h else (away_tri if a > prev_a else _upper_str(det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")))

        scorer = ""
        for k in _SCORER_KEYS:
            nm = _extract_name(det.get(k))
            if nm:
                scorer = nm
                break
        if not scorer:
            for k in ("scoringPlayerName","scorerName","shootingPlayerName"):
                v = p.get(k)
                if isinstance(v,str) and v.strip():
                    scorer = v.strip()
                    break
        if not scorer:
            sfb, _ = _players_fallback_names(p)
            if sfb:
                scorer = sfb

        assists = []
        for k in _ASSIST_KEYS:
            nm = _extract_name(det.get(k))
            if nm:
                assists.append(nm)
        if not assists:
            _, afb = _players_fallback_names(p)
            if afb:
                assists = afb

        ev = ScoringEvent(
            period=period,
            period_type=ptype,
            time=t,
            team_for=team,
            home_goals=_first_int(h),
            away_goals=_first_int(a),
            scorer=_clean_person_name(scorer),
            assists=_clean_assists(assists),
            is_shootout_winner=False,
        )

        if ptype == "SHOOTOUT":
            ev.is_shootout_winner = _is_deciding_shootout_goal(p, det)

        events.append(ev)

        if ptype != "SHOOTOUT":
            prev_h, prev_a = _first_int(h), _first_int(a)

    return events

TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")

def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or "")
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None

def parse_sportsru_goals_html(html: str, side: str) -> List[SRUGoal]:
    res: List[SRUGoal] = []
    if HAS_BS:
        soup = BS(html,"lxml" if "lxml" in globals() else "html.parser")
        ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors = [a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru = anchors[0] if anchors else None
                assists_ru = anchors[1:] if len(anchors) > 1 else []
                raw = li.get_text(" ", strip=True)
                time_ru = _extract_time(raw)
                res.append(SRUGoal(time_ru, scorer_ru, assists_ru))
    return res

def fetch_sportsru_goals(home_tri: str, away_tri: str) -> Tuple[List[SRUGoal],List[SRUGoal],str]:
    h_list = SPORTSRU_SLUGS.get(home_tri, [])
    a_list = SPORTSRU_SLUGS.get(away_tri, [])
    tried = []
    for hslug in h_list:
        for aslug in a_list:
            for left,right in ((hslug,aslug),(aslug,hslug)):
                url = f"https://www.sports.ru/hockey/match/{left}-vs-{right}/"
                tried.append(url)
                try:
                    html = http_get_text(url, timeout=20)
                except Exception as e:
                    if DEBUG_VERBOSE:
                        print(f"[DBG] sports.ru fetch fail {url}: {repr(e)}")
                    continue
                left_is_home = left in h_list
                home_side = "home" if left_is_home else "away"
                away_side = "away" if left_is_home else "home"
                h = parse_sportsru_goals_html(html, home_side)
                a = parse_sportsru_goals_html(html, away_side)
                if h or a:
                    print(f"[DBG] sports.ru goals ok for {url}: home={len(h)} away={len(a)}")
                    return h,a,url
    if DEBUG_VERBOSE and tried:
        print("[DBG] sports.ru tried URLs (no data):", " | ".join(tried))
    return [],[], ""

def merge_official_with_sportsru(
    evs: List[ScoringEvent],
    sru_home: List[SRUGoal],
    sru_away: List[SRUGoal],
    home_tri: str,
    away_tri: str
) -> List[ScoringEvent]:
    h_i = a_i = 0
    out = []

    for ev in evs:
        if ev.period_type == "SHOOTOUT":
            out.append(ev)
            continue

        if ev.team_for == home_tri and h_i < len(sru_home):
            g = sru_home[h_i]
            h_i += 1
            if g.scorer_ru:
                ev.scorer = _clean_person_name(g.scorer_ru)
            if g.assists_ru:
                ev.assists = _clean_assists(g.assists_ru)
        elif ev.team_for == away_tri and a_i < len(sru_away):
            g = sru_away[a_i]
            a_i += 1
            if g.scorer_ru:
                ev.scorer = _clean_person_name(g.scorer_ru)
            if g.assists_ru:
                ev.assists = _clean_assists(g.assists_ru)

        ev.assists = _clean_assists(ev.assists)
        out.append(ev)

    return out

def _italic(s: str) -> str:
    return f"<i>{s}</i>"

def period_title_text(num: int, ptype: str, ot_index: Optional[int], ot_total: int) -> str:
    t = (ptype or "").upper()
    if t == "REGULAR":
        return f"{num}-й период"
    if t == "OVERTIME":
        return "Овертайм" if ot_total <= 1 else f"Овертайм №{ot_index or 1}"
    if t == "SHOOTOUT":
        return "Буллиты"
    return f"Период {num}"

def line_goal(ev: ScoringEvent) -> str:
    score = f"{ev.home_goals}:{ev.away_goals}"
    who = _clean_person_name(ev.scorer) or "—"
    assists = _clean_assists(ev.assists)
    assists_text = f" ({', '.join(assists)})" if assists else ""
    return f"{score} – {ev.time} {who}{assists_text}"

def get_winning_shootout_event(events: List[ScoringEvent]) -> Optional[ScoringEvent]:
    shootout_events = [ev for ev in events if ev.period_type == "SHOOTOUT"]
    for ev in shootout_events:
        if ev.is_shootout_winner:
            return ev

    scored = [ev for ev in shootout_events if _clean_person_name(ev.scorer)]
    if scored:
        return scored[-1]

    return None

def build_single_match_text(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he = TEAM_EMOJI.get(meta.home_tri,"")
    ae = TEAM_EMOJI.get(meta.away_tri,"")
    hn = TEAM_RU.get(meta.home_tri,meta.home_tri)
    an = TEAM_RU.get(meta.away_tri,meta.away_tri)
    hrec = standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec = standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head = f"{he} <b>«{hn}»: {meta.home_score}</b> ({hrec})\n{ae} <b>«{an}»: {meta.away_score}</b> ({arec})"

    regular_and_ot = [ev for ev in events if ev.period_type != "SHOOTOUT"]
    winning_so = get_winning_shootout_event(events)

    groups: Dict[Tuple[int,str],List[ScoringEvent]] = {}
    for ev in regular_and_ot:
        groups.setdefault((ev.period,ev.period_type),[]).append(ev)

    for pnum in (1,2,3):
        if (pnum,"REGULAR") not in groups:
            groups[(pnum,"REGULAR")] = []

    ot_keys = sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total = len(ot_keys)
    ot_order = {k:i+1 for i,k in enumerate(ot_keys)}

    lines = [head]
    sort_key = lambda x:(x[0], 0 if (x[1] or "").upper()=="REGULAR" else 1)
    for key in sorted(groups.keys(), key=sort_key):
        pnum,ptype = key
        ot_idx = ot_order.get(key)
        title = period_title_text(pnum,ptype,ot_idx,ot_total)
        lines.append("")
        lines.append(_italic(title))
        per = groups[key]
        if not per:
            lines.append("Голов не было")
        else:
            for ev in per:
                lines.append(line_goal(ev))

    if winning_so:
        lines.append("")
        lines.append("Победный буллит")
        lines.append(f"{meta.home_score}:{meta.away_score} – {_clean_person_name(winning_so.scorer) or '—'}")

    return "\n".join(lines).strip()

# --- State & Telegram ---
def load_state(path: str) -> Dict[str,Any]:
    p = pathlib.Path(path)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        return {"posted":{}}
    try:
        return json.loads(p.read_text("utf-8") or "{}") or {"posted":{}}
    except Exception:
        return {"posted":{}}

def save_state(path: str, data: Dict[str,Any]) -> None:
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")

def send_telegram_text(text: str) -> None:
    token = _env_str("TELEGRAM_BOT_TOKEN","").strip()
    chat_id = _env_str("TELEGRAM_CHAT_ID","").strip()
    thread = _env_str("TELEGRAM_THREAD_ID","").strip()
    if not token or not chat_id:
        print("[ERR] Telegram token/chat_id not set")
        return
    url = f"{TG_API}/bot{token}/sendMessage"
    headers = {"Content-Type":"application/json"}
    payload = {
        "chat_id": int(chat_id) if chat_id.strip("-").isdigit() else chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "disable_notification": False,
        "parse_mode": "HTML",
    }
    if thread:
        try:
            payload["message_thread_id"] = int(thread)
        except:
            pass
    if DRY_RUN:
        print("[DRY RUN] " + textwrap.shorten(text, 200, placeholder="…"))
        return
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    try:
        data = resp.json()
    except:
        data = {"ok":None,"raw":resp.text}
    print(f"[DBG] TG HTTP={resp.status_code} JSON={data}")
    if resp.status_code != 200 or not data.get("ok",False):
        print(f"[ERR] sendMessage failed: {data.get('error_code')} {data.get('description')}")

# --- Resolve meta / autopost ---
def get_meta_by_gamepk_scan_schedule(gamePk: int) -> Optional[GameMeta]:
    raw = _list_games_for_dates(_iter_dates_around_today(3,3))
    for g in raw:
        gid = _first_int(g.get("id"),g.get("gameId"),g.get("gamePk"))
        if gid == gamePk:
            return _game_to_meta(g)
    return None

def autopost_current_hockey_day() -> List[GameMeta]:
    raw = _list_games_for_dates([_current_hockey_day_pt()])
    metas = [_game_to_meta(g) for g in raw]
    metas = [m for m in metas if m]
    finals = [m for m in metas if _is_final_state(m.state)]
    finals = sorted(finals, key=lambda m:m.gameDateUTC)
    return finals

def pending_game_text(meta: GameMeta) -> str:
    away = meta.away_tri
    home = meta.home_tri
    matchup = f"{away} - {home}"
    if _is_not_started_state(meta.state):
        return f"{matchup} ещё не началась"
    if _is_liveish_state(meta.state):
        return f"{matchup} ещё не завершилась"
    return f"{matchup} статус: {meta.state}"

def main():
    game_pk = _env_str("GAME_PK","").strip()
    game_query = _env_str("GAME_QUERY","").strip()

    standings = fetch_standings_map()
    state = load_state(STATE_PATH)
    posted: Dict[str,bool] = state.get("posted",{}) or {}

    metas: List[GameMeta] = []
    manual_mode = False

    if game_pk:
        gid = int(game_pk)
        meta = get_meta_by_gamepk_scan_schedule(gid)
        if not meta:
            print(f"[ERR] GAME_PK not found in schedule window: {gid}")
            return
        metas = [meta]
        manual_mode = True
    elif game_query:
        meta = resolve_game_by_query(game_query)
        if not meta:
            print(f"[ERR] GAME_QUERY not resolved: {game_query}")
            return
        metas = [meta]
        manual_mode = True
    else:
        metas = autopost_current_hockey_day()
        print("FINAL games:", [m.gamePk for m in metas])
        metas = [m for m in metas if not posted.get(str(m.gamePk))]
        print("Need to post:", [m.gamePk for m in metas])

    new_posts = 0
    for meta in metas:
        if manual_mode and not _is_final_state(meta.state):
            text = pending_game_text(meta)
            print("[DBG] Pending preview:\n" + text)
            send_telegram_text(text)
            continue

        evs = fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)
        sru_home, sru_away, _ = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        merged = merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)
        text = build_single_match_text(meta, standings, merged)
        print("[DBG] Single match preview:\n" + text[:500].replace("\n","¶") + "…")
        send_telegram_text(text)

        if not manual_mode:
            posted[str(meta.gamePk)] = True
            new_posts += 1

    state["posted"] = posted
    save_state(STATE_PATH, state)
    print(f"OK (posted {new_posts})")

if __name__=="__main__":
    main()
