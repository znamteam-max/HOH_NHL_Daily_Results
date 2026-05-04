#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — per-game posts & autopost (no repeats)
"""

from __future__ import annotations
import os
import re
import json
import time
import textwrap
import pathlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

import requests

try:
    from bs4 import BeautifulSoup as BS  # type: ignore
    HAS_BS = True
except Exception:
    HAS_BS = False

TG_API = "https://api.telegram.org"
DEFAULT_TELEGRAM_CHAT_ID = "-1003167239288"
NHLE_BASE = "https://api-web.nhle.com/v1"
PBP_FMT = NHLE_BASE + "/gamecenter/{gamePk}/play-by-play"
SCHED_FMT = NHLE_BASE + "/schedule/{ymd}"

PT_TZ = ZoneInfo("America/Los_Angeles")


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


DRY_RUN = _env_bool("DRY_RUN", False)
DEBUG_VERBOSE = _env_bool("DEBUG_VERBOSE", False)
STATE_PATH = _env_str("STATE_PATH", "state/posted_games.json").strip() or "state/posted_games.json"
TARGET_DATE = _env_str("TARGET_DATE", "").strip()

TEAM_RU = {
    "ANA": "Анахайм", "ARI": "Аризона", "BOS": "Бостон", "BUF": "Баффало", "CGY": "Калгари", "CAR": "Каролина",
    "CHI": "Чикаго", "COL": "Колорадо", "CBJ": "Коламбус", "DAL": "Даллас", "DET": "Детройт", "EDM": "Эдмонтон",
    "FLA": "Флорида", "LAK": "Лос-Анджелес", "MIN": "Миннесота", "MTL": "Монреаль", "NSH": "Нэшвилл",
    "NJD": "Нью-Джерси", "NYI": "Айлендерс", "NYR": "Рейнджерс", "OTT": "Оттава", "PHI": "Филадельфия",
    "PIT": "Питтсбург", "SJS": "Сан-Хосе", "SEA": "Сиэтл", "STL": "Сент-Луис", "TBL": "Тампа-Бэй",
    "TOR": "Торонто", "VAN": "Ванкувер", "VGK": "Вегас", "WSH": "Вашингтон", "WPG": "Виннипег", "UTA": "Юта",
}
TEAM_EMOJI = {
    "ANA": "🦆", "ARI": "🦂", "BOS": "🐻", "BUF": "🦬", "CGY": "🔥", "CAR": "🌪️", "CHI": "🦅", "COL": "⛰️", "CBJ": "💣",
    "DAL": "⭐️", "DET": "🛡️", "EDM": "🛢️", "FLA": "🐆", "LAK": "👑", "MIN": "🌲", "MTL": "🇨🇦", "NSH": "🐯",
    "NJD": "😈", "NYI": "🏝️", "NYR": "🗽", "OTT": "🛡", "PHI": "🛩", "PIT": "🐧", "SJS": "🦈", "SEA": "🦑", "STL": "🎵",
    "TBL": "⚡", "TOR": "🍁", "VAN": "🐳", "VGK": "🎰", "WSH": "🦅", "WPG": "✈️", "UTA": "🧊",
}

SPORTSRU_SLUGS = {
    "ANA": ["anaheim-ducks"],
    "ARI": ["arizona-coyotes"],
    "BOS": ["boston-bruins"],
    "BUF": ["buffalo-sabres"],
    "CGY": ["calgary-flames"],
    "CAR": ["carolina-hurricanes"],
    "CHI": ["chicago-blackhawks"],
    "COL": ["colorado-avalanche"],
    "CBJ": ["columbus-blue-jackets"],
    "DAL": ["dallas-stars"],
    "DET": ["detroit-red-wings"],
    "EDM": ["edmonton-oilers"],
    "FLA": ["florida-panthers"],
    "LAK": ["los-angeles-kings", "la-kings"],
    "MIN": ["minnesota-wild"],
    "MTL": ["montreal-canadiens"],
    "NSH": ["nashville-predators"],
    "NJD": ["new-jersey-devils"],
    "NYI": ["new-york-islanders"],
    "NYR": ["new-york-rangers"],
    "OTT": ["ottawa-senators"],
    "PHI": ["philadelphia-flyers"],
    "PIT": ["pittsburgh-penguins"],
    "SJS": ["san-jose-sharks"],
    "SEA": ["seattle-kraken"],
    "STL": ["st-louis-blues", "saint-louis-blues", "stlouis-blues"],
    "TBL": ["tampa-bay-lightning"],
    "TOR": ["toronto-maple-leafs"],
    "VAN": ["vancouver-canucks"],
    "VGK": ["vegas", "vegas-golden-knights", "vegas-knights", "vgk"],
    "WSH": ["washington-capitals"],
    "WPG": ["winnipeg-jets"],
    "UTA": ["utah-mammoth", "utah", "utah-hockey-club", "utah-hc", "utah-hc-nhl", "utah-mammoths"],
}

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.8",
}


def dbg(*args: Any) -> None:
    if DEBUG_VERBOSE:
        print("[DBG]", *args, flush=True)


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
                dbg(f"retry {attempt}/{tries} for {url} after {sleep_s:.2f}s: {repr(e)}")
                time.sleep(sleep_s)
            else:
                raise
    raise last


def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)


def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)


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
    series_game: Optional[int] = None
    home_series_wins: Optional[int] = None
    away_series_wins: Optional[int] = None


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
    is_shootout_scored: bool = False


@dataclass
class SRUGoal:
    time: Optional[str]
    scorer_ru: Optional[str]
    assists_ru: List[str]


@dataclass
class SRUShootoutWinner:
    scorer_ru: Optional[str]


def _upper_str(x: Any) -> str:
    try:
        return str(x or "").upper()
    except Exception:
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
        except Exception:
            continue
    return 0


def _truthy(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "y", "on")
    return False


def _extract_name(obj_or_str: Any) -> Optional[str]:
    if not obj_or_str:
        return None
    if isinstance(obj_or_str, str):
        return obj_or_str.strip() or None
    if isinstance(obj_or_str, dict):
        for k in ("name", "default", "fullName", "firstLastName", "lastFirstName", "shortName"):
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


def _roster_name_map(data: dict) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for spot in data.get("rosterSpots") or []:
        pid = _first_int(spot.get("playerId"))
        if not pid:
            continue
        first = _extract_name(spot.get("firstName")) or ""
        last = _extract_name(spot.get("lastName")) or ""
        full = _clean_person_name(f"{first} {last}")
        if full:
            out[pid] = full
    return out


def _player_name_from_id(details: dict, roster_names: Dict[int, str], *keys: str) -> str:
    for key in keys:
        pid = _first_int(details.get(key))
        if pid and pid in roster_names:
            return roster_names[pid]
    return ""


def _is_valid_player_name(s: str) -> bool:
    s = _clean_person_name(s)
    if not s:
        return False
    if len(s) > 40:
        return False
    if "НХЛ." in s or "Серия буллитов" in s:
        return False
    if re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", s):
        return False
    if re.search(r"\d", s):
        return False
    return True


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


def _target_base_date() -> date:
    if TARGET_DATE:
        try:
            return datetime.strptime(TARGET_DATE, "%Y-%m-%d").date()
        except Exception:
            print(f"[ERR] bad TARGET_DATE: {TARGET_DATE}, expected YYYY-MM-DD")
    return datetime.fromisoformat(_current_hockey_day_pt()).date()


def fetch_standings_map() -> Dict[str, TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"
    data = http_get_json(url)
    teams: Dict[str, TeamRecord] = {}
    nodes: List[dict] = []

    if isinstance(data, dict):
        if isinstance(data.get("standings"), list):
            nodes = data["standings"]
        elif isinstance(data.get("records"), list):
            nodes = data["records"]
        elif isinstance(data.get("standings"), dict):
            nodes = data["standings"].get("overallRecords", []) or []
    elif isinstance(data, list):
        nodes = data

    for r in nodes:
        abbr = ""
        ta = r.get("teamAbbrev")
        if isinstance(ta, str):
            abbr = ta.upper()
        elif isinstance(ta, dict):
            abbr = _upper_str(ta.get("default") or ta.get("tricode"))
        if not abbr:
            abbr = _upper_str(r.get("teamAbbrevTricode") or r.get("teamTriCode") or r.get("team"))

        rec = r.get("record") or r.get("overallRecord") or r.get("overallRecords") or {}
        wins = _first_int(rec.get("wins"), r.get("wins"), rec.get("gamesPlayedWins"))
        losses = _first_int(rec.get("losses"), r.get("losses"), rec.get("gamesPlayedLosses"), rec.get("regulationLosses"), r.get("regulationLosses"))
        ot = _first_int(rec.get("ot"), r.get("ot"), rec.get("otLosses"), r.get("otLosses"), rec.get("overtimeLosses"), r.get("overtimeLosses"))
        pts = _first_int(r.get("points"), rec.get("points"), r.get("pts"), r.get("teamPoints"))
        if abbr:
            teams[abbr] = TeamRecord(wins, losses, ot, pts)
    return teams


def _iter_dates_around_today(num_back: int = 2, num_fwd: int = 2) -> List[str]:
    now = datetime.now(timezone.utc).date()
    return [(now + timedelta(days=off)).isoformat() for off in range(-num_back, num_fwd + 1)]


def _list_games_for_dates(dates: List[str]) -> List[dict]:
    raw: List[dict] = []
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
    gid = _first_int(g.get("id"), g.get("gameId"), g.get("gamePk"))
    if gid == 0:
        return None

    state = _upper_str(g.get("gameState") or g.get("gameStatus"))
    gd = g.get("startTimeUTC") or g.get("gameDate") or ""
    try:
        gdt = datetime.fromisoformat(str(gd).replace("Z", "+00:00"))
    except Exception:
        gdt = datetime.now(timezone.utc)

    home = g.get("homeTeam", {}) or {}
    away = g.get("awayTeam", {}) or {}
    htri = _upper_str(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
    atri = _upper_str(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
    hscore = _first_int(home.get("score"))
    ascore = _first_int(away.get("score"))

    series_game: Optional[int] = None
    home_series_wins: Optional[int] = None
    away_series_wins: Optional[int] = None
    series = g.get("seriesStatus") or {}
    if isinstance(series, dict) and series:
        game_no = _first_int(series.get("gameNumberOfSeries"))
        series_game = game_no or None
        top = _upper_str(series.get("topSeedTeamAbbrev"))
        bottom = _upper_str(series.get("bottomSeedTeamAbbrev"))
        top_wins = _first_int(series.get("topSeedWins"))
        bottom_wins = _first_int(series.get("bottomSeedWins"))
        if htri == top:
            home_series_wins = top_wins
        elif htri == bottom:
            home_series_wins = bottom_wins
        if atri == top:
            away_series_wins = top_wins
        elif atri == bottom:
            away_series_wins = bottom_wins

    if (
        series_game
        and _is_final_state(state)
        and home_series_wins is not None
        and away_series_wins is not None
        and hscore != ascore
        and home_series_wins + away_series_wins == series_game - 1
    ):
        if hscore > ascore:
            home_series_wins += 1
        else:
            away_series_wins += 1

    return GameMeta(
        gid,
        gdt,
        state,
        htri,
        atri,
        hscore,
        ascore,
        series_game,
        home_series_wins,
        away_series_wins,
    )


def resolve_game_by_query(q: str) -> Optional[GameMeta]:
    q = q.strip()
    if not q:
        return None

    try:
        date_part, rest = q.split(" ", 1)
        y, m, d = map(int, date_part.split("-"))
    except Exception:
        print(f"[DBG] GAME_QUERY bad format: {q}")
        return None

    rest = rest.strip().upper().replace(" ", "")
    home = away = ""
    if "@" in rest:
        away, home = rest.split("@", 1)
    elif "-" in rest:
        left, right = rest.split("-", 1)
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
            dbg(f"Resolved GAME_PK={m.gamePk} for {q}")
            return m

    print(f"[DBG] Unable to resolve GAME_PK for {q}")
    return None


_ASSIST_KEYS = (
    "assist1PlayerName", "assist2PlayerName", "assist3PlayerName",
    "assist1", "assist2", "assist3",
    "primaryAssist", "secondaryAssist", "tertiaryAssist",
)
_SCORER_KEYS = (
    "scoringPlayerName", "scorerName", "shootingPlayerName", "scoringPlayer",
    "goalScorer", "primaryScorer", "playerName", "player",
    "shooterName", "shootoutShooterName", "shooter", "byPlayerName",
)


def _normalize_period_type(t: str) -> str:
    t = _upper_str(t)
    if t in ("", "REG"):
        return "REGULAR"
    if t == "OT":
        return "OVERTIME"
    if t == "SO":
        return "SHOOTOUT"
    return t


def _players_fallback_names(p: dict) -> Tuple[str, List[str]]:
    scorer = ""
    assists: List[str] = []
    try:
        for pl in p.get("players") or []:
            pt = (_upper_str(pl.get("playerType")) or _upper_str(pl.get("type"))).strip()
            nm = _extract_name(pl.get("player") or pl.get("playerName") or pl.get("name"))
            if pt in ("SCORER", "SHOOTOUTSCORER", "SHOOTER", "GOALSCORER"):
                if nm:
                    scorer = nm
            elif pt in ("ASSIST", "PRIMARYASSIST", "SECONDARYASSIST", "TERTIARYASSIST"):
                if nm:
                    assists.append(nm)
    except Exception:
        pass
    return scorer, assists


def _is_deciding_shootout_goal(details: dict) -> bool:
    for key in ("isGameWinningGoal", "isWinningGoal", "isGameDecidingGoal", "gameWinningGoal", "decidingGoal"):
        if _truthy(details.get(key)):
            return True
    return False


def _extract_shootout_scorer(play: dict, details: dict, roster_names: Dict[int, str]) -> str:
    for k in _SCORER_KEYS:
        nm = _extract_name(details.get(k))
        if nm:
            return _clean_person_name(nm)
    nm = _player_name_from_id(details, roster_names, "scoringPlayerId", "shootingPlayerId", "playerId")
    if nm:
        return _clean_person_name(nm)
    for k in ("scoringPlayerName", "scorerName", "shootingPlayerName"):
        v = play.get(k)
        if isinstance(v, str) and v.strip():
            return _clean_person_name(v)
    sfb, _ = _players_fallback_names(play)
    return _clean_person_name(sfb)


def fetch_scoring_official(gamePk: int, home_tri: str, away_tri: str) -> Tuple[List[ScoringEvent], bool]:
    data = http_get_json(PBP_FMT.format(gamePk=gamePk))
    plays = data.get("plays", []) or []
    roster_names = _roster_name_map(data)
    events: List[ScoringEvent] = []

    official_has_shootout = False
    prev_h = prev_a = 0
    prev_so_h = prev_so_a = 0

    for p in plays:
        pd = p.get("periodDescriptor", {}) or {}
        period = _first_int(pd.get("number") or p.get("period"))
        ptype = _normalize_period_type(pd.get("periodType") or "REG")
        type_key = _upper_str(p.get("typeDescKey"))
        det = p.get("details", {}) or {}
        t = str(p.get("timeInPeriod") or "00:00").replace(":", ".")

        if ptype == "SHOOTOUT":
            official_has_shootout = True

            scorer = _extract_shootout_scorer(p, det, roster_names)

            h = det.get("homeScore")
            a = det.get("awayScore")
            if not (isinstance(h, int) and isinstance(a, int)):
                sc = p.get("score", {}) or {}
                h = sc.get("home", prev_so_h)
                a = sc.get("away", prev_so_a)

            h = _first_int(h, prev_so_h)
            a = _first_int(a, prev_so_a)

            scored = False
            if h > prev_so_h or a > prev_so_a:
                scored = True
            for k in ("wasGoal", "shotWasGoal", "isGoal", "isScored", "scored"):
                if _truthy(det.get(k)):
                    scored = True

            team = home_tri if h > prev_so_h else (
                away_tri if a > prev_so_a else _upper_str(
                    det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")
                )
            )

            if scorer or scored:
                events.append(
                    ScoringEvent(
                        period=period,
                        period_type="SHOOTOUT",
                        time=t,
                        team_for=team,
                        home_goals=h,
                        away_goals=a,
                        scorer=scorer,
                        assists=[],
                        is_shootout_winner=_is_deciding_shootout_goal(det),
                        is_shootout_scored=scored,
                    )
                )

            prev_so_h, prev_so_a = h, a
            continue

        if type_key != "GOAL":
            continue

        h = det.get("homeScore")
        a = det.get("awayScore")
        if not (isinstance(h, int) and isinstance(a, int)):
            sc = p.get("score", {}) or {}
            if isinstance(sc.get("home"), int) and isinstance(sc.get("away"), int):
                h, a = sc["home"], sc["away"]
            else:
                h, a = prev_h, prev_a

        team = home_tri if h > prev_h else (
            away_tri if a > prev_a else _upper_str(
                det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")
            )
        )

        scorer = ""
        for k in _SCORER_KEYS:
            nm = _extract_name(det.get(k))
            if nm:
                scorer = nm
                break
        if not scorer:
            scorer = _player_name_from_id(det, roster_names, "scoringPlayerId", "shootingPlayerId", "playerId")
        if not scorer:
            for k in ("scoringPlayerName", "scorerName", "shootingPlayerName"):
                v = p.get(k)
                if isinstance(v, str) and v.strip():
                    scorer = v.strip()
                    break
        if not scorer:
            sfb, _ = _players_fallback_names(p)
            if sfb:
                scorer = sfb

        assists: List[str] = []
        for k in _ASSIST_KEYS:
            nm = _extract_name(det.get(k))
            if nm:
                assists.append(nm)
        for k in ("assist1PlayerId", "assist2PlayerId", "assist3PlayerId"):
            nm = _player_name_from_id(det, roster_names, k)
            if nm:
                assists.append(nm)
        if not assists:
            _, afb = _players_fallback_names(p)
            if afb:
                assists = afb

        events.append(
            ScoringEvent(
                period=period,
                period_type=ptype,
                time=t,
                team_for=team,
                home_goals=_first_int(h),
                away_goals=_first_int(a),
                scorer=_clean_person_name(scorer),
                assists=_clean_assists(assists),
            )
        )
        prev_h, prev_a = _first_int(h), _first_int(a)

    return events, official_has_shootout


TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")


def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or "")
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None


def parse_sportsru_goals_html(html: str, side: str) -> List[SRUGoal]:
    res: List[SRUGoal] = []
    if not HAS_BS:
        return res

    soup = BS(html, "html.parser")
    ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or soup.select_one(
        f"ul.match-summary__goals-list.match-summary__goals-list--{side}"
    )
    if not ul:
        return res

    for li in ul.find_all("li", recursive=False):
        raw = li.get_text(" ", strip=True)
        if "Серия буллитов" in raw:
            continue
        anchors = [a.get_text(strip=True) for a in li.find_all("a")]
        scorer_ru = anchors[0] if anchors else None
        assists_ru = anchors[1:] if len(anchors) > 1 else []
        time_ru = _extract_time(raw)
        res.append(SRUGoal(time_ru, scorer_ru, assists_ru))
    return res


def parse_sportsru_shootout_winner_html(html: str) -> Optional[SRUShootoutWinner]:
    if not HAS_BS:
        return None

    soup = BS(html, "html.parser")
    containers = soup.select(
        "ul.match-summary__goals-list--home, "
        "ul.match-summary__goals-list--away, "
        "ul.match-summary__goals-list.match-summary__goals-list--home, "
        "ul.match-summary__goals-list.match-summary__goals-list--away"
    )

    for ul in containers:
        for li in ul.find_all("li", recursive=False):
            raw = li.get_text(" ", strip=True)
            if "Серия буллитов" not in raw:
                continue

            anchors = [a.get_text(strip=True) for a in li.find_all("a")]
            if not anchors:
                continue

            name = _clean_person_name(anchors[0])
            if _is_valid_player_name(name):
                return SRUShootoutWinner(scorer_ru=name)

    return None


def fetch_sportsru_goals(home_tri: str, away_tri: str) -> Tuple[List[SRUGoal], List[SRUGoal], Optional[SRUShootoutWinner], str]:
    h_list = SPORTSRU_SLUGS.get(home_tri, [])
    a_list = SPORTSRU_SLUGS.get(away_tri, [])
    tried: List[str] = []

    for hslug in h_list:
        for aslug in a_list:
            for left, right in ((hslug, aslug), (aslug, hslug)):
                url = f"https://www.sports.ru/hockey/match/{left}-vs-{right}/"
                tried.append(url)
                try:
                    html = http_get_text(url, timeout=20)
                except Exception as e:
                    dbg(f"sports.ru fetch fail {url}: {repr(e)}")
                    continue

                left_is_home = left in h_list
                home_side = "home" if left_is_home else "away"
                away_side = "away" if left_is_home else "home"

                h = parse_sportsru_goals_html(html, home_side)
                a = parse_sportsru_goals_html(html, away_side)
                so = parse_sportsru_shootout_winner_html(html)

                if h or a or so:
                    dbg(f"sports.ru ok for {url}: home={len(h)} away={len(a)} so={getattr(so, 'scorer_ru', None)}")
                    return h, a, so, url

    dbg("sports.ru tried URLs (no data):", " | ".join(tried))
    return [], [], None, ""


def merge_official_with_sportsru(
    evs: List[ScoringEvent],
    sru_home: List[SRUGoal],
    sru_away: List[SRUGoal],
    home_tri: str,
    away_tri: str,
) -> List[ScoringEvent]:
    h_i = a_i = 0
    out: List[ScoringEvent] = []

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


def compute_player_marks(events: List[ScoringEvent]) -> Dict[str, str]:
    goals: Dict[str, int] = {}
    assists: Dict[str, int] = {}

    for ev in events:
        if ev.period_type == "SHOOTOUT":
            continue

        scorer = _clean_person_name(ev.scorer)
        if scorer:
            goals[scorer] = goals.get(scorer, 0) + 1

        for a in _clean_assists(ev.assists):
            assists[a] = assists.get(a, 0) + 1

    marks: Dict[str, str] = {}
    for n in set(goals) | set(assists):
        suffix = ""
        if goals.get(n, 0) >= 2:
            suffix += " 🔥"
        if assists.get(n, 0) >= 3:
            suffix += " 🏒"
        if suffix:
            marks[n] = suffix
    return marks


def find_last_mentions(events: List[ScoringEvent], winning_so_name: Optional[str]) -> Dict[str, int]:
    last_idx: Dict[str, int] = {}
    idx = 0

    for ev in events:
        if ev.period_type == "SHOOTOUT":
            continue

        scorer = _clean_person_name(ev.scorer)
        if scorer:
            last_idx[scorer] = idx
        idx += 1

        for a in _clean_assists(ev.assists):
            last_idx[a] = idx
            idx += 1

    if winning_so_name:
        last_idx[_clean_person_name(winning_so_name)] = idx

    return last_idx


def _event_time_sort_value(ev: ScoringEvent) -> int:
    try:
        mm, ss = str(ev.time or "00.00").replace(":", ".").split(".", 1)
        return int(mm) * 60 + int(ss)
    except Exception:
        return 0


def find_winning_goal_event(meta: GameMeta, events: List[ScoringEvent]) -> Optional[ScoringEvent]:
    if meta.home_score == meta.away_score:
        return None

    winner = meta.home_tri if meta.home_score > meta.away_score else meta.away_tri
    candidates = [
        ev for ev in events
        if ev.period_type != "SHOOTOUT"
        and ev.team_for == winner
        and ev.home_goals == meta.home_score
        and ev.away_goals == meta.away_score
        and _is_valid_player_name(ev.scorer)
    ]
    if candidates:
        return sorted(candidates, key=lambda ev: (ev.period, _event_time_sort_value(ev)))[-1]

    fallback = [
        ev for ev in events
        if ev.period_type != "SHOOTOUT"
        and ev.team_for == winner
        and _is_valid_player_name(ev.scorer)
    ]
    if fallback:
        return sorted(fallback, key=lambda ev: (ev.period, _event_time_sort_value(ev)))[-1]
    return None


def overtime_winner_line(meta: GameMeta, events: List[ScoringEvent]) -> Optional[str]:
    ev = find_winning_goal_event(meta, events)
    if not ev or ev.period_type != "OVERTIME":
        return None

    ot_index = max(1, ev.period - 3)
    prep = "во" if ot_index == 2 else "в"
    scorer = _clean_person_name(ev.scorer)
    return f"<b>Победный гол {prep} {ot_index}-м ОТ — {scorer}</b>"


def decorate_name(name: str, marks: Dict[str, str], last_mentions: Dict[str, int], current_idx: int) -> str:
    clean = _clean_person_name(name)
    if not clean:
        return "—"
    if clean in marks and last_mentions.get(clean) == current_idx:
        return f"{clean}{marks[clean]}"
    return clean


def line_goal(ev: ScoringEvent, marks: Dict[str, str], last_mentions: Dict[str, int], idx_ref: List[int]) -> str:
    score = f"{ev.home_goals}:{ev.away_goals}"

    who = decorate_name(ev.scorer, marks, last_mentions, idx_ref[0])
    idx_ref[0] += 1

    assists_out: List[str] = []
    for a in _clean_assists(ev.assists):
        assists_out.append(decorate_name(a, marks, last_mentions, idx_ref[0]))
        idx_ref[0] += 1

    assists_text = ""
    if len(assists_out) == 1:
        assists_text = f" ({assists_out[0]})"
    elif len(assists_out) >= 2:
        assists_text = f" ({', '.join(assists_out)})"

    return f"{score} – {ev.time} {who}{assists_text}"


def get_winning_shootout_name(
    events: List[ScoringEvent],
    official_has_shootout: bool,
    sportsru_winner: Optional[SRUShootoutWinner],
) -> Optional[str]:
    if not official_has_shootout:
        return None

    for ev in events:
        if ev.period_type == "SHOOTOUT" and ev.is_shootout_winner and _is_valid_player_name(ev.scorer):
            return _clean_person_name(ev.scorer)

    if sportsru_winner and sportsru_winner.scorer_ru and _is_valid_player_name(sportsru_winner.scorer_ru):
        return _clean_person_name(sportsru_winner.scorer_ru)

    scored_attempts = [
        ev for ev in events
        if ev.period_type == "SHOOTOUT" and ev.is_shootout_scored and _is_valid_player_name(ev.scorer)
    ]
    if scored_attempts:
        return _clean_person_name(scored_attempts[-1].scorer)

    return None


def build_single_match_text(
    meta: GameMeta,
    standings: Dict[str, TeamRecord],
    events: List[ScoringEvent],
    official_has_shootout: bool,
    sportsru_winner: Optional[SRUShootoutWinner] = None,
) -> str:
    he = TEAM_EMOJI.get(meta.home_tri, "")
    ae = TEAM_EMOJI.get(meta.away_tri, "")
    hn = TEAM_RU.get(meta.home_tri, meta.home_tri)
    an = TEAM_RU.get(meta.away_tri, meta.away_tri)
    hrec = standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec = standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    hmark = str(meta.home_series_wins) if meta.home_series_wins is not None else hrec
    amark = str(meta.away_series_wins) if meta.away_series_wins is not None else arec

    winning_so_name = get_winning_shootout_name(events, official_has_shootout, sportsru_winner)

    head_lines = []
    if meta.series_game:
        head_lines.append(f"<i>Матч №{meta.series_game}</i>")
    head_lines.extend([
        f"{he} <b>«{hn}»: {meta.home_score}</b> ({hmark})",
        f"{ae} <b>«{an}»: {meta.away_score}</b> ({amark})",
    ])
    if winning_so_name:
        head_lines.append("")
        head_lines.append(f"<b>Победный буллит — {winning_so_name}</b>")

    regular_and_ot = [ev for ev in events if ev.period_type != "SHOOTOUT"]

    marks = compute_player_marks(events)
    last_mentions = find_last_mentions(regular_and_ot, winning_so_name)
    winning_ot_line = overtime_winner_line(meta, regular_and_ot)
    if winning_ot_line:
        head_lines.append("")
        head_lines.append(winning_ot_line)

    head = "\n".join(head_lines)

    groups: Dict[Tuple[int, str], List[ScoringEvent]] = {}
    for ev in regular_and_ot:
        groups.setdefault((ev.period, ev.period_type), []).append(ev)

    for pnum in (1, 2, 3):
        if (pnum, "REGULAR") not in groups:
            groups[(pnum, "REGULAR")] = []

    max_ot_period = max(
        [k[0] for k in groups if (k[1] or "").upper() == "OVERTIME"],
        default=3,
    )
    for pnum in range(4, max_ot_period + 1):
        groups.setdefault((pnum, "OVERTIME"), [])

    ot_keys = sorted([k for k in groups if (k[1] or "").upper() == "OVERTIME"], key=lambda x: x[0])
    ot_total = len(ot_keys)
    ot_order = {k: i + 1 for i, k in enumerate(ot_keys)}

    lines = [head]
    sort_key = lambda x: (x[0], 0 if (x[1] or "").upper() == "REGULAR" else 1)
    idx_ref = [0]

    for key in sorted(groups.keys(), key=sort_key):
        pnum, ptype = key
        ot_idx = ot_order.get(key)
        title = period_title_text(pnum, ptype, ot_idx, ot_total)
        lines.append("")
        lines.append(_italic(title))
        per = groups[key]
        if not per:
            lines.append("Голов не было")
        else:
            for ev in per:
                lines.append(line_goal(ev, marks, last_mentions, idx_ref))

    if winning_so_name:
        lines.append("")
        lines.append("Победный буллит")
        lines.append(f"{meta.home_score}:{meta.away_score} – {winning_so_name}")

    return "\n".join(lines).strip()


def load_state(path: str) -> Dict[str, Any]:
    p = pathlib.Path(path)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        return {"posted": {}}
    try:
        return json.loads(p.read_text("utf-8") or "{}") or {"posted": {}}
    except Exception:
        return {"posted": {}}


def save_state(path: str, data: Dict[str, Any]) -> None:
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")


def send_telegram_text(text: str) -> bool:
    token = _env_str("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = _env_str("TELEGRAM_CHAT_ID", DEFAULT_TELEGRAM_CHAT_ID).strip()
    thread = _env_str("TELEGRAM_THREAD_ID", "").strip()

    if not token or not chat_id:
        print("[ERR] Telegram token/chat_id not set")
        return False

    url = f"{TG_API}/bot{token}/sendMessage"
    headers = {"Content-Type": "application/json"}
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
        except Exception:
            pass

    if DRY_RUN:
        print("[DRY RUN] " + textwrap.shorten(text, 200, placeholder="…"))
        return False

    try:
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    except Exception as exc:
        print(f"[ERR] sendMessage failed: {exc}")
        return False

    try:
        data = resp.json()
    except Exception:
        data = {"ok": None, "raw": resp.text}

    dbg(f"TG HTTP={resp.status_code} JSON={data}")
    if resp.status_code != 200 or not data.get("ok", False):
        print(f"[ERR] sendMessage failed: {data.get('error_code')} {data.get('description')}")
        return False
    return True


def get_meta_by_gamepk_scan_schedule(gamePk: int) -> Optional[GameMeta]:
    raw = _list_games_for_dates(_iter_dates_around_today(3, 3))
    for g in raw:
        gid = _first_int(g.get("id"), g.get("gameId"), g.get("gamePk"))
        if gid == gamePk:
            return _game_to_meta(g)
    return None

def autopost_current_hockey_day() -> List[GameMeta]:
    base_day = _target_base_date()

    dates = [
        (base_day - timedelta(days=2)).isoformat(),
        (base_day - timedelta(days=1)).isoformat(),
        base_day.isoformat(),
        (base_day + timedelta(days=1)).isoformat(),
        (base_day + timedelta(days=2)).isoformat(),
    ]

    print("TARGET_DATE:", TARGET_DATE or "(empty)")
    print("Autopost base date:", base_day.isoformat())
    print("Autopost schedule dates:", dates)

    raw = _list_games_for_dates(dates)
    metas = [_game_to_meta(g) for g in raw]
    metas = [m for m in metas if m]

    print("ALL games raw:", [(m.gamePk, m.away_tri, m.home_tri, m.state) for m in metas])

    finals = [m for m in metas if _is_final_state(m.state)]

    seen = set()
    uniq: List[GameMeta] = []
    for m in sorted(finals, key=lambda x: x.gameDateUTC):
        if m.gamePk not in seen:
            seen.add(m.gamePk)
            uniq.append(m)

    return uniq


def _meta_hockey_day_pt(meta: GameMeta) -> date:
    return meta.gameDateUTC.astimezone(PT_TZ).date()


def latest_final_hockey_day() -> List[GameMeta]:
    base_day = _target_base_date()
    dates = [(base_day - timedelta(days=off)).isoformat() for off in range(0, 8)]

    print("TARGET_DATE:", TARGET_DATE or "(empty)")
    print("Latest final hockey day base date:", base_day.isoformat())
    print("Latest final hockey day scan dates:", dates)

    raw = _list_games_for_dates(dates)
    metas = [_game_to_meta(g) for g in raw]
    metas = [m for m in metas if m and _is_final_state(m.state)]

    seen = set()
    uniq: List[GameMeta] = []
    for m in sorted(metas, key=lambda x: x.gameDateUTC):
        if m.gamePk not in seen:
            seen.add(m.gamePk)
            uniq.append(m)

    by_day: Dict[date, List[GameMeta]] = {}
    for m in uniq:
        day = _meta_hockey_day_pt(m)
        if day <= base_day:
            by_day.setdefault(day, []).append(m)

    if not by_day:
        print("Latest final hockey day: no final games found")
        return []

    latest_day = max(by_day.keys())
    result = sorted(by_day[latest_day], key=lambda x: x.gameDateUTC)
    print("Latest final hockey day:", latest_day.isoformat())
    print("Latest final games:", [m.gamePk for m in result])
    return result


def pending_game_text(meta: GameMeta) -> str:
    matchup = f"{meta.away_tri} - {meta.home_tri}"
    if _is_not_started_state(meta.state):
        return f"{matchup} ещё не началась"
    if _is_liveish_state(meta.state):
        return f"{matchup} ещё не завершилась"
    return f"{matchup} статус: {meta.state}"

def main() -> None:
    game_pk = _env_str("GAME_PK", "").strip()
    game_query = _env_str("GAME_QUERY", "").strip()
    resend_last_day = _env_bool("RESEND_LAST_DAY", False)

    standings = fetch_standings_map()
    state = load_state(STATE_PATH)
    posted: Dict[str, bool] = state.get("posted", {}) or {}
    force_repost: Dict[str, bool] = state.get("force_repost", {}) or {}

    dbg("already posted:", sorted(posted.keys())[:20], "total=", len(posted))
    dbg("force repost:", sorted(force_repost.keys()))

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
        if resend_last_day:
            print("RESEND_LAST_DAY enabled: reposting the latest final hockey day")
            metas = latest_final_hockey_day()
        else:
            metas = autopost_current_hockey_day()
        print("FINAL games:", [m.gamePk for m in metas])
        print("FINAL games raw:", [(m.gamePk, m.away_tri, m.home_tri, m.state) for m in metas])
        if not resend_last_day:
            metas = [
                m for m in metas
                if force_repost.get(str(m.gamePk)) or not posted.get(str(m.gamePk))
            ]
        print("Need to post:", [m.gamePk for m in metas])

    new_posts = 0
    failed_posts = 0

    for meta in metas:
        if manual_mode and not _is_final_state(meta.state):
            text = pending_game_text(meta)
            dbg("Pending preview:\n" + text)
            if send_telegram_text(text):
                new_posts += 1
            else:
                failed_posts += 1
            continue

        evs, official_has_shootout = fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)
        sru_home, sru_away, sru_so_winner, _ = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        merged = merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)

        text = build_single_match_text(
            meta=meta,
            standings=standings,
            events=merged,
            official_has_shootout=official_has_shootout,
            sportsru_winner=sru_so_winner,
        )

        dbg("official_has_shootout:", official_has_shootout)
        dbg("sportsru_so_winner:", getattr(sru_so_winner, "scorer_ru", None))
        dbg("Single match preview:\n" + text[:900].replace("\n", "¶") + "…")
        sent_ok = send_telegram_text(text)
        if not sent_ok:
            failed_posts += 1
            print(f"[ERR] not marking posted because Telegram send failed: {meta.gamePk}")
            continue

        force_repost.pop(str(meta.gamePk), None)
        if not manual_mode and not resend_last_day:
            posted[str(meta.gamePk)] = True
            new_posts += 1
            dbg(f"mark posted {meta.gamePk}")
        else:
            new_posts += 1

    state["posted"] = posted
    state["force_repost"] = force_repost
    save_state(STATE_PATH, state)
    print(f"OK (posted {new_posts}, failed {failed_posts})")


if __name__ == "__main__":
    main()
