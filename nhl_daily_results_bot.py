#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Daily Results Bot — Official chronology + sports.ru scorers

Данные:
- Расписание FINAL:        https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
- Play-by-Play (официально): https://api-web.nhle.com/v1/gamecenter/{gamePk}/play-by-play
- Турнирная таблица:       https://api-web.nhle.com/v1/standings/now
- Авторы голов/ассисты:    https://www.sports.ru/hockey/match/{home-slug}-vs-{away-slug}/
    • ul.match-summary__goals-list--home
    • ul.match-summary__goals-list--away

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TELEGRAM_THREAD_ID
- DAYS_BACK=1, DAYS_FWD=0
- DRY_RUN=0/1
- DEBUG_VERBOSE=0/1  (подробный лог)
"""

from __future__ import annotations
import os, re, json, time, textwrap
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import requests

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # fallback на regex

TG_API     = "https://api.telegram.org"
NHLE_BASE  = "https://api-web.nhle.com/v1"
PBP_FMT    = NHLE_BASE + "/gamecenter/{gamePk}/play-by-play"

# ---------- ENV ----------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None: return default
    try: return int(str(v).strip())
    except: return default

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

DAYS_BACK = _env_int("DAYS_BACK", 1)
DAYS_FWD  = _env_int("DAYS_FWD", 0)
DRY_RUN   = _env_bool("DRY_RUN", False)
DEBUG_VERBOSE = _env_bool("DEBUG_VERBOSE", True)  # включил по умолчанию, можно выключить

# ---------- RU/Naming ----------
MONTHS_RU = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}

TEAM_RU = {
    "ANA":"Анахайм","ARI":"Аризона","BOS":"Бостон","BUF":"Баффало","CGY":"Калгари","CAR":"Каролина",
    "CHI":"Чикаго","COL":"Колорадо","CBJ":"Коламбус","DAL":"Даллас","DET":"Детройт","EDM":"Эдмонтон",
    "FLA":"Флорида","LAK":"Лос-Анджелес","MIN":"Миннесота","MTL":"Монреаль","NSH":"Нэшвилл",
    "NJD":"Нью-Джерси","NYI":"Айлендерс","NYR":"Рейнджерс","OTT":"Оттава","PHI":"Филадельфия",
    "PIT":"Питтсбург","SJS":"Сан-Хосе","SEA":"Сиэтл","STL":"Сент-Луис","TBL":"Тампа-Бэй",
    "TOR":"Торонто","VAN":"Ванкувер","VGK":"Вегас","WSH":"Вашингтон","WPG":"Виннипег",
}

TEAM_EMOJI = {
    "ANA":"🦆","ARI":"🦂","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️","CBJ":"💣",
    "DAL":"⭐️","DET":"🛡️","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦","NSH":"🐯",
    "NJD":"😈","NYI":"🏝️","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈","SEA":"🦑","STL":"🎵",
    "TBL":"⚡","TOR":"🍁","VAN":"🐳","VGK":"🎰","WSH":"🦅","WPG":"✈️",
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
    "WPG":"winnipeg-jets",
}

# ---------- HTTP ----------
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.8",
}

def _get_with_retries(url: str, timeout: int = 30, tries: int = 3, backoff: float = 0.75, as_text: bool = False):
    last = None
    for attempt in range(1, tries+1):
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
                sleep_s = backoff * (2 ** (attempt-1))
                print(f"[DBG] retry {attempt}/{tries} for {url} after {sleep_s:.2f}s: {repr(e)}")
                time.sleep(sleep_s)
            else:
                raise
    raise last

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, tries=3, backoff=0.75, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, tries=3, backoff=0.75, as_text=True)

# ---------- СТРУКТУРЫ ----------
@dataclass
class TeamRecord:
    wins: int
    losses: int
    ot: int
    points: int
    def as_str(self) -> str:
        return f"{self.wins}-{self.losses}-{self.ot}, {self.points} о."

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
    time: str         # "MM.SS"
    team_for: str     # triCode
    home_goals: int
    away_goals: int
    scorer: str       # EN (official)
    assists: List[str] = field(default_factory=list)

@dataclass
class SRUGoal:
    time: Optional[str]
    scorer_ru: Optional[str]
    assists_ru: List[str]

# ---------- helpers ----------
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

def _extract_name(obj_or_str: Any) -> Optional[str]:
    if not obj_or_str:
        return None
    if isinstance(obj_or_str, str):
        return obj_or_str.strip() or None
    if isinstance(obj_or_str, dict):
        for k in ("name","default","fullName","firstLastName","lastFirstName"):
            v = obj_or_str.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None

# ---------- STANDINGS ----------
def fetch_standings_map() -> Dict[str, TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"
    data = http_get_json(url)
    teams: Dict[str, TeamRecord] = {}
    nodes = []
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
        wins   = _first_int(rec.get("wins"),   r.get("wins"),   rec.get("gamesPlayedWins"))
        losses = _first_int(rec.get("losses"), r.get("losses"), rec.get("gamesPlayedLosses"), rec.get("regulationLosses"), r.get("regulationLosses"))
        ot     = _first_int(rec.get("ot"),     r.get("ot"),     rec.get("otLosses"), r.get("otLosses"), rec.get("overtimeLosses"), r.get("overtimeLosses"))
        pts    = _first_int(r.get("points"), rec.get("points"), r.get("pts"), rec.get("teamPoints"))

        if abbr:
            teams[abbr] = TeamRecord(wins, losses, ot, pts)
    print(f"[DBG] standings map built: {len(teams)}")
    return teams

# ---------- SCHEDULE ----------
def list_final_games_window(days_back: int = 1, days_fwd: int = 0) -> List[GameMeta]:
    now_utc = datetime.now(timezone.utc)
    dates = [(now_utc - timedelta(days=d)).date().isoformat()
             for d in range(days_back, -days_fwd-1, -1)]

    metas: Dict[int, GameMeta] = {}
    for day in dates:
        url = f"{NHLE_BASE}/schedule/{day}"
        print(f"[DBG] GET {url}")
        s = http_get_json(url)
        for w in s.get("gameWeek", []) or []:
            for g in w.get("games", []) or []:
                state = _upper_str(g.get("gameState") or g.get("gameStatus"))
                if state not in ("FINAL","OFF"):
                    continue
                gid = _first_int(g.get("id"), g.get("gameId"), g.get("gamePk"))
                if gid == 0: 
                    continue
                gd = g.get("startTimeUTC") or g.get("gameDate") or ""
                try:
                    gdt = datetime.fromisoformat(str(gd).replace("Z","+00:00"))
                except Exception:
                    gdt = now_utc
                home = g.get("homeTeam", {}) or {}
                away = g.get("awayTeam", {}) or {}
                htri = _upper_str(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
                atri = _upper_str(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
                hscore = _first_int(home.get("score"))
                ascore = _first_int(away.get("score"))
                metas[gid] = GameMeta(
                    gamePk=gid, gameDateUTC=gdt, state=state,
                    home_tri=htri, away_tri=atri,
                    home_score=hscore, away_score=ascore
                )
    games = sorted(metas.values(), key=lambda m: m.gameDateUTC)
    print(f"[DBG] Collected FINAL games: {len(games)}")
    return games

# ---------- OFFICIAL PLAY-BY-PLAY ----------
def _normalize_period_type(t: str) -> str:
    t = _upper_str(t)
    if t == "REG": return "REGULAR"
    if t == "OT":  return "OVERTIME"
    if t == "SO":  return "SHOOTOUT"
    return t or "REGULAR"

def fetch_scoring_official(gamePk: int, home_tri: str, away_tri: str) -> List[ScoringEvent]:
    """
    Берём события 'goal' из gamecenter/{gamePk}/play-by-play.
    Команду-автора определяем надёжно по изменению счёта.
    """
    url = PBP_FMT.format(gamePk=gamePk)
    data = http_get_json(url)
    plays = data.get("plays", []) or []
    events: List[ScoringEvent] = []

    prev_h = 0
    prev_a = 0

    for p in plays:
        if _upper_str(p.get("typeDescKey")) != "GOAL":
            continue

        pd = p.get("periodDescriptor", {}) or {}
        period = _first_int(pd.get("number"))
        ptype  = _normalize_period_type(pd.get("periodType") or "REG")

        t = str(p.get("timeInPeriod") or "00:00").replace(":", ".")
        det = p.get("details", {}) or {}

        # Счёт после гола
        h_goals = det.get("homeScore")
        a_goals = det.get("awayScore")
        if isinstance(h_goals, int) and isinstance(a_goals, int):
            h, a = h_goals, a_goals
        else:
            score_obj = p.get("score", {}) or {}
            if isinstance(score_obj.get("home"), int) and isinstance(score_obj.get("away"), int):
                h, a = score_obj["home"], score_obj["away"]
            else:
                # Если даже здесь нет — используем прошлые (редко бывает)
                h, a = prev_h, prev_a

        # Кто забил — надёжно по дельте счёта:
        team_by_delta = None
        if h > prev_h:
            team_by_delta = home_tri
        elif a > prev_a:
            team_by_delta = away_tri

        # Доп. фолбэки из полей события
        team_fallback = _upper_str(
            det.get("eventOwnerTeamAbbrev") or
            p.get("teamAbbrev") or
            det.get("teamAbbrev") or
            det.get("scoringTeamAbbrev")
        )
        team = team_by_delta or team_fallback

        # Автор и ассисты — перебором известных ключей
        def _name_chain(*keys) -> Optional[str]:
            for k in keys:
                v = det.get(k)
                name = _extract_name(v)
                if name: 
                    return name
            # иногда имя прямо на верхнем уровне
            for k in ("scoringPlayerName","scorerName","shootingPlayerName"):
                v = p.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return None

        scorer = _name_chain(
            "scoringPlayerName", "scorerName", "shootingPlayerName", "scoringPlayer"
        ) or ""

        assists: List[str] = []
        for k in ("assist1PlayerName","assist2PlayerName","assist3PlayerName","assist1","assist2","assist3"):
            v = det.get(k)
            name = _extract_name(v)
            if name: assists.append(name)

        events.append(ScoringEvent(period, ptype, t, team, h, a, scorer, assists))

        if DEBUG_VERBOSE:
            print(f"[DBG] PBP ev: P{period} {t} team={team or '?'} score={h}:{a} "
                  f"scorer={'?' if not scorer else scorer} a={len(assists)}")

        prev_h, prev_a = h, a

    print(f"[DBG] PBP goals parsed: {len(events)} for game {gamePk}")
    return events

# ---------- SPORTSRU PARSER ----------
TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")

def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or "")
    if not m: return None
    return f"{int(m.group(1)):02d}.{m.group(2)}"

def parse_sportsru_goals_html(html: str, side: str) -> List[SRUGoal]:
    results: List[SRUGoal] = []

    if BeautifulSoup:
        soup = BeautifulSoup(html, "lxml" if "lxml" in globals() else "html.parser")
        ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or \
             soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors = [a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru = anchors[0] if anchors else None
                assists_ru = anchors[1:] if len(anchors) > 1 else []
                raw_text = li.get_text(" ", strip=True)
                time_ru = _extract_time(raw_text)
                results.append(SRUGoal(time_ru, scorer_ru, assists_ru))
    else:
        # --- Regex fallback ---
        ul_pat = re.compile(
            r'<ul[^>]*class="[^"]*match-summary__goals-list[^"]*--%s[^"]*"[^>]*>(.*?)</ul>' % side,
            re.S | re.I
        )
        li_pat = re.compile(r"<li\b[^>]*>(.*?)</li>", re.S|re.I)
        a_pat  = re.compile(r"<a\b[^>]*>(.*?)</a>", re.S|re.I)

        ul_m = ul_pat.search(html)
        if not ul_m: return results
        ul_html = ul_m.group(1)

        for li_html in li_pat.findall(ul_html):
            text = re.sub(r"<[^>]+>", " ", li_html)
            time_ru = _extract_time(text)
            names = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", m)).strip()
                     for m in a_pat.findall(li_html)]
            scorer_ru = names[0] if names else None
            assists_ru = names[1:] if len(names) > 1 else []
            results.append(SRUGoal(time_ru, scorer_ru, assists_ru))

    if DEBUG_VERBOSE and results:
        sample = ", ".join([(g.scorer_ru or "?") + (f" @{g.time}" if g.time else "") for g in results[:3]])
        print(f"[DBG] sports.ru {side} sample: {sample} (+{max(0,len(results)-3)} more)")
    return results

def fetch_sportsru_goals(home_tri: str, away_tri: str) -> Tuple[List[SRUGoal], List[SRUGoal], str]:
    home_slug = SPORTSRU_SLUG.get(home_tri)
    away_slug = SPORTSRU_SLUG.get(away_tri)
    if not home_slug or not away_slug:
        print(f"[DBG] sports.ru slug missing for {home_tri}/{away_tri}")
        return [], [], ""

    tried = []
    for order in [(home_slug, away_slug), (away_slug, home_slug)]:
        url = f"https://www.sports.ru/hockey/match/{order[0]}-vs-{order[1]}/"
        tried.append(url)
        try:
            html = http_get_text(url, timeout=20)
        except Exception as e:
            print(f"[DBG] sports.ru fetch fail {url}: {repr(e)}")
            continue

        home_side = "home" if order[0] == home_slug else "away"
        away_side = "away" if home_side == "home" else "home"
        home_goals = parse_sportsru_goals_html(html, home_side)
        away_goals = parse_sportsru_goals_html(html, away_side)

        if home_goals or away_goals:
            print(f"[DBG] sports.ru goals ok for {url}: home={len(home_goals)} away={len(away_goals)}")
            return home_goals, away_goals, url

    print(f"[DBG] sports.ru goals not found. Tried: {tried}")
    return [], [], ""

# ---------- MERGE ----------
def merge_official_with_sportsru(
    evs: List[ScoringEvent],
    sru_home: List[SRUGoal],
    sru_away: List[SRUGoal],
    home_tri: str,
    away_tri: str,
) -> List[ScoringEvent]:
    h_i = 0
    a_i = 0
    out: List[ScoringEvent] = []
    for idx, ev in enumerate(evs, start=1):
        if ev.team_for == home_tri and h_i < len(sru_home):
            g = sru_home[h_i]; h_i += 1
            ev.scorer  = g.scorer_ru or ev.scorer or ""
            ev.assists = g.assists_ru or ev.assists
        elif ev.team_for == away_tri and a_i < len(sru_away):
            g = sru_away[a_i]; a_i += 1
            ev.scorer  = g.scorer_ru or ev.scorer or ""
            ev.assists = g.assists_ru or ev.assists
        else:
            if DEBUG_VERBOSE and ev.team_for not in (home_tri, away_tri):
                print(f"[DBG] merge-skip idx={idx}: team_for='{ev.team_for}' "
                      f"not in {{{home_tri}/{away_tri}}}")
        out.append(ev)
    if DEBUG_VERBOSE:
        print(f"[DBG] used sports.ru: home_used={h_i}/{len(sru_home)} away_used={a_i}/{len(sru_away)}")
    return out

# ---------- FORMAT ----------
def period_title(num: int, ptype: str, ot_counter: Dict[str,int]) -> str:
    t = (ptype or "").upper()
    if t == "REGULAR":
        return f"{num}-й период"
    if t == "OVERTIME":
        ot_counter["n"] = ot_counter.get("n",0) + 1
        return f"Овертайм №{ot_counter['n']}"
    if t == "SHOOTOUT":
        return "Серия буллитов"
    return f"Период {num}"

def line_goal(ev: ScoringEvent) -> str:
    score = f"{ev.home_goals}:{ev.away_goals}"
    assist_txt = f" ({', '.join(ev.assists)})" if ev.assists else ""
    who = ev.scorer or "—"
    return f"{score} – {ev.time} {who}{assist_txt}"

def build_match_block(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he = TEAM_EMOJI.get(meta.home_tri, "")
    ae = TEAM_EMOJI.get(meta.away_tri, "")
    hn = TEAM_RU.get(meta.home_tri, meta.home_tri)
    an = TEAM_RU.get(meta.away_tri, meta.away_tri)
    hrec = standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec = standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head = f"{he} «{hn}»: {meta.home_score} ({hrec})\n{ae} «{an}»: {meta.away_score} ({arec})"

    groups: Dict[Tuple[int,str], List[ScoringEvent]] = {}
    for ev in events:
        groups.setdefault((ev.period, ev.period_type), []).append(ev)

    # гарантируем 1..3 периоды
    for pnum in (1,2,3):
        if (pnum, "REGULAR") not in groups:
            groups[(pnum, "REGULAR")] = []

    body: List[str] = []
    otc = {"n":0}
    sort_key = lambda x: (x[0], 0 if (x[1] or "").upper()=="REGULAR" else 1 if (x[1] or "").upper()=="OVERTIME" else 2)

    for key in sorted(groups.keys(), key=sort_key):
        period_header = period_title(key[0], key[1], otc)
        body.append("\n" + period_header)
        period_events = groups[key]
        if not period_events:
            body.append("Голов не было")  # <- без курсива
        else:
            for ev in period_events:
                body.append(line_goal(ev))

    return head + "\n\n" + "\n".join(body).strip()

def chunk_text(s: str, hard_limit: int = 3800, soft_sep: str = "——————————————————\n") -> List[str]:
    s = s.strip()
    if len(s) <= hard_limit: return [s]
    parts=[]; cur=""; blocks=s.split(soft_sep)
    for i,b in enumerate(blocks):
        piece = (b if i==0 else soft_sep+b).rstrip()
        if not cur:
            if len(piece)<=hard_limit: cur=piece
            else:
                for line in piece.splitlines(True):
                    if len(cur)+len(line)>hard_limit and cur:
                        parts.append(cur.rstrip()); cur=""
                    cur+=line
                if cur: parts.append(cur.rstrip()); cur=""
        else:
            if len(cur)+len(piece)<=hard_limit: cur+=piece
            else:
                parts.append(cur.rstrip()); cur=b.strip()
                if len(cur)>hard_limit:
                    tmp=""
                    for line in (soft_sep+cur).splitlines(True):
                        if len(tmp)+len(line)>hard_limit and tmp:
                            parts.append(tmp.rstrip()); tmp=""
                        tmp+=line
                    if tmp: parts.append(tmp.rstrip()); tmp=""
                    cur=""
    if cur: parts.append(cur.rstrip())
    if len(parts)>1:
        total=len(parts); head=parts[0]
        parts = [head] + [f"…продолжение (часть {i}/{total})\n\n{p}" for i,p in enumerate(parts[1:], start=2)]
    return parts

# ---------- TELEGRAM ----------
def send_telegram_text(text: str) -> None:
    token   = _env_str("TELEGRAM_BOT_TOKEN","").strip()
    chat_id = _env_str("TELEGRAM_CHAT_ID","").strip()
    thread  = _env_str("TELEGRAM_THREAD_ID","").strip()
    if not token or not chat_id:
        print("[ERR] Telegram token/chat_id not set"); return
    url = f"{TG_API}/bot{token}/sendMessage"
    headers = {"Content-Type": "application/json"}
    parts = chunk_text(text, 3800, "——————————————————\n")
    print(f"[DBG] Telegram parts: {len(parts)}")
    for idx, part in enumerate(parts, start=1):
        payload = {
            "chat_id": int(chat_id) if chat_id.strip("-").isdigit() else chat_id,
            "text": part,
            "disable_web_page_preview": True,
            "disable_notification": False,
        }
        if thread:
            try: payload["message_thread_id"] = int(thread)
            except: pass
        if DRY_RUN:
            print("[DRY RUN] " + textwrap.shorten(part, width=200, placeholder="…"))
            continue
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        try: data = resp.json()
        except: data = {"ok":None,"raw":resp.text}
        print(f"[DBG] TG HTTP={resp.status_code} JSON={data}")
        if resp.status_code!=200 or not data.get("ok",False):
            print(f"[ERR] sendMessage failed: {data.get('error_code')} {data.get('description')}")

# ---------- MAIN ----------
def header_ru(n_games: int) -> str:
    now = datetime.now()
    return f"🗓 Регулярный чемпионат НХЛ • {now.day} {MONTHS_RU[now.month]} • {n_games} матчей"

def make_post_text(games: List[GameMeta], standings: Dict[str,TeamRecord]) -> str:
    blocks: List[str] = [header_ru(len(games)), "", "Результаты надёжно спрятаны 👇", ""]
    for meta in games:
        evs = fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)
        sru_home, sru_away, url = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        if url:
            print(f"[DBG] sports.ru page used: {url}")
        else:
            print(f"[DBG] sports.ru authors not available; keep official names")
        merged = merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)
        block = build_match_block(meta, standings, merged)
        blocks.append(block)
    return "\n\n——————————————————\n".join(blocks).strip()

def main():
    print(f"[DBG] Window back={DAYS_BACK} fwd={DAYS_FWD}")
    games = list_final_games_window(DAYS_BACK, DAYS_FWD)
    if not games:
        print("OK (нет FINAL игр в окне)")
        return
    standings = fetch_standings_map()
    text = make_post_text(games, standings)
    print("[DBG] Preview 500:\n" + text[:500].replace("\n","¶") + "…")
    send_telegram_text(text)
    print("OK")

if __name__ == "__main__":
    main()
