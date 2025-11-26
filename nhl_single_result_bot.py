#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — manual game support (RU names + SO winner)

— Ручной режим:
   * GAME_PK=2025020xxx  → постим ровно этот матч.
   * или GAME_DATE=YYYY-MM-DD и MATCH="NYI - SEA"/"SEA@NYI"/"SEA NYI"
     → резолвим pk в окне SEARCH_BACK/SEARCH_FWD дней и постим.
— Если ручные переменные не заданы, режим «вчера+сегодня (UTC)» постит непостенные FINAL.
— «Победный буллит»: отдельным блоком:
      Победный буллит
      итоговый счёт – Имя
— Русские имена: подхватываем из sports.ru, иначе — из официального PBP.

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TELEGRAM_THREAD_ID
- STATE_PATH="state/posted_games.json"
- DRY_RUN=0/1, DEBUG_VERBOSE=0/1
- GAME_PK (опц.), GAME_DATE (опц.), MATCH (опц.), FORCE_POST=0/1 (опц.)
- SEARCH_BACK=1, SEARCH_FWD=1  — сколько дней вокруг GAME_DATE смотреть
- REQUIRE_FINAL=1               — требовать ли FINAL/OFF при ручном выборе
"""

from __future__ import annotations
import os, re, json, time, textwrap, pathlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import requests

try:
    from bs4 import BeautifulSoup as BS  # type: ignore
    HAS_BS = True
except Exception:
    HAS_BS = False

TG_API     = "https://api.telegram.org"
NHLE_BASE  = "https://api-web.nhle.com/v1"
PBP_FMT    = NHLE_BASE + "/gamecenter/{gamePk}/play-by-play"

# ---------------- ENV ----------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default
def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")
def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None: return default
    try: return int(str(v).strip())
    except: return default

DRY_RUN       = _env_bool("DRY_RUN", False)
DEBUG_VERBOSE = _env_bool("DEBUG_VERBOSE", False)
STATE_PATH    = _env_str("STATE_PATH", "state/posted_games.json")
FORCE_POST    = _env_bool("FORCE_POST", False)

GAME_PK_ENV   = _env_str("GAME_PK", "").strip()
GAME_DATE_ENV = _env_str("GAME_DATE", "").strip()  # YYYY-MM-DD
MATCH_ENV     = _env_str("MATCH", "").strip()      # e.g. "SEA - NYI" / "SEA@NYI"

SEARCH_BACK   = _env_int("SEARCH_BACK", 1)
SEARCH_FWD    = _env_int("SEARCH_FWD", 1)
REQUIRE_FINAL = _env_bool("REQUIRE_FINAL", True)

# ---------------- RU / Dictionaries ----------------
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
# синонимы кодов из реального ввода
TRI_ALIASES = {
    "NAS":"NSH", "TAM":"TBL", "PHX":"ARI", "LAK":"LAK", "NJD":"NJD", "NYR":"NYR", "NYI":"NYI",
    "MON":"MTL", "WIN":"WPG", "TB":"TBL"
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

UA_HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language":"ru,en;q=0.8",
}
def _get_with_retries(url: str, timeout: int = 30, tries: int = 3, backoff: float = 0.75, as_text: bool = False):
    last=None
    for attempt in range(1, tries+1):
        try:
            r=requests.get(url, headers=UA_HEADERS, timeout=timeout); r.raise_for_status()
            if as_text:
                r.encoding = r.apparent_encoding or "utf-8"
                return r.text
            return r.json()
        except Exception as e:
            last=e
            if attempt<tries:
                sleep_s=backoff*(2**(attempt-1))
                print(f"[DBG] retry {attempt}/{tries} for {url} after {sleep_s:.2f}s: {repr(e)}")
                time.sleep(sleep_s)
            else:
                raise
    raise last
def http_get_json(url: str, timeout: int = 30) -> Any: return _get_with_retries(url, timeout=timeout, as_text=False)
def http_get_text(url: str, timeout: int = 30) -> str: return _get_with_retries(url, timeout=timeout, as_text=True)

# ---------------- Models ----------------
@dataclass
class TeamRecord:
    wins:int; losses:int; ot:int; points:int
    def as_str(self)->str: return f"{self.wins}-{self.losses}-{self.ot}"
@dataclass
class GameMeta:
    gamePk:int; gameDateUTC:datetime; state:str; home_tri:str; away_tri:str; home_score:int; away_score:int
@dataclass
class ScoringEvent:
    period:int; period_type:str; time:str; team_for:str; home_goals:int; away_goals:int; scorer:str; assists:List[str]=field(default_factory=list)
@dataclass
class SRUGoal:
    time:Optional[str]; scorer_ru:Optional[str]; assists_ru:List[str]

# ---------------- Helpers ----------------
def _upper_str(x: Any)->str:
    try: return str(x or "").upper()
    except: return ""
def _first_int(*vals)->int:
    for v in vals:
        if v is None: continue
        try:
            s=str(v).strip()
            if s=="": continue
            return int(float(s))
        except: continue
    return 0
def _extract_name(obj_or_str: Any)->Optional[str]:
    if not obj_or_str: return None
    if isinstance(obj_or_str,str): return obj_or_str.strip() or None
    if isinstance(obj_or_str,dict):
        for k in ("name","default","fullName","firstLastName","lastFirstName","shortName"):
            v=obj_or_str.get(k)
            if isinstance(v,str) and v.strip(): return v.strip()
    return None

def _clean_person(name: str) -> str:
    n = (name or "").strip()
    n = re.sub(r"^[\(\s]+", "", n)
    n = re.sub(r"[\)\s]+$", "", n)
    return n

def _norm_tri(code: str) -> str:
    c = code.upper()
    return TRI_ALIASES.get(c, c)

# ---------------- Standings / schedule ----------------
def fetch_standings_map()->Dict[str,TeamRecord]:
    url=f"{NHLE_BASE}/standings/now"; data=http_get_json(url); teams:Dict[str,TeamRecord]={}; nodes=[]
    if isinstance(data,dict):
        if isinstance(data.get("standings"),list): nodes=data["standings"]
        elif isinstance(data.get("records"),list): nodes=data["records"]
        elif isinstance(data.get("standings"),dict): nodes=data["standings"].get("overallRecords",[]) or []
    elif isinstance(data,list): nodes=data
    for r in nodes:
        abbr=""; ta=r.get("teamAbbrev")
        if isinstance(ta,str): abbr=ta.upper()
        elif isinstance(ta,dict): abbr=_upper_str(ta.get("default") or ta.get("tricode"))
        if not abbr: abbr=_upper_str(r.get("teamAbbrevTricode") or r.get("teamTriCode") or r.get("team"))
        rec=r.get("record") or r.get("overallRecord") or r.get("overallRecords") or {}
        wins=_first_int(rec.get("wins"),r.get("wins"),rec.get("gamesPlayedWins"))
        losses=_first_int(rec.get("losses"),r.get("losses"),rec.get("gamesPlayedLosses"),rec.get("regulationLosses"),r.get("regulationLosses"))
        ot=_first_int(rec.get("ot"),r.get("ot"),rec.get("otLosses"),r.get("otLosses"),rec.get("overtimeLosses"),r.get("overtimeLosses"))
        pts=_first_int(r.get("points"),rec.get("points"),r.get("pts"),r.get("teamPoints"))
        if abbr: teams[abbr]=TeamRecord(wins,losses,ot,pts)
    return teams

def list_games_for_dates(date_list: List[str], final_only: bool)->List[GameMeta]:
    now_utc=datetime.now(timezone.utc)
    metas:Dict[int,GameMeta]={}
    for day in date_list:
        url=f"{NHLE_BASE}/schedule/{day}"; s=http_get_json(url)
        for w in s.get("gameWeek",[]) or []:
            for g in w.get("games",[]) or []:
                state=_upper_str(g.get("gameState") or g.get("gameStatus"))
                if final_only and state not in ("FINAL","OFF"): continue
                gid=_first_int(g.get("id"),g.get("gameId"),g.get("gamePk"))
                if gid==0: continue
                gd=g.get("startTimeUTC") or g.get("gameDate") or ""
                try: gdt=datetime.fromisoformat(str(gd).replace("Z","+00:00"))
                except: gdt=now_utc
                home=g.get("homeTeam",{}) or {}; away=g.get("awayTeam",{}) or {}
                htri=_upper_str(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
                atri=_upper_str(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
                hscore=_first_int(home.get("score")); ascore=_first_int(away.get("score"))
                metas[gid]=GameMeta(gid,gdt,state,htri,atri,hscore,ascore)
    return sorted(metas.values(), key=lambda m:m.gameDateUTC)

def list_games_yesterday_today_final()->List[GameMeta]:
    now_utc=datetime.now(timezone.utc)
    dates=[(now_utc - timedelta(days=1)).date().isoformat(), now_utc.date().isoformat()]
    return list_games_for_dates(dates, final_only=True)

def _parse_match_codes(match_str: str) -> Optional[Tuple[str,str]]:
    # допускаем "SEA - NYI", "SEA@NYI", "SEA NYI", смешанные тире/пробелы/эм-дэши
    s = match_str.upper().replace("—", "-").replace("–", "-").replace("@", " ").replace("-", " ")
    codes = re.findall(r"\b[A-Z]{3}\b", s)
    if len(codes) < 2: return None
    return (_norm_tri(codes[0]), _norm_tri(codes[1]))

def resolve_game_pk_from_match(game_date: str, match_str: str, back:int, fwd:int, require_final:bool)->Optional[int]:
    parsed = _parse_match_codes(match_str)
    if not parsed: return None
    a, b = parsed
    # соберём окно дат: GAME_DATE±N
    try:
        base = datetime.fromisoformat(game_date).date()
    except Exception:
        return None
    dates = [(base + timedelta(days=delta)).isoformat() for delta in range(-abs(back), abs(fwd)+1)]
    games = list_games_for_dates(dates, final_only=False)
    # сначала строгий поиск: совпадает пара {a,b} и статус при необходимости FINAL
    for g in games:
        if {a,b} == {g.home_tri, g.away_tri} and (not require_final or g.state in ("FINAL","OFF")):
            return g.gamePk
    # если не нашли под FINAL — возьмём ближайший по времени матч из пары
    if not require_final:
        candidates = [g for g in games if {a,b} == {g.home_tri, g.away_tri}]
        if candidates:
            candidates.sort(key=lambda x: abs((x.gameDateUTC - datetime.combine(base, datetime.min.time(), tzinfo=timezone.utc)).total_seconds()))
            return candidates[0].gamePk
    # отладка
    if DEBUG_VERBOSE:
        pairs = [f"{g.away_tri}@{g.home_tri} {g.state} {g.gamePk} {g.gameDateUTC.isoformat()}" for g in games]
        print("[DBG] candidates in window:\n  " + "\n  ".join(pairs))
    return None

def fetch_meta_from_schedule(game_date: str, gid: int) -> Optional[GameMeta]:
    arr = list_games_for_dates([game_date], final_only=False)
    for g in arr:
        if g.gamePk == gid:
            return g
    return None

def fetch_meta_fallback(gid:int) -> Optional[GameMeta]:
    # api-web gamecenter (несколько файлов)
    for path in ("game-summary", "boxscore", "landing"):
        url = f"{NHLE_BASE}/gamecenter/{gid}/{path}"
        try:
            js=http_get_json(url)
        except Exception:
            continue
        def pick(obj, *keys):
            for k in keys:
                v = obj.get(k) if isinstance(obj, dict) else None
                if v: return v
            return None
        home_tri = None; away_tri=None; home_score=None; away_score=None
        for key in ("homeTeam","home","home_team"):
            t = js.get(key) if isinstance(js, dict) else None
            if isinstance(t, dict):
                home_tri = home_tri or _upper_str(pick(t,"abbrev","triCode","teamAbbrev"))
                home_score = home_score if home_score is not None else _first_int(pick(t,"score","goals"))
        for key in ("awayTeam","away","away_team"):
            t = js.get(key) if isinstance(js, dict) else None
            if isinstance(t, dict):
                away_tri = away_tri or _upper_str(pick(t,"abbrev","triCode","teamAbbrev"))
                away_score = away_score if away_score is not None else _first_int(pick(t,"score","goals"))
        teams = js.get("teams") if isinstance(js, dict) else None
        if isinstance(teams, dict):
            h = teams.get("home",{}) or {}; a = teams.get("away",{}) or {}
            home_tri = home_tri or _upper_str(pick(h,"abbrev","triCode","teamAbbrev"))
            away_tri = away_tri or _upper_str(pick(a,"abbrev","triCode","teamAbbrev"))
            home_score = home_score if home_score is not None else _first_int(pick(h,"score","goals"))
            away_score = away_score if away_score is not None else _first_int(pick(a,"score","goals"))
        if home_tri and away_tri and (home_score is not None) and (away_score is not None):
            return GameMeta(gid, datetime.now(timezone.utc), "FINAL", home_tri, away_tri, int(home_score), int(away_score))
    # statsapi fallback (может иногда не резолвиться DNS — не критично)
    try:
        js=http_get_json(f"https://statsapi.web.nhl.com/api/v1/game/{gid}/feed/live")
        game = js.get("gameData",{}) or {}
        teams = game.get("teams",{}) or {}
        home = teams.get("home",{}) or {}; away = teams.get("away",{}) or {}
        home_tri=_upper_str(home.get("abbreviation") or home.get("triCode"))
        away_tri=_upper_str(away.get("abbreviation") or away.get("triCode"))
        lines=js.get("liveData",{}).get("linescore",{}) or {}
        home_score=_first_int(lines.get("teams",{}).get("home",{}).get("goals"), lines.get("homeGoals"))
        away_score=_first_int(lines.get("teams",{}).get("away",{}).get("goals"), lines.get("awayGoals"))
        state=_upper_str(js.get("gameData",{}).get("status",{}).get("abstractGameState") or "FINAL")
        return GameMeta(gid, datetime.now(timezone.utc), state, home_tri, away_tri, home_score, away_score)
    except Exception as e:
        print(f"[DBG] statsapi fallback failed: {repr(e)}")
        return None

# ---------------- PBP / sports.ru ----------------
_SO_TYPES_GOAL = {"GOAL","SHOT"}
_ASSIST_KEYS = (
    "assist1PlayerName","assist2PlayerName","assist3PlayerName",
    "assist1","assist2","assist3",
    "primaryAssist","secondaryAssist","tertiaryAssist",
)
_SCORER_KEYS = (
    "scoringPlayerName","scorerName","shootingPlayerName","scoringPlayer",
    "goalScorer","primaryScorer","playerName","player",
    "shooterName","shootoutShooterName","shooter",
    "by","goalBy","scoredBy",
)

def _normalize_period_type(t: str) -> str:
    t=_upper_str(t)
    if t in ("","REG"): return "REGULAR"
    if t=="OT": return "OVERTIME"
    if t=="SO": return "SHOOTOUT"
    return t

def _is_shootout_goal(type_key: str, details: dict, period_type: str) -> bool:
    if period_type != "SHOOTOUT": return False
    if type_key not in _SO_TYPES_GOAL: return False
    for k in ("wasGoal","shotWasGoal","isGoal","isScored","scored"):
        v = details.get(k)
        if isinstance(v, bool) and v: return True
        if isinstance(v, str) and v.strip().lower() in ("1","true","yes"): return True
    return type_key == "GOAL"

def fetch_scoring_official(gamePk:int, home_tri:str, away_tri:str)->List[ScoringEvent]:
    data=http_get_json(PBP_FMT.format(gamePk=gamePk)); plays=data.get("plays",[]) or []
    events:List[ScoringEvent]=[]; prev_h=prev_a=0
    for p in plays:
        type_key=_upper_str(p.get("typeDescKey"))
        pd=p.get("periodDescriptor",{}) or {}
        period=_first_int(pd.get("number")); ptype=_normalize_period_type(pd.get("periodType") or "REG")
        det=p.get("details",{}) or {}
        t=str(p.get("timeInPeriod") or "00:00").replace(":",".")
        is_goal = (type_key=="GOAL") or _is_shootout_goal(type_key, det, ptype)
        if not is_goal: continue

        h=det.get("homeScore"); a=det.get("awayScore")
        if not (isinstance(h,int) and isinstance(a,int)):
            sc=p.get("score",{}) or {}
            if isinstance(sc.get("home"),int) and isinstance(sc.get("away"),int): h,a=sc["home"],sc["away"]
            else: h,a=prev_h,prev_a
        team=home_tri if h>prev_h else (away_tri if a>prev_a else _upper_str(det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")))

        scorer=""
        for k in _SCORER_KEYS:
            nm=_extract_name(det.get(k))
            if nm: scorer=_clean_person(nm); break
        if not scorer:
            for k in ("scoringPlayerName","scorerName","shootingPlayerName","by"):
                v=p.get(k)
                if isinstance(v,str) and v.strip(): scorer=_clean_person(v.strip()); break

        assists=[]
        for k in _ASSIST_KEYS:
            nm=_extract_name(det.get(k))
            if nm: assists.append(_clean_person(nm))

        events.append(ScoringEvent(period,ptype,t,team,h,a,scorer,assists))
        if ptype!="SHOOTOUT":
            prev_h,prev_a=h,a
    return events

TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")
def _extract_time(text: str)->Optional[str]:
    m=TIME_RE.search(text or ""); 
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None

def parse_sportsru_goals_html(html: str, side: str)->List[SRUGoal]:
    res: List[SRUGoal] = []
    if HAS_BS:
        soup=BS(html,"lxml" if "lxml" in globals() else "html.parser")
        ul=soup.select_one(f"ul.match-summary__goals-list--{side}") or soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors=[a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru=_clean_person(anchors[0]) if anchors else None
                assists_ru=[_clean_person(x) for x in (anchors[1:] if len(anchors)>1 else [])]
                raw=li.get_text(" ", strip=True); time_ru=_extract_time(raw)
                res.append(SRUGoal(time_ru, scorer_ru, assists_ru))
    return res

def fetch_sportsru_goals(home_tri:str, away_tri:str)->Tuple[List[SRUGoal],List[SRUGoal],str]:
    hs=SPORTSRU_SLUG.get(home_tri); as_=SPORTSRU_SLUG.get(away_tri)
    if not hs or not as_: return [], [], ""
    for order in [(hs,as_),(as_,hs)]:
        url=f"https://www.sports.ru/hockey/match/{order[0]}-vs-{order[1]}/"
        try: html=http_get_text(url, timeout=20)
        except Exception as e: 
            print(f"[DBG] sports.ru fetch fail {url}: {repr(e)}"); continue
        home_side="home" if order[0]==hs else "away"; away_side="away" if home_side=="home" else "home"
        h=parse_sportsru_goals_html(html, home_side); a=parse_sportsru_goals_html(html, away_side)
        if h or a: return h,a,url
    return [],[], ""

def merge_official_with_sportsru(evs: List[ScoringEvent], sru_home: List[SRUGoal], sru_away: List[SRUGoal], home_tri:str, away_tri:str)->List[ScoringEvent]:
    h_i=a_i=0; out=[]
    for ev in evs:
        if ev.team_for==home_tri and h_i<len(sru_home):
            g=sru_home[h_i]; h_i+=1; ev.scorer=g.scorer_ru or ev.scorer or ""; ev.assists=g.assists_ru or ev.assists
        elif ev.team_for==away_tri and a_i<len(sru_away):
            g=sru_away[a_i]; a_i+=1; ev.scorer=g.scorer_ru or ev.scorer or ""; ev.assists=g.assists_ru or ev.assists
        out.append(ev)
    return out

# ---------------- Text building ----------------
def _italic(s:str)->str: return f"<i>{s}</i>"
def period_title_text(num:int, ptype:str, ot_index:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_index or 1}"
    if t=="SHOOTOUT": return "Буллиты"
    return f"Период {num}"
def line_goal(ev:ScoringEvent)->str:
    score=f"{ev.home_goals}:{ev.away_goals}"
    who=ev.scorer or "—"
    # одна скобка, без дубля «((…))»
    assists = ""
    if ev.assists:
        assists = " (" + ", ".join([a for a in ev.assists if a]) + ")"
    return f"{score} – {ev.time} {who}{assists}"

def shootout_winner_line(meta: GameMeta, events: List[ScoringEvent]) -> Optional[str]:
    so = [e for e in events if (e.period_type or "").upper()=="SHOOTOUT" and e.scorer]
    if not so: return None
    if meta.home_score > meta.away_score:
        winner = meta.home_tri
    elif meta.away_score > meta.home_score:
        winner = meta.away_tri
    else:
        winner = so[-1].team_for
    last_scorer = None
    for e in reversed(so):
        if e.team_for == winner:
            last_scorer = e.scorer
            break
    if not last_scorer: return None
    final_score = f"{meta.home_score}:{meta.away_score}"
    return f"Победный буллит\n{final_score} – {last_scorer}"

def build_single_match_text(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he=TEAM_EMOJI.get(meta.home_tri,""); ae=TEAM_EMOJI.get(meta.away_tri,"")
    hn=TEAM_RU.get(meta.home_tri,meta.home_tri); an=TEAM_RU.get(meta.away_tri,meta.away_tri)
    hrec=standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "0-0-0"
    arec=standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "0-0-0"
    head=f"{he} <b>«{hn}»: {meta.home_score}</b> ({hrec})\n{ae} <b>«{an}»: {meta.away_score}</b> ({arec})"

    groups:Dict[Tuple[int,str],List[ScoringEvent]]={}
    for ev in events: groups.setdefault((ev.period,ev.period_type),[]).append(ev)
    for pnum in (1,2,3):
        groups.setdefault((pnum,"REGULAR"),[])
    ot_keys=sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total=len(ot_keys); ot_order={k:i+1 for i,k in enumerate(ot_keys)}

    lines=[head]
    sort_key=lambda x:(x[0], 0 if (x[1] or "").upper()=="REGULAR" else 1 if (x[1] or "").upper()=="OVERTIME" else 2)
    for key in sorted(groups.keys(), key=sort_key):
        pnum,ptype=key
        if (ptype or "").upper()=="SHOOTOUT":
            continue  # попытки не расписываем
        ot_idx=ot_order.get(key)
        title=period_title_text(pnum,ptype,ot_idx,ot_total)
        lines.append("")              # пустая строка
        lines.append(_italic(title))  # курсивом
        per=groups[key]
        if not per: lines.append("Голов не было")
        else:
            for ev in per: lines.append(line_goal(ev))

    so_line = shootout_winner_line(meta, events)
    if so_line:
        lines.append("")
        lines.append(_italic("Буллиты"))
        lines.append(so_line)

    return "\n".join(lines).strip()

# ---------------- State & Telegram ----------------
def load_state(path:str)->Dict[str,Any]:
    p=pathlib.Path(path)
    if not p.exists(): p.parent.mkdir(parents=True, exist_ok=True); return {"posted":{}}
    try:
        return json.loads(p.read_text("utf-8") or "{}") or {"posted":{}}
    except Exception:
        return {"posted":{}}
def save_state(path:str, data:Dict[str,Any])->None:
    p=pathlib.Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")

def send_telegram_text(text:str)->None:
    token=_env_str("TELEGRAM_BOT_TOKEN","").strip()
    chat_id=_env_str("TELEGRAM_CHAT_ID","").strip()
    thread=_env_str("TELEGRAM_THREAD_ID","").strip()
    if not token or not chat_id: print("[ERR] Telegram token/chat_id not set"); return
    url=f"{TG_API}/bot{token}/sendMessage"; headers={"Content-Type":"application/json"}
    payload={
        "chat_id": int(chat_id) if chat_id.strip("-").isdigit() else chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "disable_notification": False,
        "parse_mode": "HTML",
    }
    if thread:
        try: payload["message_thread_id"]=int(thread)
        except: pass
    if DRY_RUN: print("[DRY RUN] "+textwrap.shorten(text, 200, placeholder="…")); return
    resp=requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    try: data=resp.json()
    except: data={"ok":None,"raw":resp.text}
    print(f"[DBG] TG HTTP={resp.status_code} JSON={data}")
    if resp.status_code!=200 or not data.get("ok",False):
        print(f"[ERR] sendMessage failed: {data.get('error_code')} {data.get('description')}")

# ---------------- Main ----------------
def main():
    state=load_state(STATE_PATH)
    posted:Dict[str,bool]=state.get("posted",{}) or {}

    # --- Manual by GAME_PK ---
    if GAME_PK_ENV:
        try:
            manual_gid = int(GAME_PK_ENV)
        except:
            print(f"[ERR] GAME_PK is not int: {GAME_PK_ENV}")
            return

        manual_meta = None
        if GAME_DATE_ENV:
            manual_meta = fetch_meta_from_schedule(GAME_DATE_ENV, manual_gid)
        if not manual_meta:
            manual_meta = fetch_meta_fallback(manual_gid)
        if not manual_meta:
            print(f"[ERR] Cannot build meta for GAME_PK={manual_gid}")
            return

        evs=fetch_scoring_official(manual_gid, manual_meta.home_tri, manual_meta.away_tri)
        sru_home, sru_away, _ = fetch_sportsru_goals(manual_meta.home_tri, manual_meta.away_tri)
        merged=merge_official_with_sportsru(evs, sru_home, sru_away, manual_meta.home_tri, manual_meta.away_tri)
        standings=fetch_standings_map()
        text=build_single_match_text(manual_meta, standings, merged)
        print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
        send_telegram_text(text)

        if not FORCE_POST:
            posted[str(manual_gid)] = True
            state["posted"]=posted; save_state(STATE_PATH, state)
        print("OK (posted 1 manual)")
        return

    # --- Manual by GAME_DATE + MATCH ---
    if GAME_DATE_ENV and MATCH_ENV:
        gid = resolve_game_pk_from_match(GAME_DATE_ENV, MATCH_ENV, SEARCH_BACK, SEARCH_FWD, REQUIRE_FINAL)
        if not gid:
            print(f"[ERR] Cannot resolve MATCH='{MATCH_ENV}' around {GAME_DATE_ENV} (±{SEARCH_BACK}/{SEARCH_FWD} days)")
            return
        print(f"[DBG] Resolved GAME_PK={gid} for {GAME_DATE_ENV} {MATCH_ENV}")

        manual_meta = fetch_meta_from_schedule(GAME_DATE_ENV, gid) or fetch_meta_fallback(gid)
        if not manual_meta:
            print(f"[ERR] Cannot build meta for GAME_PK={gid}")
            return

        evs=fetch_scoring_official(gid, manual_meta.home_tri, manual_meta.away_tri)
        sru_home, sru_away, _ = fetch_sportsru_goals(manual_meta.home_tri, manual_meta.away_tri)
        merged=merge_official_with_sportsru(evs, sru_home, sru_away, manual_meta.home_tri, manual_meta.away_tri)
        standings=fetch_standings_map()
        text=build_single_match_text(manual_meta, standings, merged)
        print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
        send_telegram_text(text)

        if not FORCE_POST:
            posted[str(gid)] = True
            state["posted"]=posted; save_state(STATE_PATH, state)
        print("OK (posted 1 manual)")
        return

    # --- Legacy mode: yesterday+today FINAL unposted ---
    games=list_games_yesterday_today_final()
    if DEBUG_VERBOSE: print(f"[DBG] FINAL games (yesterday+today): {len(games)}")
    if not games:
        print("OK (no FINAL games)"); return
    standings=fetch_standings_map()
    new_posts=0
    for meta in games:
        key=str(meta.gamePk)
        if posted.get(key) and not FORCE_POST:
            if DEBUG_VERBOSE: print(f"[DBG] skip already posted {key}")
            continue
        evs=fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)
        sru_home, sru_away, _ = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        merged=merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)
        text=build_single_match_text(meta, standings, merged)
        print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
        send_telegram_text(text)
        posted[key]=True; new_posts+=1
    state["posted"]=posted; save_state(STATE_PATH, state)
    print(f"OK (posted {new_posts})")

if __name__=="__main__":
    main()
