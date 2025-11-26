#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — per-game instant posts (no repeats)

• Формат: жирным «Команда»: счёт; рекорд в скобках (W-L-OT).
• Заголовки периодов/ОТ курсивом; «Овертайм» без №, если он один.
• Перед каждым заголовком периода — пустая строка.
• Буллиты: печатаем ТОЛЬКО победный буллит отдельным блоком:
    "Победный буллит"
    "<итоговый счёт> - <Имя>"
• Если в PBP gamecenter нет SHOOTOUT, берём победный буллит из statsapi.
• Фамилии голов — через sports.ru (кириллица), победный буллит — через ru_map.json (если есть) или латиницей.
• Поддержка UTA (Юта). Без повторов (state/posted_games.json).

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TELEGRAM_THREAD_ID
- STATE_PATH="state/posted_games.json"
- DRY_RUN=0/1
- DEBUG_VERBOSE=0/1

Ручной запуск (через workflow):
- GAME_PK (точный), либо GAME_DATE=YYYY-MM-DD + MATCH="SEA - NYI" (или "SEA@NYI")
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
SCHED_FMT  = NHLE_BASE + "/schedule/{date}"
WEB_SUMM   = NHLE_BASE + "/gamecenter/{gamePk}/game-summary"

STATS_FEED = "https://statsapi.web.nhl.com/api/v1/game/{gamePk}/feed/live"

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

DRY_RUN = _env_bool("DRY_RUN", False)
DEBUG_VERBOSE = _env_bool("DEBUG_VERBOSE", False)
STATE_PATH = _env_str("STATE_PATH", "state/posted_games.json")

# ---------------- Dictionaries ----------------
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

# ---------------- HTTP helpers ----------------
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

# ---------------- Utils ----------------
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

# ---------------- Standings ----------------
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

# ---------------- Schedule resolve for manual runs ----------------
def list_games_by_date(date_iso: str) -> List[GameMeta]:
    s=http_get_json(SCHED_FMT.format(date=date_iso))
    now_utc=datetime.now(timezone.utc)
    metas:Dict[int,GameMeta]={}
    for w in s.get("gameWeek",[]) or []:
        for g in w.get("games",[]) or []:
            gid=_first_int(g.get("id"),g.get("gameId"),g.get("gamePk"))
            if gid==0: continue
            state=_upper_str(g.get("gameState") or g.get("gameStatus"))
            gd=g.get("startTimeUTC") or g.get("gameDate") or ""
            try: gdt=datetime.fromisoformat(str(gd).replace("Z","+00:00"))
            except: gdt=now_utc
            home=g.get("homeTeam",{}) or {}; away=g.get("awayTeam",{}) or {}
            htri=_upper_str(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
            atri=_upper_str(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
            hscore=_first_int(home.get("score")); ascore=_first_int(away.get("score"))
            metas[gid]=GameMeta(gid,gdt,state,htri,atri,hscore,ascore)
    return sorted(metas.values(), key=lambda m:m.gameDateUTC)

def resolve_game_pk_from_inputs() -> Optional[int]:
    pk=_env_str("GAME_PK","").strip()
    if pk: 
        try: return int(pk)
        except: pass
    date=_env_str("GAME_DATE","").strip()
    match=_env_str("MATCH","").strip()
    if not (date and match): return None
    # normalize "SEA - NYI" / "SEA@NYI"
    m=match.replace("—","-").replace("@","-").replace("–","-")
    parts=[p.strip().upper() for p in m.split("-") if p.strip()]
    if len(parts)!=2: return None
    left,right=parts[0],parts[1]
    games=list_games_by_date(date)
    # try both orders
    for g in games:
        if {g.home_tri,g.away_tri}=={left,right}:
            # prefer exact order "AWAY - HOME" typical? we'll just return any match of pair
            print(f"[DBG] Resolved GAME_PK={g.gamePk} for {date} {match}")
            return g.gamePk
    return None

# ---------------- PBP & Sports.ru ----------------
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
        team=home_tri if h>prev_h else (away_tri if a>prev_a else _upper_str(
            det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")
        ))

        scorer=""
        for k in _SCORER_KEYS:
            nm=_extract_name(det.get(k))
            if nm: scorer=nm; break
        if not scorer:
            for k in ("scoringPlayerName","scorerName","shootingPlayerName"):
                v=p.get(k)
                if isinstance(v,str) and v.strip(): scorer=v.strip(); break

        assists=[]
        for k in _ASSIST_KEYS:
            nm=_extract_name(det.get(k))
            if nm: assists.append(nm)

        events.append(ScoringEvent(period,ptype,t,team,h,a,scorer,assists))
        if ptype!="SHOOTOUT":
            prev_h,prev_a=h,a
    return events

# --- sports.ru goals for RU names ---
TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")
def _extract_time(text: str)->Optional[str]:
    m=TIME_RE.search(text or "")
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None

def parse_sportsru_goals_html(html: str, side: str)->List[SRUGoal]:
    res: List[SRUGoal] = []
    if HAS_BS:
        soup=BS(html,"lxml" if "lxml" in globals() else "html.parser")
        sel=f"ul.match-summary__goals-list--{side}"
        ul=soup.select_one(sel) or soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors=[a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru=anchors[0] if anchors else None
                assists_ru=anchors[1:] if len(anchors)>1 else []
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
            if DEBUG_VERBOSE: print(f"[DBG] sports.ru fetch fail {url}: {repr(e)}")
            continue
        home_side="home" if order[0]==hs else "away"; away_side="away" if home_side=="home" else "home"
        h=parse_sportsru_goals_html(html, home_side); a=parse_sportsru_goals_html(html, away_side)
        if h or a:
            if DEBUG_VERBOSE: print(f"[DBG] sports.ru goals ok for {url}: home={len(h)} away={len(a)}")
            return h,a,url
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

# ---------------- RU map (optional) ----------------
def load_ru_map() -> Dict[str,str]:
    try:
        p=pathlib.Path("ru_map.json")
        if p.exists():
            data=json.loads(p.read_text("utf-8"))
            if isinstance(data, dict): return {str(k):str(v) for k,v in data.items()}
    except Exception as e:
        if DEBUG_VERBOSE: print(f"[DBG] ru_map load fail: {e}")
    return {}

def map_name_ru(name: str, ru_map: Dict[str,str]) -> str:
    if not name: return name
    # полное совпадение
    v=ru_map.get(name)
    if v: return v
    # пробуем по фамилии
    parts=name.split()
    if parts:
        last=parts[-1]
        for k,v in ru_map.items():
            if k.split()[-1]==last:
                return v
    return name

# ---------------- Stats API fallback for shootout winner ----------------
def fetch_shootout_winner_from_statsapi(gamePk: int) -> Optional[str]:
    try:
        js=http_get_json(STATS_FEED.format(gamePk=gamePk), timeout=30)
    except Exception as e:
        print(f"[DBG] statsapi fallback failed: {repr(e)}")
        return None

    # plays can be in liveData.plays.allPlays; sometimes in 'plays'
    plays = None
    live = js.get("liveData") or {}
    if isinstance(live.get("plays"), dict):
        plays = live["plays"].get("allPlays") or live["plays"].get("plays")
    if not plays and isinstance(live.get("plays"), list):
        plays = live["plays"]
    if not plays: return None

    # filter shootout goals in chronological order
    so_goals=[]
    for p in plays:
        about=p.get("about",{}) or {}
        if (about.get("periodType") or "").upper()!="SHOOTOUT":
            continue
        r=p.get("result",{}) or {}
        et=r.get("eventTypeId") or r.get("event")
        et=str(et or "").upper()
        is_goal = et in ("SHOOTOUT_GOAL","GOAL")
        if not is_goal: 
            # some feeds mark as SHOT with 'description' including 'scores'
            if str(r.get("description") or "").lower().find("scores")<0: 
                continue
        # pick scorer
        who=""
        for pl in p.get("players") or []:
            if (pl.get("playerType") or "").lower()=="scorer" and isinstance(pl.get("player"),dict):
                who=pl["player"].get("fullName") or ""
                break
        if who:
            so_goals.append(who)

    if not so_goals:
        # try linescore deciding goal info (rare)
        ls = (js.get("liveData") or {}).get("linescore") or {}
        info = ls.get("shootoutInfo") or {}
        # no uniform structure across seasons; skip if absent
        return None

    # winner is the last goal in shootout sequence
    return so_goals[-1] if so_goals else None

# ---------------- Formatting ----------------
def _italic(s:str)->str: return f"<i>{s}</i>"

def period_title_text(num:int, ptype:str, ot_index:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_index or 1}"
    return f"Период {num}"

def line_goal(ev:ScoringEvent)->str:
    score=f"{ev.home_goals}:{ev.away_goals}"; who=ev.scorer or "—"; assists=f" ({', '.join(ev.assists)})" if ev.assists else ""
    return f"{score} – {ev.time} {who}{assists}"

def build_single_match_text(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent], shootout_winner_ru: Optional[str]) -> str:
    he=TEAM_EMOJI.get(meta.home_tri,""); ae=TEAM_EMOJI.get(meta.away_tri,"")
    hn=TEAM_RU.get(meta.home_tri,meta.home_tri); an=TEAM_RU.get(meta.away_tri,meta.away_tri)
    hrec=standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec=standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head=f"{he} <b>«{hn}»: {meta.home_score}</b> ({hrec})\n{ae} <b>«{an}»: {meta.away_score}</b> ({arec})"

    # group by period (REG/OT only). SHOOTOUT мы не перечисляем по попыткам.
    groups:Dict[Tuple[int,str],List[ScoringEvent]]={}
    for ev in events:
        if (ev.period_type or "").upper()=="SHOOTOUT":
            continue
        groups.setdefault((ev.period,ev.period_type),[]).append(ev)
    for pnum in (1,2,3):
        if (pnum,"REGULAR") not in groups: groups[(pnum,"REGULAR")]=[]
    ot_keys=sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total=len(ot_keys); ot_order={k:i+1 for i,k in enumerate(ot_keys)}

    lines=[head]
    sort_key=lambda x:(x[0], 0 if (x[1] or "").upper()=="REGULAR" else 1 if (x[1] or "").upper()=="OVERTIME" else 2)
    for key in sorted(groups.keys(), key=sort_key):
        pnum,ptype=key; ot_idx=ot_order.get(key)
        title=period_title_text(pnum,ptype,ot_idx,ot_total)
        lines.append("")
        lines.append(_italic(title))
        per=groups[key]
        if not per: lines.append("Голов не было")
        else:
            for ev in per: lines.append(line_goal(ev))

    # Победный буллит (только один, отдельно)
    if shootout_winner_ru:
        lines.append("")
        lines.append(_italic("Победный буллит"))
        lines.append(f"{meta.home_score}:{meta.away_score} - {shootout_winner_ru}")

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
def list_games_yesterday_today_final()->List[GameMeta]:
    now_utc=datetime.now(timezone.utc)
    dates=[(now_utc - timedelta(days=1)).date().isoformat(), now_utc.date().isoformat()]
    metas:Dict[int,GameMeta]={}
    for day in dates:
        s=http_get_json(SCHED_FMT.format(date=day))
        for w in s.get("gameWeek",[]) or []:
            for g in w.get("games",[]) or []:
                state=_upper_str(g.get("gameState") or g.get("gameStatus"))
                if state not in ("FINAL","OFF"): continue
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

def main():
    # Resolve specific game if provided
    gamePk = resolve_game_pk_from_inputs()
    ru_map = load_ru_map()

    state=load_state(STATE_PATH)
    posted:Dict[str,bool]=state.get("posted",{}) or {}

    standings=fetch_standings_map()
    targets: List[GameMeta] = []

    if gamePk:
        # fetch meta for this gamePk (via schedule around dates)
        # quick: scan yesterday+today and also +/-2 days
        for delta in range(-2,3):
            date_iso=(datetime.utcnow()+timedelta(days=delta)).date().isoformat()
            for m in list_games_by_date(date_iso):
                if m.gamePk==gamePk: targets=[m]; break
            if targets: break
        if not targets:
            # fallback: minimal meta via web summary (may 404), skip if not found
            print(f"[DBG] GAME_PK provided but not in schedule window: {gamePk}")
            # it’s okay; we’ll try PBP anyway to print team codes
            # if PBP fails — we abort.
            pass
    else:
        # auto mode: yesterday+today finished games that weren't posted
        targets = [m for m in list_games_yesterday_today_final() if not posted.get(str(m.gamePk))]

    if not targets and not gamePk:
        print("OK (no FINAL games to post)")
        return

    posted_new=0
    metas = targets if targets else []

    if gamePk and not metas:
        # As a last resort, try to infer teams from PBP and forge meta
        try:
            # we can't infer scores safely without summary, so abort gracefully
            print("[ERR] could not resolve metadata for provided GAME_PK")
            return
        except Exception:
            return

    for meta in metas:
        key=str(meta.gamePk)
        # manual run may want to re-post: do not block on posted-state
        force_post = _env_bool("FORCE_POST", True)
        if posted.get(key) and not force_post:
            if DEBUG_VERBOSE: print(f"[DBG] skip already posted {key}")
            continue

        # pull events
        evs=fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)

        # RU names for regulation/OT via sports.ru
        sru_home, sru_away, _ = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        merged=merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)

        # shootout winner: if final is tie by reg+OT but score != tie or PBP has no SHOOTOUT
        has_so_in_pbp = any((e.period_type or "").upper()=="SHOOTOUT" for e in evs)
        shootout_winner = None
        if not has_so_in_pbp and (meta.home_score == meta.away_score):
            # unlikely case; normally SO changes final score
            pass
        if not has_so_in_pbp and (meta.home_score != meta.away_score):
            shootout_winner = fetch_shootout_winner_from_statsapi(meta.gamePk)
        elif has_so_in_pbp:
            # take last SHOOTOUT goal as winner from PBP (rarely present)
            so_only = [e for e in evs if (e.period_type or "").upper()=="SHOOTOUT"]
            if so_only:
                shootout_winner = so_only[-1].scorer or None

        shootout_winner_ru = map_name_ru(shootout_winner, ru_map) if shootout_winner else None

        text=build_single_match_text(meta, standings, merged, shootout_winner_ru)
        print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
        send_telegram_text(text)
        posted[key]=True; posted_new+=1

    state["posted"]=posted; save_state(STATE_PATH, state)
    print(f"OK (posted {posted_new}{' manual' if gamePk else ''})")

if __name__=="__main__":
    main()
