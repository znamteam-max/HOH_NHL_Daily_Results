#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — per-game instant posts (no repeats)

— Поддержка: GAME_PK или GAME_QUERY="YYYY-MM-DD HOME - AWAY" / "YYYY-MM-DD AWY@HOME".
— Формат: жирным «Команда»: счёт; рекорд в скобках (W-L-OT). Далее события по периодам.
— Фамилии подтягиваются из official PBP (players[]) + опционально sports.ru (кириллица) по порядку.
— Починен кейс пустых имён, УТА, дубликаты скобок у ассистов.

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TELEGRAM_THREAD_ID
- GAME_PK (приоритет) или GAME_QUERY="YYYY-MM-DD HOME - AWAY" / "YYYY-MM-DD AWY@HOME"
- DRY_RUN=0/1
- DEBUG_VERBOSE=0/1
"""

from __future__ import annotations
import os, re, json, textwrap
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
GAME_SUMMARY_FMT = NHLE_BASE + "/gamecenter/{gamePk}/game-summary"

def _env_str(n:str, d:str="")->str: 
    v=os.getenv(n); return v if v is not None else d
def _env_bool(n:str, d:bool=False)->bool:
    v=os.getenv(n); 
    return d if v is None else str(v).strip().lower() in ("1","true","yes","y","on")

DRY_RUN = _env_bool("DRY_RUN", False)
DEBUG_VERBOSE = _env_bool("DEBUG_VERBOSE", True)

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

UA_HEADERS={"User-Agent":"Mozilla/5.0","Accept-Language":"ru,en;q=0.8"}
def _get(url:str, to_text:bool=False, timeout:int=25):
    r=requests.get(url, headers=UA_HEADERS, timeout=timeout); r.raise_for_status()
    if to_text:
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    return r.json()

# --- Models ---
@dataclass
class TeamRecord:
    wins:int; losses:int; ot:int; points:int
    def as_str(self)->str: return f"{self.wins}-{self.losses}-{self.ot}"
@dataclass
class GameMeta:
    gamePk:int; home_tri:str; away_tri:str; home_score:int; away_score:int
@dataclass
class ScoringEvent:
    period:int; period_type:str; time:str; team_for:str; home_goals:int; away_goals:int; scorer:str; assists:List[str]=field(default_factory=list)
@dataclass
class SRUGoal:
    time:Optional[str]; scorer_ru:Optional[str]; assists_ru:List[str]

# --- Utils ---
def _upper(x:Any)->str:
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
def _name(obj)->Optional[str]:
    if not obj: return None
    if isinstance(obj,str): return obj.strip() or None
    if isinstance(obj,dict):
        for k in ("name","playerName","playerFullName","default","fullName","firstLastName","lastFirstName","shortName"):
            v=obj.get(k)
            if isinstance(v,str) and v.strip(): return v.strip()
    return None
def _clean_person(s:str)->str:
    return re.sub(r"^\(+|\)+$", "", (s or "").strip())

# --- Standings/Meta ---
def fetch_standings()->Dict[str,TeamRecord]:
    js=_get(f"{NHLE_BASE}/standings/now")
    teams={}; nodes=[]
    if isinstance(js,dict):
        if isinstance(js.get("standings"),list): nodes=js["standings"]
        elif isinstance(js.get("records"),list): nodes=js["records"]
        elif isinstance(js.get("standings"),dict): nodes=js["standings"].get("overallRecords",[]) or []
    elif isinstance(js,list): nodes=js
    for r in nodes:
        abbr=""; ta=r.get("teamAbbrev")
        if isinstance(ta,str): abbr=ta.upper()
        elif isinstance(ta,dict): abbr=_upper(ta.get("default") or ta.get("tricode"))
        if not abbr: abbr=_upper(r.get("teamAbbrevTricode") or r.get("teamTriCode") or r.get("team"))
        rec=r.get("record") or r.get("overallRecord") or r.get("overallRecords") or {}
        wins=_first_int(rec.get("wins"),r.get("wins"),rec.get("gamesPlayedWins"))
        losses=_first_int(rec.get("losses"),r.get("losses"),rec.get("gamesPlayedLosses"),rec.get("regulationLosses"),r.get("regulationLosses"))
        ot=_first_int(rec.get("ot"),r.get("ot"),rec.get("otLosses"),r.get("otLosses"),rec.get("overtimeLosses"),r.get("overtimeLosses"))
        pts=_first_int(r.get("points"),rec.get("points"),r.get("pts"),rec.get("teamPoints"))
        if abbr: teams[abbr]=TeamRecord(wins,losses,ot,pts)
    return teams

def fetch_meta(gamePk:int)->GameMeta:
    js=_get(GAME_SUMMARY_FMT.format(gamePk=gamePk))
    g=js.get("game",{}) or {}
    home=g.get("homeTeam",{}) or {}; away=g.get("awayTeam",{}) or {}
    htri=_upper(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
    atri=_upper(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
    hsc=_first_int(home.get("score")); asc=_first_int(away.get("score"))
    return GameMeta(gamePk, htri, atri, hsc, asc)

# --- PBP parse ---
_SO_TYPES_GOAL={"GOAL","SHOT"}
def _ptype(t:str)->str:
    t=_upper(t)
    if t in ("","REG"): return "REGULAR"
    if t=="OT": return "OVERTIME"
    if t=="SO": return "SHOOTOUT"
    return t
def _is_so_goal(type_key:str, det:dict, pt:str)->bool:
    if pt!="SHOOTOUT": return False
    if type_key not in _SO_TYPES_GOAL: return False
    for k in ("wasGoal","shotWasGoal","isGoal","isScored","scored"):
        v=det.get(k)
        if isinstance(v,bool) and v: return True
        if isinstance(v,str) and v.strip().lower() in ("1","true","yes"): return True
    return type_key=="GOAL"

_SCORER_KEYS=("scoringPlayerName","scorerName","shootingPlayerName","scoringPlayer","goalScorer","primaryScorer","playerName","player","shooterName","shootoutShooterName","shooter","scorer")
_ASSIST_KEYS=("assist1PlayerName","assist2PlayerName","assist3PlayerName","assist1","assist2","assist3","primaryAssist","secondaryAssist","tertiaryAssist")

def _extract_from_players(p:dict)->Tuple[str,List[str]]:
    arr=None
    for key in ("players","playerList","participants"):
        v=p.get(key)
        if isinstance(v,list) and v: arr=v; break
    if not arr:
        det=p.get("details") or {}
        for key in ("players","playerList","participants"):
            v=det.get(key)
            if isinstance(v,list) and v: arr=v; break
    scorer=""; assists=[]
    for it in arr or []:
        role = (_name(it.get("playerType")) or _name(it.get("type")) or _name(it.get("role")) or "").lower()
        nm = _name(it) or _name(it.get("player","")) or _name({"name":it.get("firstLastName")})
        if not nm: continue
        if "scor" in role or "shoot" in role or role in ("goal","goalscorer","scorer"):
            scorer = nm
        elif "assist" in role:
            assists.append(nm)
    return scorer, assists

def fetch_scoring_official(gamePk:int, home_tri:str, away_tri:str)->List[ScoringEvent]:
    js=_get(PBP_FMT.format(gamePk=gamePk))
    plays=js.get("plays",[]) or []
    out=[]; prev_h=prev_a=0
    for p in plays:
        type_key=_upper(p.get("typeDescKey"))
        pd=p.get("periodDescriptor",{}) or {}
        per=_first_int(pd.get("number")); pt=_ptype(pd.get("periodType") or "REG")
        det=p.get("details",{}) or {}
        t=str(p.get("timeInPeriod") or "00:00").replace(":",".")
        is_goal = (type_key=="GOAL") or _is_so_goal(type_key, det, pt)
        if not is_goal: continue

        h=det.get("homeScore"); a=det.get("awayScore")
        if not (isinstance(h,int) and isinstance(a,int)):
            sc=p.get("score",{}) or {}
            if isinstance(sc.get("home"),int) and isinstance(sc.get("away"),int): h,a=sc["home"],sc["away"]
            else: h,a=prev_h,prev_a

        team = home_tri if h>prev_h else (away_tri if a>prev_a else _upper(
            det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")
        ))

        scorer=""
        for k in _SCORER_KEYS:
            nm=_name(det.get(k))
            if nm: scorer=nm; break
        assists=[]
        for k in _ASSIST_KEYS:
            nm=_name(det.get(k))
            if nm: assists.append(nm)

        if not scorer:
            s2,a2=_extract_from_players(p)
            scorer=s2 or scorer
            if not assists and a2: assists=a2

        scorer=_clean_person(scorer)
        assists=[_clean_person(x) for x in assists]

        out.append(ScoringEvent(per,pt,t,team,h,a,scorer,assists))
        if pt!="SHOOTOUT": prev_h,prev_a=h,a
    if DEBUG_VERBOSE: print(f"[DBG] PBP goals parsed: {len(out)} for game {gamePk}")
    return out

# --- sports.ru ---
def _sru_fetch(home:str, away:str)->Tuple[List[SRUGoal],List[SRUGoal],str]:
    hs=SPORTSRU_SLUG.get(home); as_=SPORTSRU_SLUG.get(away)
    if not hs or not as_: return [],[], ""
    for order in [(hs,as_),(as_,hs)]:
        url=f"https://www.sports.ru/hockey/match/{order[0]}-vs-{order[1]}/"
        try:
            html=_get(url, to_text=True, timeout=20)
        except Exception as e:
            if DEBUG_VERBOSE: print("[DBG] sports.ru fail", url, repr(e))
            continue
        if not HAS_BS: return [],[], ""
        soup=BS(html,"html.parser")
        def parse(side:str)->List[SRUGoal]:
            res=[]
            ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
            if not ul: return res
            for li in ul.find_all("li", recursive=False):
                anchors=[a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru=anchors[0] if anchors else None
                assists_ru=anchors[1:] if len(anchors)>1 else []
                raw=li.get_text(" ", strip=True)
                m=re.search(r"\b(\d{1,2})[:.](\d{2})\b", raw or "")
                tm = f"{int(m.group(1)):02d}.{m.group(2)}" if m else None
                res.append(SRUGoal(tm, scorer_ru, assists_ru))
            return res
        home_side = "home" if order[0]==hs else "away"
        h = parse(home_side); a = parse("away" if home_side=="home" else "home")
        if h or a:
            if DEBUG_VERBOSE: print(f"[DBG] sports.ru goals ok for {url}: home={len(h)} away={len(a)}")
            return h,a,url
    return [],[], ""

def merge_with_sru(evs: List[ScoringEvent], sru_home: List[SRUGoal], sru_away: List[SRUGoal], home_tri:str, away_tri:str)->List[ScoringEvent]:
    h_i=a_i=0; out=[]
    for ev in evs:
        if ev.team_for==home_tri and h_i<len(sru_home):
            g=sru_home[h_i]; h_i+=1
            ev.scorer=g.scorer_ru or ev.scorer or ""
            ev.assists=g.assists_ru or ev.assists
        elif ev.team_for==away_tri and a_i<len(sru_away):
            g=sru_away[a_i]; a_i+=1
            ev.scorer=g.scorer_ru or ev.scorer or ""
            ev.assists=g.assists_ru or ev.assists
        ev.scorer=_clean_person(ev.scorer)
        ev.assists=[_clean_person(x) for x in ev.assists]
        out.append(ev)
    return out

# --- Format & Telegram ---
def _italic(s:str)->str: return f"<i>{s}</i>"
def _period_title(num:int, ptype:str, ot_i:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_i or 1}"
    if t=="SHOOTOUT": return "Буллиты"
    return f"Период {num}"
def _line(ev:ScoringEvent)->str:
    score=f"{ev.home_goals}:{ev.away_goals}"
    who=ev.scorer or "—"
    ass = [x for x in ev.assists if x]
    tail = f" ({', '.join(ass)})" if ass else ""
    return f"{score} – {ev.time} {who}{tail}"

def build_text(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he=TEAM_EMOJI.get(meta.home_tri,""); ae=TEAM_EMOJI.get(meta.away_tri,"")
    hn=TEAM_RU.get(meta.home_tri,meta.home_tri); an=TEAM_RU.get(meta.away_tri,meta.away_tri)
    hrec=standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec=standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head=f"{he} <b>«{hn}»: {meta.home_score}</b> ({hrec})\n{ae} <b>«{an}»: {meta.away_score}</b> ({arec})"

    groups:Dict[Tuple[int,str],List[ScoringEvent]]={}
    for ev in events: groups.setdefault((ev.period,ev.period_type),[]).append(ev)
    for pnum in (1,2,3):
        if (pnum,"REGULAR") not in groups: groups[(pnum,"REGULAR")]=[]
    ot_keys=sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total=len(ot_keys); ot_order={k:i+1 for i,k in enumerate(ot_keys)}

    lines=[head]
    sort_key=lambda x:(x[0], 0 if (x[1] or "").upper()=="REGULAR" else 1 if (x[1] or "").upper()=="OVERTIME" else 2)
    for key in sorted(groups.keys(), key=sort_key):
        pnum,ptype=key; ot_idx=ot_order.get(key)
        title=_period_title(pnum,ptype,ot_idx,ot_total)
        lines.append("")
        lines.append(_italic(title))
        per=groups[key]
        if not per: lines.append("Голов не было")
        else:
            for ev in per: lines.append(_line(ev))
    return "\n".join(lines).strip()

def send_tg(text:str)->None:
    token=_env_str("TELEGRAM_BOT_TOKEN","").strip()
    chat=_env_str("TELEGRAM_CHAT_ID","").strip()
    thread=_env_str("TELEGRAM_THREAD_ID","").strip()
    if not token or not chat: print("[ERR] Telegram token/chat_id not set"); return
    url=f"{TG_API}/bot{token}/sendMessage"
    payload={"chat_id": int(chat) if chat.strip("-").isdigit() else chat,
             "text": text, "disable_web_page_preview": True, "disable_notification": False, "parse_mode":"HTML"}
    if thread:
        try: payload["message_thread_id"]=int(thread)
        except: pass
    if DRY_RUN:
        print("[DRY RUN] "+textwrap.shorten(text,200,placeholder="…")); return
    r=requests.post(url, json=payload, timeout=30)
    try: js=r.json()
    except: js={"ok":None,"raw":r.text}
    if DEBUG_VERBOSE: print(f"[DBG] TG HTTP={r.status_code} JSON keys={list(js.keys())}")
    if r.status_code!=200 or not js.get("ok",False):
        print(f"[ERR] sendMessage failed: {js.get('error_code')} {js.get('description')}")

# --- GAME QUERY resolve ---
def resolve_game_pk() -> Optional[int]:
    gp=(_env_str("GAME_PK","") or "").strip()
    if gp: 
        try: return int(gp)
        except: pass
    gq=(_env_str("GAME_QUERY","") or "").strip()
    if not gq: 
        print("[ERR] provide GAME_PK or GAME_QUERY")
        return None
    # Формат: YYYY-MM-DD HOME - AWAY или YYYY-MM-DD AWY@HOME
    m=re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s+(.+)$", gq)
    if not m: 
        print(f"[ERR] GAME_QUERY invalid: {gq}")
        return None
    day=m.group(1); tail=m.group(2).upper().replace("—","-").replace("–","-")
    home=away=""
    if "@" in tail:
        away,home=[re.sub(r"\s+","",x) for x in tail.split("@",1)]
    elif "-" in tail:
        left,right=[re.sub(r"\s+","",x) for x in tail.split("-",1)]
        home,away=left,right
    else:
        print(f"[ERR] GAME_QUERY team pair invalid: {gq}"); return None

    js=_get(f"{NHLE_BASE}/schedule/{day}")
    candidates = js.get("games")
    if candidates is None:
        candidates=[]
        for w in js.get("gameWeek") or []:
            candidates += (w.get("games") or [])
    for g in candidates:
        ht=_upper((g.get("homeTeam") or {}).get("abbrev") or (g.get("homeTeam") or {}).get("triCode"))
        at=_upper((g.get("awayTeam") or {}).get("abbrev") or (g.get("awayTeam") or {}).get("triCode"))
        if ht==home and at==away:
            gid=_first_int(g.get("id"),g.get("gameId"),g.get("gamePk"))
            if gid: 
                if DEBUG_VERBOSE: print(f"[DBG] Resolved GAME_PK={gid} for {gq}")
                return gid
    print(f"[ERR] GAME_QUERY not resolved: {gq}")
    return None

def main():
    gid=resolve_game_pk()
    if not gid: 
        return
    meta=fetch_meta(gid)
    events=fetch_scoring_official(gid, meta.home_tri, meta.away_tri)
    # sports.ru (транслит)
    sru_h, sru_a, _ = _sru_fetch(meta.home_tri, meta.away_tri)
    events=merge_with_sru(events, sru_h, sru_a, meta.home_tri, meta.away_tri)

    standings=fetch_standings()
    text=build_text(meta, standings, events)
    if DEBUG_VERBOSE: print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
    send_tg(text)
    print("OK (posted 1)")

if __name__=="__main__":
    main()
