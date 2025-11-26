#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — постим один матч

— GAME_PK или GAME_QUERY ("YYYY-MM-DD AAA - BBB" или "AAA@BBB"; порядок допускается любой).
— META: сначала game-summary; если 404 — ищем в schedule за последние 10 дней и подставляем.
— Голы и имена: такой же «робастный» парсер, как в daily (details.scorer/assists, details.players[]).
— Буллиты: «Победный буллит\n<итоговый счёт> - <Имя>».
"""

from __future__ import annotations
import os, re, json, time, textwrap, pathlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import requests

TG_API     = "https://api.telegram.org"
NHLE_BASE  = "https://api-web.nhle.com/v1"
PBP_FMT    = NHLE_BASE + "/gamecenter/{gamePk}/play-by-play"
GAME_SUMMARY_FMT = NHLE_BASE + "/gamecenter/{gamePk}/game-summary"
SCHED_FMT  = NHLE_BASE + "/schedule/{day}"

# ===== ENV =====
def _env_str(n, d=""): v=os.getenv(n); return v if v is not None else d
def _env_bool(n, d=False): 
    v=os.getenv(n); 
    return d if v is None else (str(v).strip().lower() in ("1","true","yes","y","on"))
def _env_int(n, d:int): 
    v=os.getenv(n); 
    if v is None: return d
    try: return int(str(v).strip())
    except: return d

DRY_RUN=_env_bool("DRY_RUN", False)
DEBUG_VERBOSE=_env_bool("DEBUG_VERBOSE", False)

# ===== RU / TEAMS =====
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

UA_HEADERS={"User-Agent":"Mozilla/5.0", "Accept-Language":"ru,en;q=0.8"}

def _get_with_retries(url: str, timeout: int = 30, tries: int = 3, backoff: float = 0.75, as_text: bool = False):
    last=None
    for i in range(1, tries+1):
        try:
            r=requests.get(url, headers=UA_HEADERS, timeout=timeout); r.raise_for_status()
            if as_text:
                r.encoding = r.apparent_encoding or "utf-8"
                return r.text
            return r.json()
        except Exception as e:
            last=e
            if i<tries:
                sleep_s=backoff*(2**(i-1))
                print(f"[DBG] retry {i}/{tries} for {url} after {sleep_s:.2f}s: {repr(e)}")
                time.sleep(sleep_s)
            else:
                raise
    raise last
def http_get_json(url: str, timeout: int = 30) -> Any: return _get_with_retries(url, timeout=timeout, as_text=False)

# ===== Models =====
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

# ===== small utils =====
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

def _pick_name_from_person(obj: Any) -> Optional[str]:
    if not obj: return None
    if isinstance(obj, str):
        return obj.strip() or None
    if isinstance(obj, dict):
        for k in ("displayName","playerName","default","name","fullName","firstLastName","lastFirstName","shortName"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        def _part(d):
            if isinstance(d, str) and d.strip(): return d.strip()
            if isinstance(d, dict):
                for kk in ("default","name"):
                    vv=d.get(kk)
                    if isinstance(vv, str) and vv.strip(): return vv.strip()
            return None
        fn=_part(obj.get("firstName")); ln=_part(obj.get("lastName"))
        if fn or ln:
            return " ".join([x for x in (fn, ln) if x])
    return None

# ===== standings =====
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

# ===== GAME_QUERY resolve =====
def fetch_schedule_day(day: str) -> List[dict]:
    js = http_get_json(SCHED_FMT.format(day=day))
    games = js.get("games")
    if games is None:
        games=[]
        for w in js.get("gameWeek") or []:
            games += w.get("games") or []
    return games or []

def resolve_game_pk_from_query(game_query: str, search_back:int=1, search_fwd:int=1, require_final:bool=True) -> Optional[int]:
    game_query = (game_query or "").strip()
    m = re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s+(.+)$", game_query)
    if not m: return None
    base_day, rest = m.group(1), m.group(2).strip().upper().replace(" ", "")
    if "@" in rest:
        away, home = rest.split("@",1)
    elif "-" in rest:
        left, right = rest.split("-",1)
        home, away = left, right
    else:
        return None
    tri_set={home, away}
    base_dt = datetime.fromisoformat(base_day)
    days=[(base_dt+timedelta(days=d)).date().isoformat() for d in range(-search_back, search_fwd+1)]
    for d in days:
        for g in fetch_schedule_day(d):
            st=_upper_str(g.get("gameState") or g.get("gameStatus"))
            if require_final and st not in ("FINAL","OFF"): continue
            ht=_upper_str((g.get("homeTeam") or {}).get("abbrev") or (g.get("homeTeam") or {}).get("triCode") or (g.get("homeTeam") or {}).get("teamAbbrev"))
            at=_upper_str((g.get("awayTeam") or {}).get("abbrev") or (g.get("awayTeam") or {}).get("triCode") or (g.get("awayTeam") or {}).get("teamAbbrev"))
            if {ht, at} != tri_set: continue
            gid=_first_int(g.get("id"), g.get("gameId"), g.get("gamePk"))
            if gid: 
                print(f"[DBG] Resolved GAME_PK={gid} for {game_query}")
                return gid
    print(f"[DBG] Unable to resolve GAME_PK for {game_query}")
    return None

# ===== PBP parsing (same as daily) =====
_SO_TYPES_GOAL={"GOAL","SHOT"}
_SCORER_KEYS=("scorer","shootoutScorer","shootoutShooter","shootingPlayerName",
              "scoringPlayerName","scorerName","primaryScorer","playerName","player")
_ASSIST_KEYS=("assists","assist","assist1","assist2","assist3","primaryAssist","secondaryAssist","tertiaryAssist")

def _normalize_period_type(t: str) -> str:
    t=_upper_str(t)
    if t in ("","REG"): return "REGULAR"
    if t=="OT": return "OVERTIME"
    if t=="SO": return "SHOOTOUT"
    return t
def _is_shootout_goal(type_key: str, details: dict, period_type: str) -> bool:
    if period_type!="SHOOTOUT": return False
    if type_key not in _SO_TYPES_GOAL: return False
    for k in ("wasGoal","shotWasGoal","isGoal","isScored","scored"):
        v=details.get(k)
        if isinstance(v,bool) and v: return True
        if isinstance(v,str) and v.strip().lower() in ("1","true","yes"): return True
    return type_key=="GOAL"

def _names_from_players_arrays(p: dict) -> Tuple[Optional[str], List[str]]:
    scorer=None; assists=[]
    d=p.get("details") or {}
    candidates=[]
    for arr in (p.get("players"), d.get("players"), d.get("assists"), d.get("assistants")):
        if isinstance(arr, list): candidates.append(arr)
    for arr in candidates:
        for itm in arr:
            if not isinstance(itm, dict): continue
            role=_upper_str(itm.get("playerType") or itm.get("type"))
            nm = _pick_name_from_person(itm) or _pick_name_from_person(itm.get("player")) \
                 or _pick_name_from_person(itm.get("name")) \
                 or _pick_name_from_person({"firstName":itm.get("firstName"),"lastName":itm.get("lastName")})
            if not nm: continue
            if role in ("SCORER","GOALSCORER","SHOOTER","SHOOTOUTSHOOTER"):
                scorer = nm
            elif "ASSIST" in role:
                assists.append(nm)
    return scorer, assists

def fetch_scoring_official(gamePk:int, home_tri:str, away_tri:str)->List[ScoringEvent]:
    data=http_get_json(PBP_FMT.format(gamePk=gamePk))
    plays=data.get("plays",[]) or []
    events:List[ScoringEvent]=[]
    prev_h=prev_a=0
    for p in plays:
        type_key=_upper_str(p.get("typeDescKey"))
        pd=p.get("periodDescriptor",{}) or {}
        period=_first_int(pd.get("number"))
        ptype=_normalize_period_type(pd.get("periodType") or "REG")
        det=p.get("details",{}) or {}
        t=str(p.get("timeInPeriod") or "00:00").replace(":",".")
        is_goal = (type_key=="GOAL") or _is_shootout_goal(type_key, det, ptype)
        if not is_goal: continue

        h=det.get("homeScore"); a=det.get("awayScore")
        if not (isinstance(h,int) and isinstance(a,int)):
            sc=p.get("score",{}) or {}
            if isinstance(sc.get("home"),int) and isinstance(sc.get("away"),int):
                h,a=sc["home"],sc["away"]
            else:
                h,a=prev_h,prev_a

        team = home_tri if h>prev_h else (away_tri if a>prev_a else _upper_str(
            det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")
        ))

        # names
        scorer=""
        for k in _SCORER_KEYS:
            nm=_pick_name_from_person(det.get(k))
            if nm: scorer=nm; break
        assists=[]
        for k in _ASSIST_KEYS:
            val=det.get(k)
            if isinstance(val, list):
                for obj in val:
                    nm=_pick_name_from_person(obj)
                    if nm: assists.append(nm)
            else:
                nm=_pick_name_from_person(val)
                if nm: assists.append(nm)
        if not scorer or not assists:
            sc2, as2 = _names_from_players_arrays(p)
            if not scorer and sc2: scorer=sc2
            if not assists and as2: assists=as2

        events.append(ScoringEvent(period, ptype, t, team, h, a, scorer or "", [s for s in assists if s]))
        if ptype!="SHOOTOUT": prev_h,prev_a=h,a
    return events

# ===== sports.ru (optional, как в daily — опустим ради краткости вывода) =====

# ===== formatting & telegram =====
def _italic(s:str)->str: return f"<i>{s}</i>"
def period_title_text(num:int, ptype:str, ot_index:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_index or 1}"
    if t=="SHOOTOUT": return "Буллиты"
    return f"Период {num}"
def _line_goal(ev:ScoringEvent)->str:
    assists=f" ({', '.join(ev.assists)})" if ev.assists else ""
    return f"{ev.home_goals}:{ev.away_goals} – {ev.time} {ev.scorer or '—'}{assists}"

def _shootout_winner_line(meta: GameMeta, events: List[ScoringEvent]) -> Optional[str]:
    so=[e for e in events if e.period_type=="SHOOTOUT"]
    if not so: return None
    winner = meta.home_tri if meta.home_score > meta.away_score else meta.away_tri
    wg=[e for e in so if e.team_for==winner and (e.scorer or "").strip()]
    if not wg: return None
    last=wg[-1]; final=f"{meta.home_score}:{meta.away_score}"
    return f"Победный буллит\n{final} - {last.scorer}"

def send_telegram_text(text:str)->None:
    token=_env_str("TELEGRAM_BOT_TOKEN","").strip()
    chat_id=_env_str("TELEGRAM_CHAT_ID","").strip()
    thread=_env_str("TELEGRAM_THREAD_ID","").strip()
    if not token or not chat_id: print("[ERR] Telegram token/chat_id not set"); return
    url=f"{TG_API}/bot{token}/sendMessage"; headers={"Content-Type":"application/json"}
    payload={"chat_id": int(chat_id) if chat_id.strip("-").isdigit() else chat_id,
             "text": text, "disable_web_page_preview": True, "disable_notification": False, "parse_mode": "HTML"}
    if thread:
        try: payload["message_thread_id"]=int(thread)
        except: pass
    if DRY_RUN: print("[DRY RUN] "+textwrap.shorten(text,200,placeholder="…")); return
    resp=requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    try: data=resp.json()
    except: data={"ok":None,"raw":resp.text}
    print(f"[DBG] TG HTTP={resp.status_code} JSON={data}")

# ===== meta fetch with fallback =====
def fetch_meta_from_summary(gamePk:int) -> Optional[GameMeta]:
    try:
        js=_get_with_retries(GAME_SUMMARY_FMT.format(gamePk=gamePk), tries=3, backoff=0.75, as_text=False)
    except Exception as e:
        if DEBUG_VERBOSE: print(f"[DBG] summary 404/err for {gamePk}: {repr(e)}")
        return None
    g=js.get("game",{}) if isinstance(js, dict) else {}
    home=_upper_str((g.get("homeTeam") or {}).get("abbrev") or (g.get("homeTeam") or {}).get("triCode"))
    away=_upper_str((g.get("awayTeam") or {}).get("abbrev") or (g.get("awayTeam") or {}).get("triCode"))
    h=_first_int((g.get("homeTeam") or {}).get("score"))
    a=_first_int((g.get("awayTeam") or {}).get("score"))
    gd=g.get("startTimeUTC") or g.get("gameDate") or ""
    try: gdt=datetime.fromisoformat(str(gd).replace("Z","+00:00"))
    except: gdt=datetime.now(timezone.utc)
    state=_upper_str(g.get("gameState") or g.get("gameStatus"))
    return GameMeta(gamePk, gdt, state, home, away, h, a)

def fetch_meta_from_schedule(gamePk:int) -> Optional[GameMeta]:
    # Ищем в окне последних 10 дней и ближайших 2 — этого достаточно для ручных запросов.
    today=datetime.utcnow().date()
    for d_off in range(-10,3):
        day=(today+timedelta(days=d_off)).isoformat()
        s=http_get_json(SCHED_FMT.format(day=day))
        games = s.get("games")
        if games is None:
            games=[]
            for w in (s.get("gameWeek") or []): games += (w.get("games") or [])
        for g in games:
            gid=_first_int(g.get("id"), g.get("gameId"), g.get("gamePk"))
            if gid!=gamePk: continue
            home=g.get("homeTeam",{}) or {}; away=g.get("awayTeam",{}) or {}
            htri=_upper_str(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
            atri=_upper_str(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
            hscr=_first_int(home.get("score")); ascr=_first_int(away.get("score"))
            gd=g.get("startTimeUTC") or g.get("startTime") or g.get("gameDate") or ""
            try: gdt=datetime.fromisoformat(str(gd).replace("Z","+00:00"))
            except: gdt=datetime.now(timezone.utc)
            state=_upper_str(g.get("gameState") or g.get("gameStatus"))
            return GameMeta(gamePk, gdt, state, htri, atri, hscr, ascr)
    return None

def fetch_meta(gamePk:int) -> GameMeta:
    m = fetch_meta_from_summary(gamePk) or fetch_meta_from_schedule(gamePk)
    if not m: raise RuntimeError(f"Meta not found for {gamePk}")
    return m

# ===== single text =====
def period_title_text(num:int, ptype:str, ot_index:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_index or 1}"
    if t=="SHOOTOUT": return "Буллиты"
    return f"Период {num}"

def build_single_match_text(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he=TEAM_EMOJI.get(meta.home_tri,""); ae=TEAM_EMOJI.get(meta.away_tri,"")
    hn=TEAM_RU.get(meta.home_tri,meta.home_tri); an=TEAM_RU.get(meta.away_tri,meta.away_tri)
    # standings опционально — могут не совпасть с днём матча, но рекорд всё равно полезен
    hrec="?"; arec="?"
    try:
        st=json.loads(_get_with_retries(f"{NHLE_BASE}/standings/now", as_text=False))
    except Exception:
        st=None
    if st:
        # маленький одноразовый маппинг, чтобы не таскать весь код от daily
        def _rec(team_abbr:str)->str:
            arr=st.get("standings") or st.get("records") or []
            for r in arr:
                ta=r.get("teamAbbrev") or (r.get("teamAbbrev") or {}).get("default")
                if _upper_str(ta)==team_abbr:
                    rec=r.get("record") or r.get("overallRecord") or {}
                    w=_first_int(rec.get("wins"), r.get("wins")); l=_first_int(rec.get("losses"),r.get("losses")); ot=_first_int(rec.get("ot"), r.get("ot"))
                    return f"{w}-{l}-{ot}"
            return "?"
        hrec=_rec(meta.home_tri); arec=_rec(meta.away_tri)

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
        title=period_title_text(pnum,ptype,ot_idx,ot_total)
        lines.append("")
        lines.append(_italic(title))
        per=groups[key]
        if ptype=="SHOOTOUT":
            win_line = _shootout_winner_line(meta, per)
            lines.append(win_line or "Буллиты не определили победителя")
        elif not per:
            lines.append("Голов не было")
        else:
            for ev in per: lines.append(_line_goal(ev))
    return "\n".join(lines).strip()

# ===== main =====
def main():
    game_pk = _env_str("GAME_PK").strip()
    game_query = _env_str("GAME_QUERY").strip()
    if not game_pk and game_query:
        gid = resolve_game_pk_from_query(
            game_query,
            _env_int("SEARCH_BACK",1),
            _env_int("SEARCH_FWD",1),
            _env_bool("REQUIRE_FINAL", True)
        )
        if gid: game_pk=str(gid)
        else:
            print(f"[ERR] GAME_QUERY not resolved: {game_query}")
            raise SystemExit(1)
    if not game_pk:
        print("[ERR] provide GAME_PK or GAME_QUERY"); raise SystemExit(1)

    gid=int(game_pk)
    meta = fetch_meta(gid)
    evs = fetch_scoring_official(gid, meta.home_tri, meta.away_tri)

    text = build_single_match_text(meta, {}, evs)
    print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
    send_telegram_text(text)
    print("OK")

if __name__=="__main__":
    main()
