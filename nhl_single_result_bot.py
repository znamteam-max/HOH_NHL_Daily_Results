#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — per-game instant posts (no repeats)

— Можно запускать по расписанию (вчера+сегодня, FINAL и ещё не отправлялись)
  или вручную на конкретный матч.
— Ручной запуск:
    * GAME_PK="2025020349"
    * GAME_QUERY="YYYY-MM-DD NSH - DET"  (алиасы: NAS→NSH, LA→LAK, TB→TBL, MON→MTL, ARZ→ARI)
— Формат: эмодзи вне «шапки», внутри — жирным «Команда»: счёт; рекорд в скобках (W-L-OT).
— Заголовки периодов/ОТ/буллитов — курсивом; «Овертайм» без №, если он один.
— Перед каждым заголовком периода — пустая строка.
— Победный буллит: отдельный блок в конце:
    Победный буллит
    <итоговый счёт> – <Имя>

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TELEGRAM_THREAD_ID
- STATE_PATH="state/posted_games.json"
- GAME_PK (опц.)
- GAME_QUERY (опц.) — приоритет над автоматикой
- DRY_RUN=0/1
- DEBUG_VERBOSE=0/1
"""

from __future__ import annotations
import os, re, json, time, textwrap, pathlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date

import requests

try:
    from bs4 import BeautifulSoup as BS  # type: ignore
    HAS_BS = True
except Exception:
    HAS_BS = False

TG_API     = "https://api.telegram.org"
NHLE_BASE  = "https://api-web.nhle.com/v1"
PBP_FMT    = NHLE_BASE + "/gamecenter/{gamePk}/play-by-play"

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
GAME_PK_ENV = _env_str("GAME_PK","").strip()
GAME_QUERY  = _env_str("GAME_QUERY","").strip()

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

# --- Models ---
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

# --- Helpers ---
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

# --- Standings / schedule / PBP / sports.ru ---
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

def list_games_yesterday_today_final()->List[GameMeta]:
    now_utc=datetime.now(timezone.utc)
    dates=[(now_utc - timedelta(days=1)).date().isoformat(), now_utc.date().isoformat()]
    metas:Dict[int,GameMeta]={}
    for day in dates:
        url=f"{NHLE_BASE}/schedule/{day}"; s=http_get_json(url)
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

# Aliases for convenience
ALIAS = {"NAS":"NSH","LA":"LAK","TB":"TBL","MON":"MTL","ARZ":"ARI"}
def _norm_tri(s: str)->str:
    s = _upper_str(s.strip())
    return ALIAS.get(s, s)

def resolve_game_pk_by_query(q: str) -> Optional[int]:
    """
    Формат: 'YYYY-MM-DD NSH - DET' (home - away).
    Ищем в окне [date-1 .. date+1] по schedule.
    """
    try:
        day_str, teams = q.strip().split(" ", 1)
        d = date.fromisoformat(day_str)
        parts = re.split(r"\s*-\s*", teams)
        if len(parts) != 2: return None
        home = _norm_tri(parts[0])
        away = _norm_tri(parts[1])
    except Exception:
        return None

    def fetch_sched(d: date) -> List[dict]:
        js = http_get_json(f"{NHLE_BASE}/schedule/{d.isoformat()}")
        out=[]
        for w in js.get("gameWeek",[]) or []:
            out.extend(w.get("games",[]) or [])
        return out

    pool=[]
    for delta in (-1,0,1):
        pool.extend(fetch_sched(d + timedelta(days=delta)))

    for g in pool:
        gid=_first_int(g.get("id"),g.get("gameId"),g.get("gamePk"))
        if gid==0: continue
        ht=_upper_str((g.get("homeTeam") or {}).get("abbrev") or (g.get("homeTeam") or {}).get("triCode") or (g.get("homeTeam") or {}).get("teamAbbrev"))
        at=_upper_str((g.get("awayTeam") or {}).get("abbrev") or (g.get("awayTeam") or {}).get("triCode") or (g.get("awayTeam") or {}).get("teamAbbrev"))
        st=_upper_str(g.get("gameState") or g.get("gameStatus"))
        if ht==home and at==away and st in ("FINAL","OFF"):
            print(f"[DBG] Resolved GAME_PK={gid} for {q}")
            return gid
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

def _name_from_player_obj(o: dict) -> Optional[str]:
    for k in ("playerName","name","fullName","firstLastName","lastFirstName","default","shortName"):
        v = o.get(k)
        if isinstance(v, str) and v.strip(): return v.strip()
    pl = o.get("player") or {}
    for k in ("fullName","firstLastName","lastFirstName","name","default","shortName"):
        v = pl.get(k)
        if isinstance(v, str) and v.strip(): return v.strip()
    f = (o.get("firstName") or "").strip()
    l = (o.get("lastName") or "").strip()
    if f or l: return (f + " " + l).strip()
    return None

_PAREN_CLEAN = re.compile(r'^[\(\s]+|[\)\s]+$')
def _clean_paren(s: str) -> str:
    return _PAREN_CLEAN.sub("", s or "").strip()

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
            if nm: scorer=nm; break

        assists=[]
        for k in _ASSIST_KEYS:
            nm=_extract_name(det.get(k))
            if nm: assists.append(_clean_paren(nm))

        # Фоллбэк players[]
        if not scorer or not assists:
            players = p.get("players") or []
            assists_by_order: Dict[int, str] = {}
            for pl in players:
                ptype_pl = (pl.get("playerType") or "").strip().lower()
                nm = _name_from_player_obj(pl) or ""
                if not nm: continue
                if ptype_pl in ("scorer","shooter") and not scorer:
                    scorer = nm
                elif ptype_pl == "assist":
                    order = _first_int(pl.get("assistOrder"), pl.get("orderNumber"), pl.get("sequence"))
                    if order == 0: order = 99
                    assists_by_order[order] = nm
            if not assists and assists_by_order:
                assists = [assists_by_order[k] for k in sorted(assists_by_order.keys())]

        assists = [_clean_paren(x) for x in assists if _clean_paren(x)]
        events.append(ScoringEvent(period,ptype,t,team,h,a,scorer or "—",assists))
        if ptype!="SHOOTOUT":
            prev_h,prev_a=h,a
    return events

TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")
def _extract_time(text: str)->Optional[str]:
    m=TIME_RE.search(text or "")
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None

def parse_sportsru_goals_html(html: str, side: str)->List[SRUGoal]:
    res: List[SRUGoal] = []
    if HAS_BS:
        soup=BS(html,"lxml" if "lxml" in globals() else "html.parser")
        ul=soup.select_one(f"ul.match-summary__goals-list--{side}") or soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
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
            print(f"[DBG] sports.ru fetch fail {url}: {repr(e)}"); continue
        home_side="home" if order[0]==hs else "away"; away_side="away" if home_side=="home" else "home"
        h=parse_sportsru_goals_html(html, home_side); a=parse_sportsru_goals_html(html, away_side)
        if h or a:
            return h,a,url
    return [],[], ""

def merge_official_with_sportsru(evs: List[ScoringEvent], sru_home: List[SRUGoal], sru_away: List[SRUGoal], home_tri:str, away_tri:str)->List[ScoringEvent]:
    def pick_by_time(side_list: List[SRUGoal], t: str, used: set) -> Optional[SRUGoal]:
        for i, g in enumerate(side_list):
            if i in used: continue
            if g.time and g.time == t:
                used.add(i); return g
        if t and "." in t:
            m = t.split(".")[0]
            for i, g in enumerate(side_list):
                if i in used: continue
                if g.time and g.time.split(".")[0] == m:
                    used.add(i); return g
        return None

    used_h, used_a = set(), set()
    out=[]
    for ev in evs:
        if ev.team_for==home_tri:
            g=pick_by_time(sru_home, ev.time, used_h)
            if g:
                ev.scorer=g.scorer_ru or ev.scorer or ""
                ev.assists=g.assists_ru or ev.assists
        elif ev.team_for==away_tri:
            g=pick_by_time(sru_away, ev.time, used_a)
            if g:
                ev.scorer=g.scorer_ru or ev.scorer or ""
                ev.assists=g.assists_ru or ev.assists
        ev.assists=[_clean_paren(x) for x in (ev.assists or []) if _clean_paren(x)]
        out.append(ev)
    return out

def _italic(s:str)->str: return f"<i>{s}</i>"
def period_title_text(num:int, ptype:str, ot_index:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_index or 1}"
    if t=="SHOOTOUT": return "Буллиты"
    return f"Период {num}"
def line_goal(ev:ScoringEvent)->str:
    score=f"{ev.home_goals}:{ev.away_goals}"; who=ev.scorer or "—"; assists=f" ({', '.join(ev.assists)})" if ev.assists else ""
    return f"{score} – {ev.time} {who}{assists}"

def build_single_match_text(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he=TEAM_EMOJI.get(meta.home_tri,""); ae=TEAM_EMOJI.get(meta.away_tri,"")
    hn=TEAM_RU.get(meta.home_tri,meta.home_tri); an=TEAM_RU.get(meta.away_tri,meta.away_tri)

    # Вне «шапки» — только эмодзи (без дубля названий)
    visible = f"{he}\n{ae}"

    hrec=standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec=standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head=f"<b>«{hn}»: {meta.home_score}</b> ({hrec})\n<b>«{an}»: {meta.away_score}</b> ({arec})"

    groups:Dict[Tuple[int,str],List[ScoringEvent]]={}
    for ev in events: groups.setdefault((ev.period,ev.period_type),[]).append(ev)
    for pnum in (1,2,3):
        if (pnum,"REGULAR") not in groups: groups[(pnum,"REGULAR")]=[]
    ot_keys=sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total=len(ot_keys); ot_order={k:i+1 for i,k in enumerate(ot_keys)}

    # Победный буллит
    so_goals = [ev for ev in events if (ev.period_type or "").upper()=="SHOOTOUT" and ev.scorer and ev.scorer != "—"]
    winner = so_goals[-1].scorer if so_goals else None

    lines=[head]
    sort_key=lambda x:(x[0], 0 if (x[1] or "").upper()=="REGULAR" else 1 if (x[1] or "").upper()=="OVERTIME" else 2)
    for key in sorted(groups.keys(), key=sort_key):
        pnum,ptype=key; ot_idx=ot_order.get(key)
        title=period_title_text(pnum,ptype,ot_idx,ot_total)
        lines.append("")              # пустая строка
        lines.append(_italic(title))  # курсивом
        per=groups[key]
        if not per: lines.append("Голов не было")
        else:
            for ev in per: lines.append(line_goal(ev))

    if winner:
        lines.append("")
        lines.append("Победный буллит")
        lines.append(f"{meta.home_score}:{meta.away_score} – {winner}")

    return (visible + "\n\n" + "\n".join(lines)).strip()

# --- State & Telegram ---
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

def main():
    # Ручной режим: по GAME_QUERY → GAME_PK, иначе GAME_PK_ENV, иначе авто-постинг
    gid: Optional[int] = None
    if GAME_QUERY:
        gid = resolve_game_pk_by_query(GAME_QUERY)
        if not gid:
            print(f"[ERR] GAME_QUERY not resolved: {GAME_QUERY}")
            return
    elif GAME_PK_ENV:
        try:
            gid = int(GAME_PK_ENV)
        except Exception:
            print("[ERR] GAME_PK is not a number"); return

    if gid:
        # Единичный матч по gid
        meta = fetch_game_meta(gid)
        if not meta:
            print(f"[ERR] game meta not found for {gid}")
            return
        standings = fetch_standings_map()
        evs=fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)
        sru_home, sru_away, _ = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        merged=merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)
        text=build_single_match_text(meta, standings, merged)
        print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
        send_telegram_text(text)
        print("OK (posted 1 manual)")
        return

    # Автоматический режим: вчера+сегодня (UTC), без повторов
    state=load_state(STATE_PATH)
    posted:Dict[str,bool]=state.get("posted",{}) or {}
    games=list_games_yesterday_today_final()
    if DEBUG_VERBOSE: print(f"[DBG] FINAL games (yesterday+today): {len(games)}")
    if not games:
        print("OK (no FINAL games)"); return
    standings=fetch_standings_map()
    new_posts=0
    for meta in games:
        key=str(meta.gamePk)
        if posted.get(key):
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

def fetch_game_meta(gamePk:int) -> Optional[GameMeta]:
    # game-summary нужен для рекордов счёта и команд
    url_stats = f"https://statsapi.web.nhl.com/api/v1/game/{gamePk}/feed/live"
    url_web   = f"{NHLE_BASE}/gamecenter/{gamePk}/game-summary"
    # Пробуем сначала web (быстро падает 404 когда игра не найдена), затем stats
    try:
        js = http_get_json(url_web)
    except Exception:
        js = None
    if not js:
        try:
            data = http_get_json(url_stats)
        except Exception as e:
            print(f"[ERR] fetch_game_meta: {repr(e)}"); return None
        g  = data.get("gameData",{}) or {}
        st = data.get("liveData",{}).get("linescore",{}) or {}
        teams = g.get("teams",{}) or {}
        ht = _upper_str(((teams.get("home") or {}).get("abbreviation")) or "")
        at = _upper_str(((teams.get("away") or {}).get("abbreviation")) or "")
        hscore = _first_int((st.get("teams") or {}).get("home",{}).get("goals"))
        ascore = _first_int((st.get("teams") or {}).get("away",{}).get("goals"))
        state  = _upper_str((data.get("gameData") or {}).get("status",{}).get("abstractGameState"))
        dt_utc = datetime.now(timezone.utc)
        return GameMeta(gamePk, dt_utc, state, ht, at, hscore, ascore)

    # web-summary структура
    try:
        game = js.get("summary",{}) or js
        ht = _upper_str((game.get("homeTeam") or {}).get("abbrev") or (game.get("homeTeam") or {}).get("triCode") or (game.get("homeTeam") or {}).get("teamAbbrev"))
        at = _upper_str((game.get("awayTeam") or {}).get("abbrev") or (game.get("awayTeam") or {}).get("triCode") or (game.get("awayTeam") or {}).get("teamAbbrev"))
        hscore = _first_int((game.get("homeTeam") or {}).get("score"))
        ascore = _first_int((game.get("awayTeam") or {}).get("score"))
        state = _upper_str(game.get("gameState") or game.get("gameStatus") or "FINAL")
        start = (game.get("gameDate") or game.get("startTimeUTC") or "")
        dt_utc = datetime.fromisoformat(str(start).replace("Z","+00:00")) if start else datetime.now(timezone.utc)
        return GameMeta(gamePk, dt_utc, state, ht, at, hscore, ascore)
    except Exception as e:
        print(f"[ERR] parse web-summary: {repr(e)}"); return None

if __name__=="__main__":
    main()
