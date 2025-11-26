#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Single Result Bot — per-game instant posts (no repeats)

— Постит один выбранный матч (через GAME_PK или GAME_QUERY).
— Шапка: «Команда»: счёт; рекорд (W-L-OT).
— Периоды/ОТ/буллиты — курсивом; список событий.
— Буллиты: выводим только одну строку «Победный буллит» в формате:
    Победный буллит
    <итоговый счёт> - <Имя>
— Имена: берём из official PBP (в т.ч. из players[]), где возможно подменяем на
  sports.ru (кириллица). Добавлена поддержка Юты (UTA).
— GAME_QUERY разрешается даже если порядок «HOME - AWAY» указан наоборот.

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TELEGRAM_THREAD_ID
- GAME_PK (опц.), GAME_QUERY (опц. строка вида "YYYY-MM-DD AAA - BBB" или "AAA@BBB")
- SEARCH_BACK=1, SEARCH_FWD=1, REQUIRE_FINAL=1, FORCE_POST=1
- DRY_RUN=0/1, DEBUG_VERBOSE=0/1
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
SCHED_FMT  = NHLE_BASE + "/schedule/{day}"
GAME_SUMMARY_FMT = NHLE_BASE + "/gamecenter/{gamePk}/game-summary"

# ========= ENV =========
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

# ========= RU / TEAMS =========
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
    # UTA (Юта) на sports.ru пока нестабильна
}

# ========= HTTP =========
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

# ========= Models =========
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

# ========= helpers =========
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
def _clean_parens(s: str) -> str:
    s = s.strip()
    if s.startswith("(") and s.endswith(")") and s.count("(")==1 and s.count(")")==1:
        s = s[1:-1]
    s = s.replace("((", "(").replace("))", ")")
    return s

# ========= standings / schedule =========
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

def fetch_schedule_day(day: str) -> List[dict]:
    js = http_get_json(SCHED_FMT.format(day=day))
    games = js.get("games")
    if games is None:
        games = []
        for w in js.get("gameWeek") or []:
            games += w.get("games") or []
    return games or []

def resolve_game_pk_from_query(game_query: str, search_back:int=1, search_fwd:int=1, require_final:bool=True) -> Optional[int]:
    """
    GAME_QUERY: "YYYY-MM-DD AAA - BBB" (HOME-AWAY) ИЛИ "AAA@BBB" (AWAY@HOME)
    Разрешаем и «перевёрнутый» порядок — матч ищется по множеству {AAA,BBB}.
    """
    game_query = (game_query or "").strip()
    if not game_query: return None
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

    tri_set = {home, away}
    # Окно поиска по календарю
    base_dt = datetime.fromisoformat(base_day)
    days = [(base_dt + timedelta(days=d)).date().isoformat() for d in range(-search_back, search_fwd+1)]
    for d in days:
        for g in fetch_schedule_day(d):
            st = _upper_str(g.get("gameState") or g.get("gameStatus"))
            if require_final and st not in ("FINAL","OFF"): 
                continue
            ht = _upper_str((g.get("homeTeam") or {}).get("abbrev") or (g.get("homeTeam") or {}).get("triCode") or (g.get("homeTeam") or {}).get("teamAbbrev"))
            at = _upper_str((g.get("awayTeam") or {}).get("abbrev") or (g.get("awayTeam") or {}).get("triCode") or (g.get("awayTeam") or {}).get("teamAbbrev"))
            if {ht, at} != tri_set:
                continue
            gid = _first_int(g.get("id"), g.get("gameId"), g.get("gamePk"))
            if gid:
                print(f"[DBG] Resolved GAME_PK={gid} for {game_query}")
                return gid
    print(f"[DBG] Unable to resolve GAME_PK for {game_query}")
    return None

# ========= PBP parsing =========
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

def _names_from_players_list(p: dict) -> Tuple[Optional[str], List[str]]:
    """
    Универсальный разбор p['players'] NHL-web:
    ищем Scorer/Shooter и Assist/Primary/Secondary.
    """
    scorer = None
    assists: List[str] = []
    arr = p.get("players") or []
    if isinstance(arr, list):
        for item in arr:
            if not isinstance(item, dict): 
                continue
            ptype = _upper_str(item.get("playerType") or item.get("type"))
            # Имя может лежать в разных полях:
            nm = _extract_name(item) or _extract_name(item.get("player", {})) or _extract_name(item.get("name", {}))
            if not nm:
                first = item.get("firstName", {})
                last  = item.get("lastName",  {})
                fn = _extract_name(first) or first.get("default") if isinstance(first, dict) else first
                ln = _extract_name(last) or last.get("default") if isinstance(last, dict) else last
                nm = " ".join([x for x in [fn, ln] if x])
            if not nm: 
                continue
            if ptype in ("SCORER","GOALSCORER","SHOOTER","SHOOTOUTSHOOTER"):
                scorer = nm
            elif "ASSIST" in ptype:
                assists.append(nm)
    return scorer, assists

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

        # ---- извлекаем имена
        scorer=""
        for k in _SCORER_KEYS:
            nm=_extract_name(det.get(k))
            if nm: scorer=nm; break
        assists=[]
        for k in _ASSIST_KEYS:
            nm=_extract_name(det.get(k))
            if nm: assists.append(nm)

        # fallback на players[]
        if not scorer or not assists:
            sc2, as2 = _names_from_players_list(p)
            if not scorer and sc2: scorer = sc2
            if not assists and as2: assists = as2

        # финальная чистка скобок у ассистов
        assists = [_clean_parens(x) for x in assists]

        events.append(ScoringEvent(period,ptype,t,team,h,a,scorer or "", assists))
        if ptype!="SHOOTOUT":
            prev_h,prev_a=h,a
    return events

# ========= sports.ru =========
TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")
def _extract_time(text: str)->Optional[str]:
    m=TIME_RE.search(text or ""); 
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None

def parse_sportsru_goals_html(html: str, side: str)->List[Tuple[Optional[str], Optional[str], List[str]]]:
    """
    Возвращаем (time, scorer_ru, assists_ru[]). Буллиты sports.ru почти не отдают — не рассчитываем на них.
    """
    res: List[Tuple[Optional[str], Optional[str], List[str]]] = []
    if HAS_BS:
        soup=BS(html,"lxml" if "lxml" in globals() else "html.parser")
        ul=soup.select_one(f"ul.match-summary__goals-list--{side}") or soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors=[a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru=anchors[0] if anchors else None
                assists_ru=[_clean_parens(a) for a in (anchors[1:] if len(anchors)>1 else [])]
                raw=li.get_text(" ", strip=True); time_ru=_extract_time(raw)
                res.append((time_ru, scorer_ru, assists_ru))
    return res

def fetch_sportsru_goals(home_tri:str, away_tri:str):
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

def merge_official_with_sportsru(evs: List[ScoringEvent], sru_home, sru_away, home_tri:str, away_tri:str)->List[ScoringEvent]:
    """
    Идём по официальным событиям по порядку, подставляем кириллицу по индексу команды.
    """
    h_i=a_i=0; out=[]
    for ev in evs:
        if ev.period_type=="SHOOTOUT":
            out.append(ev); continue  # буллиты не пытаемся биндить к sports.ru
        if ev.team_for==home_tri and h_i<len(sru_home):
            _, scorer_ru, assists_ru = sru_home[h_i]; h_i+=1
            ev.scorer = scorer_ru or ev.scorer or ""
            ev.assists = assists_ru or ev.assists
        elif ev.team_for==away_tri and a_i<len(sru_away):
            _, scorer_ru, assists_ru = sru_away[a_i]; a_i+=1
            ev.scorer = scorer_ru or ev.scorer or ""
            ev.assists = assists_ru or ev.assists
        out.append(ev)
    return out

# ========= formatting =========
def _italic(s:str)->str: return f"<i>{s}</i>"
def period_title_text(num:int, ptype:str, ot_index:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_index or 1}"
    if t=="SHOOTOUT": return "Буллиты"
    return f"Период {num}"
def _line_goal(ev:ScoringEvent)->str:
    score=f"{ev.home_goals}:{ev.away_goals}"
    who=ev.scorer or "—"
    assists=f" ({', '.join(ev.assists)})" if ev.assists else ""
    line = f"{score} – {ev.time} {who}{assists}"
    return line.replace("((", "(").replace("))", ")")

def _shootout_winner_line(meta: GameMeta, events: List[ScoringEvent]) -> Optional[str]:
    so_goals = [e for e in events if e.period_type=="SHOOTOUT"]
    if not so_goals:
        return None
    winner = meta.home_tri if meta.home_score > meta.away_score else meta.away_tri
    # последний результативный удар победившей команды
    winner_goals = [e for e in so_goals if e.team_for==winner and (e.scorer or "").strip()]
    if not winner_goals:
        return None
    last = winner_goals[-1]
    final = f"{meta.home_score}:{meta.away_score}"
    return f"Победный буллит\n{final} - {last.scorer}"

def build_single_match_text(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
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
        title=period_title_text(pnum,ptype,ot_idx,ot_total)
        lines.append("")              # пустая строка
        lines.append(_italic(title))  # курсивом
        per=groups[key]
        if ptype=="SHOOTOUT":
            win_line = _shootout_winner_line(meta, per)
            if win_line: lines.append(win_line)
            else: lines.append("Буллиты не определили победителя")  # на всякий случай
        elif not per:
            lines.append("Голов не было")
        else:
            for ev in per: lines.append(_line_goal(ev))
    return "\n".join(lines).strip()

# ========= state & telegram =========
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

# ========= main =========
def fetch_meta(gamePk:int)->GameMeta:
    js = http_get_json(GAME_SUMMARY_FMT.format(gamePk=gamePk))
    g = js.get("game", {}) if isinstance(js, dict) else {}
    home = _upper_str(((g.get("homeTeam") or {}).get("abbrev")) or ((g.get("homeTeam") or {}).get("triCode")))
    away = _upper_str(((g.get("awayTeam") or {}).get("abbrev")) or ((g.get("awayTeam") or {}).get("triCode")))
    hsc = _first_int((g.get("homeTeam") or {}).get("score"))
    asc = _first_int((g.get("awayTeam") or {}).get("score"))
    gd  = g.get("startTimeUTC") or g.get("gameDate") or ""
    try: gdt = datetime.fromisoformat(str(gd).replace("Z","+00:00"))
    except: gdt = datetime.now(timezone.utc)
    state = _upper_str(g.get("gameState") or g.get("gameStatus"))
    return GameMeta(gamePk, gdt, state, home, away, hsc, asc)

def main():
    game_pk = _env_str("GAME_PK").strip()
    game_query = _env_str("GAME_QUERY").strip()
    if not game_pk and game_query:
        sb = _env_int("SEARCH_BACK", 1); sf = _env_int("SEARCH_FWD", 1)
        rf = _env_bool("REQUIRE_FINAL", True)
        gid = resolve_game_pk_from_query(game_query, sb, sf, rf)
        if gid: game_pk = str(gid)
        else:
            print(f"[ERR] GAME_QUERY not resolved: {game_query}")
            raise SystemExit(1)

    if not game_pk:
        print("[ERR] provide GAME_PK or GAME_QUERY")
        raise SystemExit(1)

    gid = int(game_pk)
    meta = fetch_meta(gid)
    standings = fetch_standings_map()
    evs = fetch_scoring_official(gid, meta.home_tri, meta.away_tri)
    # пробуем подменить на кириллицу (если есть)
    sru_h, sru_a, _ = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
    merged = merge_official_with_sportsru(evs, sru_h, sru_a, meta.home_tri, meta.away_tri)

    text = build_single_match_text(meta, standings, merged)
    print("[DBG] Single match preview:\n"+text[:300].replace("\n","¶")+"…")
    send_telegram_text(text)
    print("OK")

if __name__=="__main__":
    main()
