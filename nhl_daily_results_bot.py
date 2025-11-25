#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, re, json, textwrap
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

# API endpoints
STATS_BASE = "https://statsapi.web.nhl.com/api/v1"
NHLE_BASE  = "https://api-web.nhle.com/v1"
TG_API     = "https://api.telegram.org"

# --------- RU naming / emojis ----------
TEAM_RU = {
    "ANA":"Анахайм","ARI":"Аризона","BOS":"Бостон","BUF":"Баффало","CGY":"Калгари","CAR":"Каролина",
    "CHI":"Чикаго","COL":"Колорадо","CBJ":"Коламбус","DAL":"Даллас","DET":"Детройт","EDM":"Эдмонтон",
    "FLA":"Флорида","LAK":"Лос-Анджелес","MIN":"Миннесота","MTL":"Монреаль","NSH":"Нэшвилл",
    "NJD":"Нью-Джерси","NYI":"Айлендерс","NYR":"Рейнджерс","OTT":"Оттава","PHI":"Филадельфия",
    "PIT":"Питтсбург","SJS":"Сан-Хосе","SEA":"Сиэтл","STL":"Сент-Луис","TBL":"Тампа-Бэй",
    "TOR":"Торонто","VAN":"Ванкувер","VGK":"Вегас","WSH":"Вашингтон","WPG":"Виннипег","UTA":"UTA",
}
TEAM_EMOJI = {
    "ANA":"🦆","ARI":"🦂","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️","CBJ":"💣",
    "DAL":"⭐️","DET":"🛡️","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦","NSH":"🐯",
    "NJD":"😈","NYI":"🏝️","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈","SEA":"🦑","STL":"🎵",
    "TBL":"⚡","TOR":"🍁","VAN":"🐳","VGK":"🎰","WSH":"🦅","WPG":"✈️","UTA":"🟪",
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
    "WPG":"winnipeg-jets","UTA":"uta",
}

# --------- HTTP helpers ----------
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.8",
}
def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(UA_HEADERS)
    return s
SESSION = _build_session()

def http_get_json(url: str, timeout: int = 30):
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return json.loads(r.text or "{}")

def http_get_text(url: str, timeout: int = 30) -> str:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def _un(x):
    if isinstance(x, dict):
        return x.get("default") or next(iter(x.values()), None)
    return x

# --------- Data classes ----------
@dataclass
class TeamRecord:
    wins:int; losses:int; ot:int
    def as_str(self)->str:  # без очков
        return f"{self.wins}-{self.losses}-{self.ot}"

@dataclass
class GameMeta:
    gamePk:int
    gameDateUTC:datetime
    state:str
    home_tri:str
    away_tri:str
    home_score:int
    away_score:int

@dataclass
class ScoringEvent:
    period:int
    period_type:str  # REGULAR/OVERTIME/SHOOTOUT
    time:str         # "MM.SS"
    team_for:str     # triCode
    home_goals:int
    away_goals:int
    scorer:str
    assists:List[str]=field(default_factory=list)

@dataclass
class SRUGoal:
    time:Optional[str]
    scorer_ru:Optional[str]
    assists_ru:List[str]

# --------- tiny utils ----------
def _upper_str(v: Any)->str:
    return str(v or "").upper()
def _strip_wrapping_parens(s: str)->str:
    ss=s.strip()
    while ss.startswith("(") and ss.endswith(")") and len(ss) >= 2:
        ss = ss[1:-1].strip()
    return ss
def _italic(s:str)->str:
    return f"<i>{s}</i>"

# --------- standings ----------
def fetch_standings_map()->Dict[str,TeamRecord]:
    url=f"{NHLE_BASE}/standings/now"
    data=http_get_json(url)
    teams:Dict[str,TeamRecord]={}
    nodes=[]
    if isinstance(data,dict):
        nodes = data.get("standings") or data.get("records") or []
        if isinstance(nodes,dict):
            nodes = nodes.get("overallRecords", []) or []
    elif isinstance(data,list):
        nodes=data
    for r in nodes:
        abbr=""
        ta=r.get("teamAbbrev")
        if isinstance(ta,str): abbr=ta.upper()
        elif isinstance(ta,dict): abbr=(ta.get("default") or ta.get("tricode") or "").upper()
        if not abbr:
            abbr=(r.get("teamAbbrevTricode") or r.get("teamTriCode") or "").upper()
        rec=r.get("record") or r.get("overallRecord") or {}
        wins=int(rec.get("wins") or rec.get("gamesPlayedWins") or 0)
        losses=int(rec.get("losses") or rec.get("gamesPlayedLosses") or 0)
        ot=int(rec.get("ot") or rec.get("overtimeLosses") or 0)
        if abbr: teams[abbr]=TeamRecord(wins,losses,ot)
    return teams

# --------- schedule (single) ----------
def fetch_game_meta(gamePk:int)->GameMeta:
    url=f"{STATS_BASE}/game/{gamePk}/feed/live"
    try:
        data=http_get_json(url)
        gd = data.get("gameData",{}) or {}
        teams = gd.get("teams",{}) or {}
        live = data.get("liveData",{}) or {}
        linescore = live.get("linescore",{}) or {}
        home = linescore.get("teams",{}).get("home",{}) or {}
        away = linescore.get("teams",{}).get("away",{}) or {}
        home_tri = (teams.get("home",{}).get("abbreviation") or home.get("team",{}).get("triCode") or "").upper()
        away_tri = (teams.get("away",{}).get("abbreviation") or away.get("team",{}).get("triCode") or "").upper()
        status  = (gd.get("status",{}).get("detailedState") or "").upper()
        dt = gd.get("datetime",{}).get("dateTime") or ""
        try:
            gdt = datetime.fromisoformat(dt.replace("Z","+00:00"))
        except Exception:
            gdt = datetime.now(timezone.utc)
        final_home = int(home.get("goals") or 0)
        final_away = int(away.get("goals") or 0)
        return GameMeta(
            gamePk=gamePk, gameDateUTC=gdt, state=status,
            home_tri=home_tri, away_tri=away_tri,
            home_score=final_home, away_score=final_away
        )
    except RequestException:
        # fallback на gamecenter
        url2=f"{NHLE_BASE}/gamecenter/{gamePk}/boxscore"
        bx=http_get_json(url2)  # достаточно, чтобы вытащить triCodes и фин.счёт
        home=bx.get("homeTeam") or {}
        away=bx.get("awayTeam") or {}
        home_tri=(home.get("abbrev") or home.get("triCode") or "").upper()
        away_tri=(away.get("abbrev") or away.get("triCode") or "").upper()
        final_home=int(bx.get("homeTeam",{}).get("score") or 0)
        final_away=int(bx.get("awayTeam",{}).get("score") or 0)
        return GameMeta(
            gamePk=gamePk, gameDateUTC=datetime.now(timezone.utc), state="FINAL",
            home_tri=home_tri, away_tri=away_tri,
            home_score=final_home, away_score=final_away
        )

# --------- official PBP (primary) + FALLBACK ----------
def fetch_scoring_from_gamecenter(gamePk:int, home_tri:str, away_tri:str)->List[ScoringEvent]:
    url=f"{NHLE_BASE}/gamecenter/{gamePk}/play-by-play"
    data=http_get_json(url)
    plays=data.get("plays") or []
    out:List[ScoringEvent]=[]
    h=a=0
    for p in plays:
        tcode=(p.get("typeCode") or p.get("typeDescKey") or "").lower()
        if tcode not in ("goal","shootoutgoal"):
            continue
        pdesc=p.get("periodDescriptor") or {}
        period=int(pdesc.get("number") or p.get("period") or 0)
        ptype=(pdesc.get("periodType") or p.get("periodType") or "REGULAR").upper()
        if tcode=="shootoutgoal": ptype="SHOOTOUT"
        t=(p.get("timeInPeriod") or "00:00").replace(":", ".")
        team = (_un(p.get("teamAbbrev")) or _un(p.get("details",{}).get("eventOwnerTeamAbbrev")) or "").upper()
        det=p.get("details") or {}
        try:
            if det.get("homeScore") is not None: h=int(det["homeScore"])
            if det.get("awayScore") is not None: a=int(det["awayScore"])
        except Exception: pass
        scorer=_un(det.get("scoringPlayerName")) or _un(det.get("scorerName")) or ""
        assists=[]
        for k in ("assist1PlayerName","assist2PlayerName","assist3PlayerName"):
            nm=_un(det.get(k))
            if nm: assists.append(_strip_wrapping_parens(nm))
        out.append(ScoringEvent(period,ptype,t,team,h,a,scorer,assists))
    print(f"[DBG] PBP (fallback) goals parsed: {len(out)} for game {gamePk}")
    return out

def fetch_scoring_official(gamePk:int, home_tri:str, away_tri:str)->List[ScoringEvent]:
    url=f"{STATS_BASE}/game/{gamePk}/feed/live"
    try:
        data=http_get_json(url)
        plays=((data.get("liveData",{}) or {}).get("plays",{}) or {})
        allPlays=plays.get("allPlays",[]) or []
        idxs=plays.get("scoringPlays",[]) or []
        out:List[ScoringEvent]=[]
        h=a=0
        for i in idxs:
            if not (0<=i<len(allPlays)): continue
            p=allPlays[i] or {}
            res=p.get("result",{}) or {}
            if _upper_str(res.get("eventTypeId"))!="GOAL": continue
            about=p.get("about",{}) or {}
            period=int(about.get("period") or 0)
            ptype=_upper_str(about.get("periodType") or "REGULAR")
            t=str(about.get("periodTime") or "00:00").replace(":",".")
            team=((p.get("team",{}) or {}).get("triCode") or "").upper()
            goals=about.get("goals",{}) or {}
            if isinstance(goals.get("home"),int) and isinstance(goals.get("away"),int):
                h,a=goals["home"],goals["away"]
            else:
                if team==home_tri: h+=1
                elif team==away_tri: a+=1
            scorer=""; assists:List[str]=[]
            for comp in (p.get("players") or []):
                role=(comp.get("playerType") or "").lower()
                name=((comp.get("player") or {}).get("fullName") or "").strip()
                if role=="scorer" and name: scorer=name
                elif role=="assist" and name: assists.append(name)
            assists=[_strip_wrapping_parens(a) for a in assists if a.strip()]
            out.append(ScoringEvent(period,ptype,t,team,h,a,scorer,assists))
        if not out:
            print("[WARN] statsapi returned no scoring events; falling back to gamecenter")
            return fetch_scoring_from_gamecenter(gamePk, home_tri, away_tri)
        print(f"[DBG] PBP goals parsed: {len(out)} for game {gamePk}")
        return out
    except RequestException as e:
        print(f"[WARN] statsapi error {repr(e)}; falling back to gamecenter")
        return fetch_scoring_from_gamecenter(gamePk, home_tri, away_tri)

# --------- sports.ru parsing (optional RU names) ----------
TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")
def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or "")
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None

def parse_sportsru_goals_html(html: str, side: str) -> List[SRUGoal]:
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except Exception:
        BeautifulSoup = None
    results: List[SRUGoal] = []
    if BeautifulSoup:
        soup = BeautifulSoup(html, "lxml" if "lxml" in globals() else "html.parser")
        ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or \
             soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors=[a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru=anchors[0] if anchors else None
                assists_ru=anchors[1:] if len(anchors)>1 else []
                raw_text=li.get_text(" ", strip=True)
                time_ru=_extract_time(raw_text)
                results.append(SRUGoal(time_ru, scorer_ru, assists_ru))
        return results
    # regex fallback
    ul_pat=re.compile(
        r'<ul[^>]*class="[^"]*match-summary__goals-list[^"]*--%s[^"]*"[^>]*>(.*?)</ul>'%side,
        re.S|re.I
    )
    li_pat=re.compile(r"<li\b[^>]*>(.*?)</li>", re.S|re.I)
    a_pat=re.compile(r"<a\b[^>]*>(.*?)</a>", re.S|re.I)
    ul_m=ul_pat.search(html)
    if not ul_m: return results
    ul_html=ul_m.group(1)
    for li_html in li_pat.findall(ul_html):
        text=re.sub(r"<[^>]+>"," ", li_html)
        time_ru=_extract_time(text)
        names=[re.sub(r"\s+"," ", re.sub(r"<[^>]+>","", m)).strip()
               for m in a_pat.findall(li_html)]
        scorer_ru=names[0] if names else None
        assists_ru=names[1:] if len(names)>1 else []
        results.append(SRUGoal(time_ru, scorer_ru, assists_ru))
    return results

def fetch_sportsru_goals(home_tri:str, away_tri:str)->Tuple[List[SRUGoal], List[SRUGoal], str]:
    hs=SPORTSRU_SLUG.get(home_tri); as_=SPORTSRU_SLUG.get(away_tri)
    if not hs or not as_: return [],[], ""
    for order in [(hs,as_),(as_,hs)]:
        url=f"https://www.sports.ru/hockey/match/{order[0]}-vs-{order[1]}/"
        try:
            html=http_get_text(url, timeout=20)
        except Exception:
            continue
        home_side = "home" if order[0]==hs else "away"
        away_side = "away" if home_side=="home" else "home"
        hg=parse_sportsru_goals_html(html, home_side)
        ag=parse_sportsru_goals_html(html, away_side)
        if hg or ag: return hg, ag, url
    return [],[], ""

# --------- merge ----------
def merge_official_with_sportsru(evs:List[ScoringEvent], sru_home:List[SRUGoal], sru_away:List[SRUGoal],
                                 home_tri:str, away_tri:str)->List[ScoringEvent]:
    hi=0; ai=0; out=[]
    for ev in evs:
        if ev.team_for==home_tri and hi<len(sru_home):
            g=sru_home[hi]; hi+=1
            if g.scorer_ru: ev.scorer=g.scorer_ru
            if g.assists_ru: ev.assists=[_strip_wrapping_parens(a) for a in g.assists_ru if a.strip()]
        elif ev.team_for==away_tri and ai<len(sru_away):
            g=sru_away[ai]; ai+=1
            if g.scorer_ru: ev.scorer=g.scorer_ru
            if g.assists_ru: ev.assists=[_strip_wrapping_parens(a) for a in g.assists_ru if a.strip()]
        out.append(ev)
    return out

# --------- formatting ----------
def line_goal(ev: ScoringEvent)->str:
    score=f"{ev.home_goals}:{ev.away_goals}"
    assists = [a for a in ev.assists if a]
    txt = f" ({', '.join(assists)})" if assists else ""
    who = ev.scorer or "—"
    return f"{score} – {ev.time} {who}{txt}"

def build_single_match_text(meta:GameMeta, standings:Dict[str,TeamRecord], events:List[ScoringEvent])->str:
    he=TEAM_EMOJI.get(meta.home_tri,""); ae=TEAM_EMOJI.get(meta.away_tri,"")
    hn=TEAM_RU.get(meta.home_tri,meta.home_tri); an=TEAM_RU.get(meta.away_tri,meta.away_tri)
    hrec=standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec=standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head=f"{he} <b>«{hn}»: {meta.home_score}</b> ({hrec})\n{ae} <b>«{an}»: {meta.away_score}</b> ({arec})"

    groups:Dict[Tuple[int,str],List[ScoringEvent]]={}
    for ev in events: groups.setdefault((ev.period,ev.period_type),[]).append(ev)

    # ensure 1..3 present
    for p in (1,2,3):
        groups.setdefault((p,"REGULAR"), [])

    # overtime order + count
    ot_keys=sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total=len(ot_keys); ot_order={k:i+1 for i,k in enumerate(ot_keys)}

    lines=[head]
    def sort_key(k):
        t=(k[1] or "").upper()
        return (k[0], 0 if t=="REGULAR" else 1 if t=="OVERTIME" else 2)

    for key in sorted(groups.keys(), key=sort_key):
        pnum,ptype=key
        t=(ptype or "").upper()
        lines.append("")
        if t=="REGULAR":
            lines.append(_italic(f"{pnum}-й период"))
            per=groups[key]
            if not per: lines.append("Голов не было")
            else:
                for ev in per: lines.append(line_goal(ev))
        elif t=="OVERTIME":
            title = "Овертайм" if ot_total<=1 else f"Овертайм №{ot_order.get(key,1)}"
            lines.append(_italic(title))
            per=groups[key]
            if not per: lines.append("Голов не было")
            else:
                for ev in per: lines.append(line_goal(ev))
        else:  # SHOOTOUT
            per=groups[key]
            lines.append(_italic("Буллиты"))
            if not per:
                lines.append("Голов не было")
            else:
                # Победный буллит — последний забитый; показываем итоговый счёт матча
                last = per[-1]
                who = last.scorer or "—"
                final_score = f"{meta.home_score}:{meta.away_score}"
                lines.append("Победный буллит")
                lines.append(f"{final_score} - {who}")

    return "\n".join(lines).strip()

# --------- Telegram ----------
def send_telegram_text(text: str) -> None:
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat_id=os.getenv("TELEGRAM_CHAT_ID","").strip()
    thread=os.getenv("TELEGRAM_THREAD_ID","").strip()
    if not token or not chat_id:
        print("[ERR] Telegram token/chat_id not set"); return
    url=f"{TG_API}/bot{token}/sendMessage"
    headers={"Content-Type":"application/json"}
    payload={
        "chat_id": int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "disable_notification": False,
    }
    if thread:
        try: payload["message_thread_id"]=int(thread)
        except: pass
    resp=requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    try: data=resp.json()
    except: data={"ok":None,"raw":resp.text}
    print(f"[DBG] TG HTTP={resp.status_code} JSON={data}")
    if resp.status_code!=200 or not data.get("ok",False):
        print(f"[ERR] sendMessage failed: {data.get('error_code')} {data.get('description')}")

# --------- main (single by gamePk env) ----------
def main():
    gp=os.getenv("GAME_PK","").strip()
    if not gp or not gp.isdigit():
        print("[ERR] provide GAME_PK in env"); return
    gamePk=int(gp)
    meta=fetch_game_meta(gamePk)
    standings=fetch_standings_map()
    evs=fetch_scoring_official(gamePk, meta.home_tri, meta.away_tri)

    # опционально подменяем на sports.ru (если страница есть)
    sru_home, sru_away, url = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
    if sru_home or sru_away:
        evs = merge_official_with_sportsru(evs, sru_home, sru_away,
                                           meta.home_tri, meta.away_tri)

    text=build_single_match_text(meta, standings, evs)
    print("[DBG] Single match preview:\n"+text.replace("\n","¶")[:200]+"…")
    send_telegram_text(text)
    print("OK (posted 1)")

if __name__=="__main__":
    main()
