#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Daily Results — daily summary with spoilers

— Собирает FINAL-матчи за окно «REPORT_DATE_LOCAL-1 … REPORT_DATE_LOCAL+1» с учётом REPORT_TZ.
— Снаружи: эмодзи + названия команд (без счёта).
— Внутри <tg-spoiler>…</tg-spoiler>: жирный счёт с рекордами (W-L-OT), события по периодам.
— Заголовки периодов/ОТ/буллитов — курсивом; «Овертайм» без №, если один.
— Перед каждым заголовком периода добавляется пустая строка.
— Имена: сначала official PBP (players[]), потом по порядку подмешиваем sports.ru (кириллица) где доступно.
— Исправлены кейсы с пустыми именами, УТА, дубликаты скобок в ассистах.
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
    BeautifulSoup = None  # fallback

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

REPORT_DATE_LOCAL = _env_str("REPORT_DATE_LOCAL", "").strip()
REPORT_TZ         = _env_str("REPORT_TZ", "Europe/Amsterdam").strip()
DRY_RUN           = _env_bool("DRY_RUN", False)
DEBUG_VERBOSE     = _env_bool("DEBUG_VERBOSE", False)

# ---------- RU ----------
MONTHS_RU = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}
def plural_ru(n: int, one: str, two: str, five: str) -> str:
    n = abs(n) % 100; n1 = n % 10
    if 11 <= n <= 19: return five
    if 2 <= n1 <= 4:  return two
    if n1 == 1:       return one
    return five

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

# ---------- HTTP ----------
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
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

# ---------- DATA ----------
@dataclass
class TeamRecord:
    wins: int; losses: int; ot: int; points: int
    def as_str(self) -> str:
        return f"{self.wins}-{self.losses}-{self.ot}"
@dataclass
class GameMeta:
    gamePk: int; gameDateUTC: datetime; state: str
    home_tri: str; away_tri: str; home_score: int; away_score: int
@dataclass
class ScoringEvent:
    period: int; period_type: str; time: str; team_for: str
    home_goals: int; away_goals: int; scorer: str; assists: List[str]=field(default_factory=list)
@dataclass
class SRUGoal:
    time: Optional[str]; scorer_ru: Optional[str]; assists_ru: List[str]

# ---------- helpers ----------
def _upper_str(x: Any) -> str:
    try: return str(x or "").upper()
    except: return ""
def _first_int(*vals) -> int:
    for v in vals:
        if v is None: continue
        try:
            s = str(v).strip()
            if s == "": continue
            return int(float(s))
        except: continue
    return 0
def _extract_name(obj_or_str: Any) -> Optional[str]:
    if not obj_or_str: return None
    if isinstance(obj_or_str, str): return obj_or_str.strip() or None
    if isinstance(obj_or_str, dict):
        for k in ("name","playerName","playerFullName","default","fullName","firstLastName","lastFirstName","shortName"):
            v = obj_or_str.get(k)
            if isinstance(v, str) and v.strip(): return v.strip()
    return None
def _clean_person(s: str) -> str:
    # убрать случайные скобки вокруг имён из сторонних источников
    return re.sub(r"^\(+|\)+$", "", (s or "").strip())

# ---------- standings ----------
def fetch_standings_map() -> Dict[str, TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"; data = http_get_json(url)
    teams: Dict[str, TeamRecord] = {}; nodes = []
    if isinstance(data, dict):
        if isinstance(data.get("standings"), list): nodes = data["standings"]
        elif isinstance(data.get("records"), list): nodes = data["records"]
        elif isinstance(data.get("standings"), dict): nodes = data["standings"].get("overallRecords", []) or []
    elif isinstance(data, list): nodes = data
    for r in nodes:
        abbr = ""; ta = r.get("teamAbbrev")
        if isinstance(ta, str): abbr = ta.upper()
        elif isinstance(ta, dict): abbr = _upper_str(ta.get("default") or ta.get("tricode"))
        if not abbr: abbr = _upper_str(r.get("teamAbbrevTricode") or r.get("teamTriCode") or r.get("team"))
        rec = r.get("record") or r.get("overallRecord") or r.get("overallRecords") or {}
        wins=_first_int(rec.get("wins"),r.get("wins"),rec.get("gamesPlayedWins"))
        losses=_first_int(rec.get("losses"),r.get("losses"),rec.get("gamesPlayedLosses"),rec.get("regulationLosses"),r.get("regulationLosses"))
        ot=_first_int(rec.get("ot"),r.get("ot"),rec.get("otLosses"),r.get("otLosses"),rec.get("overtimeLosses"),r.get("overtimeLosses"))
        pts=_first_int(r.get("points"),rec.get("points"),r.get("pts"),rec.get("teamPoints"))
        if abbr: teams[abbr]=TeamRecord(wins,losses,ot,pts)
    if DEBUG_VERBOSE: print(f"[DBG] standings map built: {len(teams)}")
    return teams

# ---------- schedule ----------
def _window_dates_local() -> List[str]:
    # REPORT_DATE_LOCAL (в REPORT_TZ) => берём D-1, D, D+1, чтобы покрыть поздние матчи
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(REPORT_TZ or "Europe/Amsterdam")
    if REPORT_DATE_LOCAL:
        base = datetime.fromisoformat(REPORT_DATE_LOCAL).date()
    else:
        now = datetime.now(tz)
        base = now.date()
    dates = [ (base - timedelta(days=1)).isoformat(),
              base.isoformat(),
              (base + timedelta(days=1)).isoformat() ]
    return dates

def list_final_games_window() -> List[GameMeta]:
    metas: Dict[int, GameMeta] = {}
    for day in _window_dates_local():
        url = f"{NHLE_BASE}/schedule/{day}"
        if DEBUG_VERBOSE: print(f"[DBG] GET {url}")
        s = http_get_json(url)
        games = s.get("games")
        if games is None:
            games=[]
            for w in s.get("gameWeek", []) or []:
                games += (w.get("games") or [])
        for g in games:
            state = _upper_str(g.get("gameState") or g.get("gameStatus"))
            if state not in ("FINAL","OFF"): continue
            gid = _first_int(g.get("id"), g.get("gameId"), g.get("gamePk"))
            if gid == 0: continue
            gd = g.get("startTimeUTC") or g.get("gameDate") or ""
            try: gdt = datetime.fromisoformat(str(gd).replace("Z","+00:00"))
            except: gdt = datetime.now(timezone.utc)
            home = g.get("homeTeam", {}) or {}; away = g.get("awayTeam", {}) or {}
            htri = _upper_str(home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev"))
            atri = _upper_str(away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev"))
            hscore = _first_int(home.get("score")); ascore = _first_int(away.get("score"))
            metas[gid] = GameMeta(gid,gdt,state,htri,atri,hscore,ascore)
    games = sorted(metas.values(), key=lambda m: m.gameDateUTC)
    if DEBUG_VERBOSE: print(f"[DBG] Collected FINAL games: {len(games)}")
    return games

# ---------- PBP ----------
_SO_TYPES_GOAL = {"GOAL","SHOT"}  # в SO часть «голов» помечается как SHOT с флагом wasGoal
_ASSIST_KEYS = (
    "assist1PlayerName","assist2PlayerName","assist3PlayerName",
    "assist1","assist2","assist3",
    "primaryAssist","secondaryAssist","tertiaryAssist",
)
_SCORER_KEYS = (
    "scoringPlayerName","scorerName","shootingPlayerName","scoringPlayer",
    "goalScorer","primaryScorer","playerName","player",
    "shooterName","shootoutShooterName","shooter","scorer",
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

def _extract_from_players_array(p: dict) -> Tuple[str, List[str]]:
    """
    Универсально достаем автора/ассисты из p["players"] или details["players"].
    Ищем по признакам роли: Scorer/Shooter/Goal/Assist.
    """
    players = []
    for key in ("players","playerList","participants"):
        v = p.get(key)
        if isinstance(v, list) and v: players = v; break
    if not players:
        det = p.get("details") or {}
        for key in ("players","playerList","participants"):
            v = det.get(key)
            if isinstance(v, list) and v: players = v; break

    scorer = None
    assists: List[str] = []
    for it in players or []:
        role = (_extract_name(it.get("playerType")) or _extract_name(it.get("type")) or _extract_name(it.get("role")) or "").lower()
        name = _extract_name(it) or _extract_name(it.get("player","")) or _extract_name({"name":it.get("firstLastName")})
        if not name: continue
        if "scor" in role or "shoot" in role or role in ("goal","goalscorer","scorer"):
            scorer = name
        elif "assist" in role:
            assists.append(name)
    return scorer or "", assists

def fetch_scoring_official(gamePk: int, home_tri: str, away_tri: str) -> List[ScoringEvent]:
    data = http_get_json(PBP_FMT.format(gamePk=gamePk))
    plays = data.get("plays", []) or []
    events: List[ScoringEvent] = []
    prev_h=prev_a=0

    for p in plays:
        type_key = _upper_str(p.get("typeDescKey"))
        pd = p.get("periodDescriptor", {}) or {}
        period = _first_int(pd.get("number"))
        ptype  = _normalize_period_type(pd.get("periodType") or "REG")
        det = p.get("details", {}) or {}
        t = str(p.get("timeInPeriod") or "00:00").replace(":", ".")

        is_goal = (type_key == "GOAL") or _is_shootout_goal(type_key, det, ptype)
        if not is_goal:
            continue

        # Счёт
        h = det.get("homeScore"); a = det.get("awayScore")
        if not (isinstance(h,int) and isinstance(a,int)):
            sc = p.get("score", {}) or {}
            if isinstance(sc.get("home"),int) and isinstance(sc.get("away"),int):
                h,a = sc["home"], sc["away"]
            else:
                h,a = prev_h, prev_a  # в SO не меняется

        # Команда, забившая
        team = home_tri if h>prev_h else (away_tri if a>prev_a else _upper_str(
            det.get("eventOwnerTeamAbbrev") or p.get("teamAbbrev") or det.get("teamAbbrev") or det.get("scoringTeamAbbrev")
        ))

        # Автор и ассисты — сначала details-ключи, затем players[]
        scorer = ""
        for k in _SCORER_KEYS:
            nm = _extract_name(det.get(k))
            if nm: scorer = nm; break
        assists: List[str] = []
        for k in _ASSIST_KEYS:
            nm = _extract_name(det.get(k))
            if nm: assists.append(nm)

        if not scorer:
            s2, a2 = _extract_from_players_array(p)
            scorer = s2 or scorer
            # если ранее что-то собрали — не перетираем ассисты, а дополняем
            if not assists and a2:
                assists = a2

        scorer = _clean_person(scorer)
        assists = [_clean_person(x) for x in assists]

        events.append(ScoringEvent(period, ptype, t, team, h, a, scorer, assists))
        if ptype != "SHOOTOUT":
            prev_h, prev_a = h, a

    if DEBUG_VERBOSE: print(f"[DBG] PBP goals parsed: {len(events)} for game {gamePk}")
    return events

# ---------- sports.ru ----------
TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")
def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or ""); 
    return f"{int(m.group(1)):02d}.{m.group(2)}" if m else None

def parse_sportsru_goals_html(html: str, side: str) -> List[SRUGoal]:
    res: List[SRUGoal] = []
    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if ul:
            for li in ul.find_all("li", recursive=False):
                anchors = [a.get_text(strip=True) for a in li.find_all("a")]
                scorer_ru = anchors[0] if anchors else None
                assists_ru = anchors[1:] if len(anchors) > 1 else []
                raw_text = li.get_text(" ", strip=True)
                time_ru = _extract_time(raw_text)
                res.append(SRUGoal(time_ru, scorer_ru, assists_ru))
    return res

def fetch_sportsru_goals(home_tri: str, away_tri: str) -> Tuple[List[SRUGoal], List[SRUGoal], str]:
    hs = SPORTSRU_SLUG.get(home_tri); as_ = SPORTSRU_SLUG.get(away_tri)
    if not hs or not as_: return [], [], ""
    for order in [(hs,as_),(as_,hs)]:
        url = f"https://www.sports.ru/hockey/match/{order[0]}-vs-{order[1]}/"
        try:
            html = http_get_text(url, timeout=20)
        except Exception as e:
            if DEBUG_VERBOSE: print(f"[DBG] sports.ru fetch fail {url}: {repr(e)}")
            continue
        home_side = "home" if order[0]==hs else "away"
        away_side = "away" if home_side=="home" else "home"
        h = parse_sportsru_goals_html(html, home_side); a = parse_sportsru_goals_html(html, away_side)
        if h or a:
            if DEBUG_VERBOSE: print(f"[DBG] sports.ru goals ok for {url}: home={len(h)} away={len(a)}")
            return h,a,url
    return [],[], ""

# ---------- merge & format ----------
def merge_official_with_sportsru(evs: List[ScoringEvent], sru_home: List[SRUGoal], sru_away: List[SRUGoal], home_tri: str, away_tri: str) -> List[ScoringEvent]:
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
        # финальная зачистка лишних скобок
        ev.scorer=_clean_person(ev.scorer)
        ev.assists=[_clean_person(x) for x in ev.assists]
        out.append(ev)
    if DEBUG_VERBOSE: print(f"[DBG] used sports.ru: home_used={h_i}/{len(sru_home)} away_used={a_i}/{len(sru_away)}")
    return out

def _italic(s: str) -> str: return f"<i>{s}</i>"
def period_title_text(num: int, ptype: str, ot_index: Optional[int], ot_total: int) -> str:
    t=(ptype or "").upper()
    if t=="REGULAR": return f"{num}-й период"
    if t=="OVERTIME": return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_index or 1}"
    if t=="SHOOTOUT": return "Буллиты"
    return f"Период {num}"
def line_goal(ev: ScoringEvent) -> str:
    score=f"{ev.home_goals}:{ev.away_goals}"
    who=ev.scorer or "—"
    # если лишь один ассист — просто одно имя в одних скобках
    assists = [x for x in ev.assists if x]
    tail = f" ({', '.join(assists)})" if assists else ""
    return f"{score} – {ev.time} {who}{tail}"

def build_match_block_with_spoiler(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he = TEAM_EMOJI.get(meta.home_tri, ""); ae = TEAM_EMOJI.get(meta.away_tri, "")
    hn = TEAM_RU.get(meta.home_tri, meta.home_tri); an = TEAM_RU.get(meta.away_tri, meta.away_tri)
    # ВИДИМЫЙ заголовок (без форматирования, чтобы Telegram не «ломал»):
    visible = f"{he} «{hn}»\n{ae} «{an}»"

    hrec = standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec = standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head_hidden = f"<b>«{hn}»: {meta.home_score}</b> ({hrec})\n<b>«{an}»: {meta.away_score}</b> ({arec})"

    groups: Dict[Tuple[int,str], List[ScoringEvent]] = {}
    for ev in events: groups.setdefault((ev.period, ev.period_type), []).append(ev)
    for pnum in (1,2,3):
        if (pnum,"REGULAR") not in groups: groups[(pnum,"REGULAR")] = []
    ot_keys = sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total = len(ot_keys); ot_order = {k:i+1 for i,k in enumerate(ot_keys)}

    body_lines: List[str] = [head_hidden]
    sort_key = lambda x:(x[0], 0 if (x[1] or "").upper()=="REGULAR" else 1 if (x[1] or "").upper()=="OVERTIME" else 2)
    for key in sorted(groups.keys(), key=sort_key):
        pnum, ptype = key; ot_idx = ot_order.get(key)
        title = period_title_text(pnum, ptype, ot_idx, ot_total)
        body_lines.append("")                 # пустая строка перед заголовком периода
        body_lines.append(_italic(title))     # заголовок курсивом
        period_events = groups[key]
        if not period_events:
            body_lines.append("Голов не было")
        else:
            for ev in period_events: body_lines.append(line_goal(ev))

    hidden = "<tg-spoiler>" + ("\n".join(body_lines).strip()) + "</tg-spoiler>"
    return visible + "\n\n" + hidden

# ---------- telegram ----------
def chunk_text(s: str, hard_limit: int = 3800, soft_sep: str = "——————————————————\n") -> List[str]:
    s=s.strip()
    if len(s)<=hard_limit: return [s]
    parts=[]; cur=""; blocks=s.split(soft_sep)
    for i,b in enumerate(blocks):
        piece=(b if i==0 else soft_sep+b).rstrip()
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
                    if tmp: parts.append(tmp.rstrip()); tmp=""; cur=""
    if cur: parts.append(cur.rstrip())
    if len(parts)>1:
        total=len(parts); head=parts[0]
        parts=[head]+[f"…продолжение (часть {i}/{total})\n\n{p}" for i,p in enumerate(parts[1:],start=2)]
    return parts

def send_telegram_text(text: str) -> None:
    token=_env_str("TELEGRAM_BOT_TOKEN","").strip()
    chat_id=_env_str("TELEGRAM_CHAT_ID","").strip()
    thread=_env_str("TELEGRAM_THREAD_ID","").strip()
    if not token or not chat_id: print("[ERR] Telegram token/chat_id not set"); return
    url=f"{TG_API}/bot{token}/sendMessage"; headers={"Content-Type":"application/json"}
    parts=chunk_text(text, 3800, "——————————————————\n")
    if DEBUG_VERBOSE: print(f"[DBG] Telegram parts: {len(parts)}")
    for part in parts:
        payload={
            "chat_id": int(chat_id) if chat_id.strip("-").isdigit() else chat_id,
            "text": part,
            "disable_web_page_preview": True,
            "disable_notification": False,
            "parse_mode": "HTML",
        }
        if thread:
            try: payload["message_thread_id"]=int(thread)
            except: pass
        if DRY_RUN: 
            print("[DRY RUN] "+textwrap.shorten(part,200,placeholder="…")); 
            continue
        resp=requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        try: data=resp.json()
        except: data={"ok":None,"raw":resp.text}
        if DEBUG_VERBOSE: print(f"[DBG] TG HTTP={resp.status_code} JSON keys={list(data.keys())}")
        if resp.status_code!=200 or not data.get("ok",False):
            print(f"[ERR] sendMessage failed: {data.get('error_code')} {data.get('description')}")

# ---------- main ----------
def header_ru(day_local: datetime, n_games: int) -> str:
    word=plural_ru(n_games,"матч","матча","матчей")
    return f"🗓 Регулярный чемпионат НХЛ • {day_local.day} {MONTHS_RU[day_local.month]} • {n_games} {word}"

def make_post_text(day_local: datetime, games: List[GameMeta], standings: Dict[str,TeamRecord]) -> str:
    header_block = f"{header_ru(day_local, len(games))}\n\nРезультаты надёжно спрятаны 👇"
    blocks: List[str] = [header_block]
    for meta in games:
        evs = fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)
        sru_home, sru_away, _ = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        merged = merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)
        blocks.append(build_match_block_with_spoiler(meta, standings, merged))
    return "\n\n——————————————————\n".join(blocks).strip()

def main():
    # локальный день для заголовка
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(REPORT_TZ or "Europe/Amsterdam")
    day_local = datetime.fromisoformat((REPORT_DATE_LOCAL or datetime.now(tz).date().isoformat())) \
                        .replace(tzinfo=tz, hour=12, minute=0, second=0, microsecond=0)
    print(f"[DBG] Daily summary for {(REPORT_DATE_LOCAL or day_local.date().isoformat())} in {tz.key}")

    games=list_final_games_window()
    if not games: 
        print("OK (нет FINAL игр в окне)")
        return
    standings=fetch_standings_map()
    text=make_post_text(day_local, games, standings)
    if DEBUG_VERBOSE: print("[DBG] Preview 500:\n"+text[:500].replace("\n","¶")+"…")
    send_telegram_text(text)
    print("OK")

if __name__=="__main__": main()
