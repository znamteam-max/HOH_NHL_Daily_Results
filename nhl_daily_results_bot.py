#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, re, json, textwrap
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import requests

TG_API   = "https://api.telegram.org"
NHLE_BASE = "https://api-web.nhle.com/v1"
STATS_BASE = "https://statsapi.web.nhl.com/api/v1"

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

# ---------- HTTP ----------
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.8",
}

def http_get_json(url: str, timeout: int = 30) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return json.loads(r.text or "{}")

def http_get_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

# ---------- STRUCTS ----------
@dataclass
class TeamRecord:
    wins:int; losses:int; ot:int
    def as_str(self)->str:  # без очков
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

@dataclass
class SRUGoal:
    time: Optional[str]
    scorer_ru: Optional[str]
    assists_ru: List[str]

# ---------- UTILS ----------
def _upper_str(v: Any)->str:
    return str(v or "").upper()

def _extract_name(v: Any)->Optional[str]:
    if isinstance(v,str):
        s=v.strip()
        return s if s else None
    return None

def _strip_wrapping_parens(s: str)->str:
    ss=s.strip()
    while ss.startswith("(") and ss.endswith(")") and len(ss)>=2:
        ss = ss[1:-1].strip()
    return ss

def _italic(s:str)->str:
    return f"<i>{s}</i>"

def ru_plural(n:int, one:str, few:str, many:str)->str:
    n10=n%10; n100=n%100
    if n10==1 and n100!=11: return one
    if 2<=n10<=4 and not 12<=n100<=14: return few
    return many

# ---------- STANDINGS ----------
def fetch_standings_map() -> Dict[str, TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"
    data = http_get_json(url)
    teams: Dict[str, TeamRecord] = {}
    nodes = []
    if isinstance(data, dict):
        nodes = data.get("standings") or data.get("records") or []
        if isinstance(nodes,dict):
            nodes = nodes.get("overallRecords", []) or []
    elif isinstance(data, list):
        nodes = data
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
        if abbr:
            teams[abbr] = TeamRecord(wins, losses, ot)
    return teams

# ---------- SCHEDULE ----------
def list_final_games_window(days_back: int = 1, days_fwd: int = 0) -> List[GameMeta]:
    now_utc = datetime.now(timezone.utc)
    dates = [(now_utc - timedelta(days=d)).date().isoformat()
             for d in range(days_back, -days_fwd-1, -1)]

    metas: Dict[int, GameMeta] = {}
    for day in dates:
        url = f"{NHLE_BASE}/schedule/{day}"
        s = http_get_json(url)
        for w in s.get("gameWeek", []) or []:
            for g in w.get("games", []) or []:
                state = (g.get("gameState") or g.get("gameStatus") or "").upper()
                if state not in ("FINAL","OFF"):
                    continue
                gid = int(g.get("id") or g.get("gameId") or g.get("gamePk") or 0)
                if gid == 0: continue
                gd = g.get("startTimeUTC") or g.get("gameDate") or ""
                try:
                    gdt = datetime.fromisoformat(gd.replace("Z","+00:00"))
                except Exception:
                    gdt = now_utc
                home = g.get("homeTeam", {}) or {}
                away = g.get("awayTeam", {}) or {}
                htri = (home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev") or "").upper()
                atri = (away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev") or "").upper()
                hscore = int(home.get("score") or 0)
                ascore = int(away.get("score") or 0)
                metas[gid] = GameMeta(
                    gamePk=gid, gameDateUTC=gdt, state=state,
                    home_tri=htri, away_tri=atri,
                    home_score=hscore, away_score=ascore
                )
    games = sorted(metas.values(), key=lambda m: m.gameDateUTC)
    return games

# ---------- OFFICIAL LIVEFEED ----------
def fetch_scoring_official(gamePk: int, home_tri: str, away_tri: str) -> List[ScoringEvent]:
    url = f"{STATS_BASE}/game/{gamePk}/feed/live"
    data = http_get_json(url)
    plays = (data.get("liveData", {}).get("plays", {}) or {})
    allPlays = plays.get("allPlays", []) or []
    idxs = plays.get("scoringPlays", []) or []
    events: List[ScoringEvent] = []
    h=a=0
    for i in idxs:
        if not (0 <= i < len(allPlays)): continue
        p = allPlays[i]
        res = p.get("result", {}) or {}
        if _upper_str(res.get("eventTypeId")) != "GOAL": continue
        about = p.get("about", {}) or {}
        period = int(about.get("period") or 0)
        ptype  = _upper_str(about.get("periodType") or "REGULAR")
        t = (about.get("periodTime") or "00:00").replace(":", ".")
        team = ((p.get("team", {}) or {}).get("triCode") or "").upper()

        goals = about.get("goals", {}) or {}
        if isinstance(goals.get("home"), int) and isinstance(goals.get("away"), int):
            h, a = goals["home"], goals["away"]
        else:
            if team == home_tri: h += 1
            elif team == away_tri: a += 1

        scorer="" ; assists:List[str]=[]
        for comp in (p.get("players") or []):
            role=(comp.get("playerType") or "").lower()
            name=((comp.get("player") or {}).get("fullName") or "").strip()
            if role=="scorer" and name:
                scorer=name
            elif role=="assist" and name:
                assists.append(name)

        if not scorer:
            det_candidates = ("scoringPlayer","scorerName","scoringPlayerName","shootingPlayerName",
                              "goalScorer","primaryScorer","playerName")
            for k in det_candidates:
                nm = _extract_name(res.get(k))
                if nm: scorer=nm; break
        if not scorer:
            # любой *name без goalie/team/coach
            for source in (res, p):
                for k,v in (source or {}).items():
                    if isinstance(v,str):
                        lk=k.lower()
                        if "name" in lk and not any(x in lk for x in ("goalie","team","coach")):
                            s=v.strip()
                            if s and (" " in s or "-" in s):
                                scorer=s; break
                if scorer: break

        assists=[_strip_wrapping_parens(a) for a in assists if a.strip()]
        events.append(ScoringEvent(period, ptype, t, team, h, a, scorer, assists))
    return events

# ---------- SPORTSRU PARSER ----------
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
    ul_pat = re.compile(
        r'<ul[^>]*class="[^"]*match-summary__goals-list[^"]*--%s[^"]*"[^>]*>(.*?)</ul>'%side,
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
    return results

def fetch_sportsru_goals(home_tri: str, away_tri: str) -> Tuple[List[SRUGoal], List[SRUGoal], str]:
    home_slug = SPORTSRU_SLUG.get(home_tri)
    away_slug = SPORTSRU_SLUG.get(away_tri)
    if not home_slug or not away_slug:
        return [], [], ""
    for order in [(home_slug, away_slug), (away_slug, home_slug)]:
        url = f"https://www.sports.ru/hockey/match/{order[0]}-vs-{order[1]}/"
        try:
            html = http_get_text(url, timeout=20)
        except Exception:
            continue
        home_side = "home" if order[0] == home_slug else "away"
        away_side = "away" if home_side == "home" else "home"
        home_goals = parse_sportsru_goals_html(html, home_side)
        away_goals = parse_sportsru_goals_html(html, away_side)
        if home_goals or away_goals:
            return home_goals, away_goals, url
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
    for ev in evs:
        if ev.team_for == home_tri and h_i < len(sru_home):
            g = sru_home[h_i]; h_i += 1
            if g.scorer_ru: ev.scorer  = g.scorer_ru
            if g.assists_ru: ev.assists = [_strip_wrapping_parens(a) for a in g.assists_ru if a.strip()]
        elif ev.team_for == away_tri and a_i < len(sru_away):
            g = sru_away[a_i]; a_i += 1
            if g.scorer_ru: ev.scorer  = g.scorer_ru
            if g.assists_ru: ev.assists = [_strip_wrapping_parens(a) for a in g.assists_ru if a.strip()]
        out.append(ev)
    return out

# ---------- FORMAT ----------
def period_title_text(num:int, ptype:str, ot_idx:Optional[int], ot_total:int)->str:
    t=(ptype or "").upper()
    if t=="REGULAR":
        return f"{num}-й период"
    if t=="OVERTIME":
        return "Овертайм" if ot_total<=1 else f"Овертайм №{ot_idx or 1}"
    if t=="SHOOTOUT":
        return "Буллиты"
    return f"Период {num}"

def line_goal(ev: ScoringEvent) -> str:
    score = f"{ev.home_goals}:{ev.away_goals}"
    assists = [a for a in ev.assists if a]
    assist_txt = f" ({', '.join(assists)})" if assists else ""
    who = ev.scorer or "—"
    return f"{score} – {ev.time} {who}{assist_txt}"

def build_match_block_with_spoiler(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> Tuple[str,str]:
    # outside (no score)
    he = TEAM_EMOJI.get(meta.home_tri, "")
    ae = TEAM_EMOJI.get(meta.away_tri, "")
    hn = TEAM_RU.get(meta.home_tri, meta.home_tri)
    an = TEAM_RU.get(meta.away_tri, meta.away_tri)
    outside = f"{he} <b>«{hn}»</b>\n{ae} <b>«{an}»</b>"

    # inside spoiler (with score & record)
    hrec = standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "?"
    arec = standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "?"
    head = f"<b>«{hn}»: {meta.home_score}</b> ({hrec})\n<b>«{an}»: {meta.away_score}</b> ({arec})"

    groups: Dict[Tuple[int,str], List[ScoringEvent]] = {}
    for ev in events: groups.setdefault((ev.period, ev.period_type), []).append(ev)

    # ensure base periods
    for p in (1,2,3): groups.setdefault((p,"REGULAR"), [])

    # overtime indexing
    ot_keys = sorted([k for k in groups if (k[1] or "").upper()=="OVERTIME"], key=lambda x:x[0])
    ot_total=len(ot_keys); ot_order={k:i+1 for i,k in enumerate(ot_keys)}

    body_lines: List[str] = [head]
    def sort_key(k):
        t=(k[1] or "").upper()
        return (k[0], 0 if t=="REGULAR" else 1 if t=="OVERTIME" else 2)

    for key in sorted(groups.keys(), key=sort_key):
        pnum, ptype = key
        ot_idx = ot_order.get(key)
        title = period_title_text(pnum, ptype, ot_idx, ot_total)
        body_lines.append("")
        body_lines.append(_italic(title))
        period_events = groups[key]
        T = (ptype or "").upper()
        if T == "SHOOTOUT":
            if not period_events:
                body_lines.append("Голов не было")
            else:
                last = period_events[-1]
                who = last.scorer or "—"
                final_score = f"{meta.home_score}:{meta.away_score}"
                body_lines.append("Победный буллит")
                body_lines.append(f"{final_score} - {who}")
        else:
            if not period_events:
                body_lines.append("Голов не было")
            else:
                for ev in period_events:
                    body_lines.append(line_goal(ev))

    inside = "<tg-spoiler>" + "\n".join(body_lines).strip() + "</tg-spoiler>"
    return outside, inside

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
            "chat_id": int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id,
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

# ---------- HEADER ----------
def header_ru(n_games: int) -> str:
    now = datetime.now()
    word = ru_plural(n_games, "матч", "матча", "матчей")
    return f"🗓 Регулярный чемпионат НХЛ • {now.day} {MONTHS_RU[now.month]} • {n_games} {word}"

# ---------- MAKE POST ----------
def make_post_text(games: List[GameMeta], standings: Dict[str,TeamRecord]) -> str:
    blocks: List[str] = [header_ru(len(games)), "", "Результаты надёжно спрятаны 👇", "", "——————————————————"]
    for idx, meta in enumerate(games, start=1):
        evs = fetch_scoring_official(meta.gamePk, meta.home_tri, meta.away_tri)
        sru_home, sru_away, url = fetch_sportsru_goals(meta.home_tri, meta.away_tri)
        if sru_home or sru_away:
            evs = merge_official_with_sportsru(evs, sru_home, sru_away, meta.home_tri, meta.away_tri)
        outside, inside = build_match_block_with_spoiler(meta, standings, evs)
        blocks.append(outside + "\n\n" + inside)
        if idx != len(games):
            blocks.append("\n——————————————————")
    return "\n".join(blocks).strip()

# ---------- MAIN ----------
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
