#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, time, re, textwrap
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, timedelta
import requests
from bs4 import BeautifulSoup

API = "https://api-web.nhle.com"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NHLSingleBot/1.3; +github)",
    "Accept": "application/json, text/plain, */*",
}

SCHEDULE_FMT     = API + "/v1/schedule/{ymd}"
GAME_SUMMARY_FMT = API + "/v1/gamecenter/{gamePk}/game-summary"
GAME_PBP_FMT     = API + "/v1/gamecenter/{gamePk}/play-by-play"
GAME_BOX_FMT     = API + "/v1/gamecenter/{gamePk}/boxscore"

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID   = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
DEBUG_VERBOSE = (os.getenv("DEBUG_VERBOSE") or "1").strip() == "1"

GAME_PK_ENV = (os.getenv("GAME_PK") or "").strip()
GAME_QUERY  = (os.getenv("GAME_QUERY") or "").strip()

def dbg(msg: str):
    if DEBUG_VERBOSE:
        print(f"[DBG] {msg}", flush=True)

def _get_with_retries(url: str, *, timeout: int = 30, as_text: bool = False) -> Any:
    last=None
    for i in range(3):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text if as_text else r.json()
        except Exception as e:
            last = e
            dbg(f"retry {i+1}/3 for {url} after {0.75*(i+1):.2f}s: {repr(e)}")
            time.sleep(0.75*(i+1))
    if last: raise last

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)

TEAM_RU = {
    "EDM":"Эдмонтон","DAL":"Даллас","DET":"Детройт","NSH":"Нэшвилл",
    "TBL":"Тампа-Бэй","CGY":"Калгари","FLA":"Флорида","PHI":"Филадельфия",
    "NJD":"Нью-Джерси","STL":"Сент-Луис","NYI":"Айлендерс","BOS":"Бостон",
    "PIT":"Питтсбург","BUF":"Баффало","WSH":"Вашингтон","WPG":"Виннипег",
    "CAR":"Каролина","NYR":"Рейнджерс","CBJ":"Коламбус","TOR":"Торонто",
    "CHI":"Чикаго","MIN":"Миннесота","COL":"Колорадо","SJS":"Сан-Хосе",
    "UTA":"Юта","MTL":"Монреаль","VGK":"Вегас","OTT":"Оттава",
    "ANA":"Анахайм","VAN":"Ванкувер","SEA":"Сиэтл","LAK":"Лос-Анджелес",
}
TEAM_EMOJI = {
    "EDM":"🛢️","DAL":"⭐️","DET":"🛡️","NSH":"🐯","TBL":"⚡","CGY":"🔥",
    "FLA":"🐆","PHI":"🛩","NJD":"😈","STL":"🎵","NYI":"🏝️","BOS":"🐻",
    "PIT":"🐧","BUF":"🦬","WSH":"🦅","WPG":"✈️","CAR":"🌪️","NYR":"🗽",
    "CBJ":"💣","TOR":"🍁","CHI":"🦅","MIN":"🌲","COL":"⛰️","SJS":"🦈",
    "UTA":"🧊","MTL":"🇨🇦","VGK":"🎰","OTT":"🛡","ANA":"🦆","VAN":"🐳",
    "SEA":"🦑","LAK":"👑",
}

def _slugify_en(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s); s = re.sub(r"\s+", "-", s); s = re.sub(r"-+", "-", s)
    return s

SPORTSRU_TEAM_SLUGS = {
    "VGK": ["vegas","vegas-golden-knights"],
    "UTA": ["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
    "UTH": ["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
    "UTAH":["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
}

def _team_slug_variants_for_sportsru(team: Dict[str,Any]) -> List[str]:
    v=[]; abbr=(team.get("abbrev") or team.get("triCode") or "").upper()
    if abbr in SPORTSRU_TEAM_SLUGS: v += SPORTSRU_TEAM_SLUGS[abbr]
    place=_slugify_en(team.get("placeName") or team.get("city") or "")
    nick=_slugify_en(team.get("teamName") or team.get("name") or "")
    if place and nick: v.append(f"{place}-{nick}")
    if nick: v.append(nick)
    if place and place not in v: v.append(place)
    seen=set(); out=[]
    for x in v:
        if x and x not in seen: out.append(x); seen.add(x)
    return out

def gen_sportsru_match_urls(home_team: Dict[str,Any], away_team: Dict[str,Any]) -> List[str]:
    base="https://www.sports.ru/hockey/match"
    hs=_team_slug_variants_for_sportsru(home_team)
    as_=_team_slug_variants_for_sportsru(away_team)
    urls=[]
    for h in hs:
        for a in as_:
            urls += [f"{base}/{h}-vs-{a}/", f"{base}/{a}-vs-{h}/",
                     f"{base}/{h}-vs-{a}/stat/", f"{base}/{a}-vs-{h}/stat/"]
    seen=set(); out=[]
    for u in urls:
        if u not in seen: out.append(u); seen.add(u)
    return out

def try_parse_sportsru_names(url: str) -> Dict[str,str]:
    try:
        html = http_get_text(url, timeout=25)
    except Exception as e:
        dbg(f"sports.ru fetch fail {url}: {e!r}")
        return {}
    soup = BeautifulSoup(html, "html.parser")
    ru: Dict[str,str] = {}
    for a in soup.select("a[href*='/hockey/players/'], a[href*='/hockey/player/']"):
        txt = (a.get_text(strip=True) or "")
        if not txt: continue
        ru_last = txt.split()[-1]
        en = (a.get("title") or a.get("data-name") or a.get("data-player-name") or "").strip()
        if not en:
            href = a.get("href") or ""
            m = re.search(r"/players/([\w-]+)/", href)
            if m: en = m.group(1).replace("-", " ")
        if en:
            en_last = en.split()[-1].title()
            if en_last and ru_last: ru[en_last] = ru_last
    if ru: dbg(f"sports.ru names extracted from {url}: {len(ru)}")
    return ru

def fetch_ru_name_map_for_match(home_team: Dict[str,Any], away_team: Dict[str,Any]) -> Dict[str,str]:
    tried=[]
    for url in gen_sportsru_match_urls(home_team, away_team):
        tried.append(url)
        mp = try_parse_sportsru_names(url)
        if mp:
            dbg(f"sports.ru goals ok for {url}")
            return mp
    dbg("sports.ru tried URLs (no data): " + " | ".join(tried[:8]))
    return {}

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)

def fetch_json_safe(url: str) -> Optional[Dict[str,Any]]:
    try:
        js = http_get_json(url)
        if isinstance(js, dict): return js
    except Exception as e:
        dbg(f"fetch fail {url}: {e!r}")
    return None

def resolve_game_pk_from_query(q: str) -> Optional[int]:
    try:
        ymd, pair = q.split(" ", 1)
        base = date.fromisoformat(ymd)
    except Exception:
        return None
    home, away = "", ""
    if "@" in pair:
        a, h = pair.split("@", 1); away, home = a.strip().upper(), h.strip().upper()
    elif "-" in pair:
        left, right = pair.split("-", 1); home, away = left.strip().upper(), right.strip().upper()
    else:
        return None

    def find_on_day(d: date) -> Optional[int]:
        js = http_get_json(SCHEDULE_FMT.format(ymd=d.isoformat()))
        games = js.get("games")
        if games is None:
            games=[]
            for w in (js.get("gameWeek") or []):
                games.extend(w.get("games") or [])
        for g in games:
            h = ((g.get("homeTeam") or {}).get("abbrev") or "").upper()
            a = ((g.get("awayTeam") or {}).get("abbrev") or "").upper()
            if h == home and a == away:
                return g.get("id")
        return None

    for d in (base - timedelta(days=1), base, base + timedelta(days=1)):
        gid = find_on_day(d)
        if gid:
            dbg(f"Resolved GAME_PK={gid} for {q}")
            return gid
    return None

def mmss_to_ru(mmss: str) -> str:
    return (mmss or "00:00").replace(":", ".")

def load_pbp_data(game_pk: int):
    js_any = http_get_json(GAME_PBP_FMT.format(gamePk=game_pk))
    plays_obj = {"scoringPlays": [], "allPlays": [], "shootoutPlays": []}
    if isinstance(js_any, dict):
        raw = js_any.get("plays")
        if isinstance(raw, dict):
            plays_obj = {"scoringPlays": raw.get("scoringPlays") or [],
                         "allPlays":     raw.get("allPlays")     or [],
                         "shootoutPlays":raw.get("shootoutPlays") or []}
        elif isinstance(raw, list):
            plays_obj["allPlays"] = raw
        else:
            plays_obj["allPlays"] = js_any.get("allPlays") or []
    elif isinstance(js_any, list):
        plays_obj["allPlays"] = js_any

    scoring = plays_obj.get("scoringPlays") or []
    allplays = plays_obj.get("allPlays") or []
    raw_so = plays_obj.get("shootoutPlays") or []

    goals=[]
    for ev in scoring:
        per = (
            (ev.get("periodDescriptor") or {}).get("number")
            or (ev.get("period") or {}).get("number")
            or (ev.get("about")  or {}).get("periodNumber")
            or 0
        ) or 0
        tm  = ev.get("timeInPeriod") or (ev.get("about") or {}).get("periodTime") or "00:00"
        owner = ( (ev.get("details") or {}).get("eventOwnerTeamAbbrev")
                  or ev.get("teamAbbrev")
                  or (ev.get("team") or {}).get("abbrev") or "" )
        def _scorer(ev):
            sc = ev.get("scorer")
            if isinstance(sc, dict):
                nm = (sc.get("lastName") or sc.get("name") or sc.get("fullName") or "").strip()
                if nm: return nm.split()[-1]
            for p in ev.get("players") or []:
                if (p.get("type") or p.get("playerType") or "").lower() in ("scorer","shooter"):
                    nm = (p.get("lastName") or p.get("name") or p.get("fullName") or "").strip()
                    if nm: return nm.split()[-1]
            det = ev.get("details") or {}
            for k in ("shootoutShooterName","scoringPlayerName","scorerName"):
                nm = (det.get(k) or "").strip()
                if nm: return nm.split()[-1]
            return ""
        def _assists(ev):
            out=[]
            for a in ev.get("assists") or []:
                nm = (a.get("lastName") or a.get("name") or a.get("fullName") or "").strip()
                if nm: out.append(nm.split()[-1])
            if out: return out
            for p in ev.get("players") or []:
                if (p.get("type") or p.get("playerType") or "").lower().startswith("assist"):
                    nm = (p.get("lastName") or p.get("name") or p.get("fullName") or "").strip()
                    if nm: out.append(nm.split()[-1])
            return out
        goals.append({"period":per,"time":tm,"teamAbbrev":owner,"scorer":_scorer(ev),"assists":_assists(ev)})

    if not raw_so:
        for ev in allplays:
            per = (
                (ev.get("periodDescriptor") or {}).get("number")
                or (ev.get("period") or {}).get("number")
                or (ev.get("about")  or {}).get("periodNumber")
                or 0
            ) or 0
            ptype = (ev.get("periodDescriptor") or {}).get("periodType") \
                    or (ev.get("about") or {}).get("ordinalNum") or ""
            if per >= 5 or str(ptype).upper() == "SO":
                raw_so.append(ev)

    shootout=[]
    rnd=0
    for ev in raw_so:
        team = ((ev.get("details") or {}).get("eventOwnerTeamAbbrev")
                or ev.get("teamAbbrev")
                or (ev.get("team") or {}).get("abbrev") or "").upper()
        # shooter
        sc=""
        scd=ev.get("scorer")
        if isinstance(scd, dict):
            nm=(scd.get("lastName") or scd.get("name") or scd.get("fullName") or "").strip()
            if nm: sc = nm.split()[-1]
        if not sc:
            for p in ev.get("players") or []:
                if (p.get("type") or p.get("playerType") or "").lower() in ("scorer","shooter"):
                    nm=(p.get("lastName") or p.get("name") or p.get("fullName") or "").strip()
                    if nm: sc = nm.split()[-1]; break
        if not sc:
            det = ev.get("details") or {}
            for k in ("shootoutShooterName","scoringPlayerName","scorerName"):
                nm = (det.get(k) or "").strip()
                if nm: sc = nm.split()[-1]; break
        tdk = (ev.get("typeDescKey") or "").lower()
        det = ev.get("details") or {}
        is_goal = bool(det.get("isGoal"))
        if "goal" in tdk: is_goal = True
        if "miss" in tdk or "no_goal" in tdk: is_goal = False
        round_no = det.get("shootoutRound") or det.get("round") or rnd + 1
        rnd = int(round_no)
        shootout.append({"round":rnd,"teamAbbrev":team,"shooter":sc,"result":"goal" if is_goal else "miss"})

    return goals, shootout, (js_any.get("gameInfo") if isinstance(js_any, dict) else {})

TEAM_EMOJI = TEAM_EMOJI  # reuse

def send_telegram_text(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = requests.post(url, json=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram error: {js}")

def render_single(game_pk: int) -> str:
    sumjs = fetch_json_safe(GAME_SUMMARY_FMT.format(gamePk=game_pk))
    boxjs = None if sumjs else fetch_json_safe(GAME_BOX_FMT.format(gamePk=game_pk))
    goals, shootout, game_info = load_pbp_data(game_pk)

    if sumjs:
        g = sumjs.get("game", {})
        home, away = (g.get("homeTeam") or {}), (g.get("awayTeam") or {})
        hscore, ascore = int(home.get("score",0)), int(away.get("score",0))
    elif boxjs:
        home, away = (boxjs.get("homeTeam") or {}).get("team", {}) or {}, (boxjs.get("awayTeam") or {}).get("team", {}) or {}
        hscore = int((boxjs.get("homeTeam") or {}).get("score",0))
        ascore = int((boxjs.get("awayTeam") or {}).get("score",0))
    else:
        home = (game_info.get("homeTeam") or {})
        away = (game_info.get("awayTeam") or {})
        h_ab = (home.get("abbrev") or "").upper()
        a_ab = (away.get("abbrev") or "").upper()
        hscore = sum(1 for ev in goals if (ev.get("teamAbbrev") or "").upper()==h_ab)
        ascore = sum(1 for ev in goals if (ev.get("teamAbbrev") or "").upper()==a_ab)

    h_ab = (home.get("abbrev") or home.get("triCode") or "").upper()
    a_ab = (away.get("abbrev") or away.get("triCode") or "").upper()
    h_name, a_name = TEAM_RU.get(h_ab, h_ab), TEAM_RU.get(a_ab, a_ab)
    h_emoji, a_emoji = TEAM_EMOJI.get(h_ab, "•"), TEAM_EMOJI.get(a_ab, "•")

    def record_of(t: Dict[str,Any]) -> str:
        r = (t.get("record") or {})
        return f"{r.get('wins',0)}-{r.get('losses',0)}-{r.get('ot',0)}" if r else "—"

    h_rec = record_of(home)
    a_rec = record_of(away)

    # sports.ru русские фамилии
    ru_map = fetch_ru_name_map_for_match(home, away)

    header = [
        f"{h_emoji} «{h_name}» — {hscore} ({h_rec})",
        f"{a_emoji} «{a_name}» — {ascore} ({a_rec})",
        "",
    ]

    per_goals = {1:[],2:[],3:[]}
    ot_goals: List[str] = []
    so_lines: List[str] = []

    h_c = a_c = 0
    for ev in goals:
        # period/time
        per = (
            (ev.get("periodDescriptor") or {}).get("number")
            or (ev.get("period") or {}).get("number")
            or (ev.get("about")  or {}).get("periodNumber")
            or ev.get("period")
            or 0
        ) or 0
        tm  = mmss_to_ru(ev.get("time") if isinstance(ev.get("time"), str) else ev.get("time","00:00"))
        owner = (ev.get("teamAbbrev") or "").upper()
        if owner == h_ab: h_c += 1
        elif owner == a_ab: a_c += 1
        scorer = ru_last_or_keep((ev.get("scorer") or "").title(), ru_map)
        assists = [ru_last_or_keep(x.title(), ru_map) for x in (ev.get("assists") or [])]
        who = f"{scorer} ({', '.join(assists)})" if assists else (scorer or "—")
        line = f"{h_c}:{a_c} – {tm} {who}"
        if per in (1,2,3): per_goals[per].append(line)
        elif per == 4:     ot_goals.append(line)

    if shootout:
        so_h = so_a = 0
        for ev in shootout:
            team = ev["teamAbbrev"]
            shooter = ru_last_or_keep((ev["shooter"] or "").title(), ru_map)
            res = ev["result"]
            if res == "goal":
                if team == h_ab: so_h += 1
                elif team == a_ab: so_a += 1
            word = "гол" if res == "goal" else "мимо"
            so_lines.append(f"Раунд {ev['round']} — {shooter or '—'} — {word} (SO {so_h}:{so_a})")

    def per_block(title: str, arr: List[str]) -> List[str]:
        out=[title]
        out += (arr or ["Голов не было"])
        out.append("")
        return out

    body=[]
    body += per_block("<i>1-й период</i>", per_goals[1])
    body += per_block("<i>2-й период</i>", per_goals[2])
    body += per_block("<i>3-й период</i>", per_goals[3])
    if ot_goals: body += per_block("<i>Овертайм</i>", ot_goals)
    if so_lines: body += per_block("<i>Буллиты</i>", so_lines)

    txt = []
    txt.append("<tg-spoiler>")
    txt.extend(header)
    txt.extend(body)
    txt.append("</tg-spoiler>")
    return "\n".join(txt).replace("\n\n\n","\n\n").strip()

def send(gid: int):
    text = render_single(gid)
    dbg("Single match preview:\n" + text[:500] + ("…" if len(text) > 500 else ""))
    send_telegram_text(text)
    print("OK (posted 1)")

def main():
    gid = None
    if GAME_PK_ENV:
        try: gid = int(GAME_PK_ENV)
        except Exception: gid = None
    if not gid and GAME_QUERY:
        gid = resolve_game_pk_from_query(GAME_QUERY)
    if not gid:
        print("[ERR] provide GAME_PK or GAME_QUERY", file=sys.stderr)
        return
    send(gid)

if __name__ == "__main__":
    main()
