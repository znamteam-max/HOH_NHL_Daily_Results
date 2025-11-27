#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Single Result → Telegram

Фиксы:
- Поиск по GAME_QUERY: "YYYY-MM-DD SEA - NYI" или inputs из workflow (HOME - AWAY / AWAY@HOME)
- Если /game-summary даёт 404 — падаем на /play-by-play и /boxscore
- Те же генераторы sports.ru URL (Utah Mammoth / Vegas), имена голов/ассистов
- Сообщение формируется даже при отсутствии меты — из PBP
- Если ни GAME_PK, ни GAME_QUERY — можно использовать как «детектор» (но по умолчанию просто откажем, чтобы не спамить)

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  GAME_PK — если известен
  GAME_QUERY — "YYYY-MM-DD HOME - AWAY"
  DEBUG_VERBOSE – "1" для логов
"""

from __future__ import annotations
import os, sys, time, re, textwrap
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

API = "https://api-web.nhle.com"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NHLSingleBot/1.1; +github)",
    "Accept": "application/json, text/plain, */*",
}

SCHEDULE_FMT = API + "/v1/schedule/{ymd}"
GAME_SUMMARY_FMT = API + "/v1/gamecenter/{gamePk}/game-summary"
GAME_PBP_FMT = API + "/v1/gamecenter/{gamePk}/play-by-play"
GAME_BOX_FMT = API + "/v1/gamecenter/{gamePk}/boxscore"

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
DEBUG_VERBOSE = (os.getenv("DEBUG_VERBOSE") or "1").strip() == "1"

GAME_PK_ENV = (os.getenv("GAME_PK") or "").strip()
GAME_QUERY = (os.getenv("GAME_QUERY") or "").strip()

def dbg(msg: str):
    if DEBUG_VERBOSE:
        print(f"[DBG] {msg}", flush=True)

def _get_with_retries(url: str, *, timeout: int = 30, as_text: bool = False) -> Any:
    last = None
    for i in range(3):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text if as_text else r.json()
        except Exception as e:
            last = e
            dbg(f"retry {i+1}/3 for {url} after {0.75*(i+1):.2f}s: {repr(e)}")
            time.sleep(0.75 * (i + 1))
    if last:
        raise last

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)

# --- RU/emoji (сжато) ---
TEAM_RU = {
    "EDM":"Эдмонтон","DAL":"Даллас","DET":"Детройт","NSH":"Нэшвилл",
    "TBL":"Тампа-Бэй","CGY":"Калгари","FLA":"Флорида","PHI":"Филадельфия",
    "NJD":"Нью-Джерси","STL":"Сент-Луис","NYI":"Айлендерс","BOS":"Бостон",
    "PIT":"Питтсбург","BUF":"Баффало","WSH":"Вашингтон","WPG":"Виннипег",
    "CAR":"Каролина","NYR":"Рейнджерс","CBJ":"Коламбус","TOR":"Торонто",
    "CHI":"Чикаго","MIN":"Миннесота","COL":"Колорадо","SJS":"Сан-Хосе",
    "UTA":"Юта","MTL":"Монреаль","VGK":"Вегас","OTT":"Оттава",
    "ANA":"Анахайм","VAN":"Ванкувер","SEA":"Сиэтл","LAK":"Лос-Анджелес"
}
TEAM_EMOJI = {
    "EDM":"🛢️","DAL":"⭐️","DET":"🛡️","NSH":"🐯",
    "TBL":"⚡","CGY":"🔥","FLA":"🐆","PHI":"🛩",
    "NJD":"😈","STL":"🎵","NYI":"🏝️","BOS":"🐻",
    "PIT":"🐧","BUF":"🦬","WSH":"🦅","WPG":"✈️",
    "CAR":"🌪️","NYR":"🗽","CBJ":"💣","TOR":"🍁",
    "CHI":"🦅","MIN":"🌲","COL":"⛰️","SJS":"🦈",
    "UTA":"🧊","MTL":"🇨🇦","VGK":"🎰","OTT":"🛡",
    "ANA":"🦆","VAN":"🐳","SEA":"🦑","LAK":"👑",
}

# --- Sports.ru slug generator (Utah/Vegas) ---
def _slugify_en(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s

SPORTSRU_TEAM_SLUGS = {
    "VGK": ["vegas", "vegas-golden-knights"],
    "UTA": ["utah-mammoth", "utah", "utah-hc", "utah-hockey-club", "utah-hc-nhl"],
    "UTH": ["utah-mammoth", "utah", "utah-hc", "utah-hockey-club", "utah-hc-nhl"],
    "UTAH": ["utah-mammoth", "utah", "utah-hc", "utah-hockey-club", "utah-hc-nhl"],
}

def _team_slug_variants_for_sportsru(team: Dict[str, Any]) -> List[str]:
    v: List[str] = []
    abbr = (team.get("abbrev") or team.get("triCode") or "").upper()
    if abbr in SPORTSRU_TEAM_SLUGS:
        v.extend(SPORTSRU_TEAM_SLUGS[abbr])

    place = _slugify_en(team.get("placeName") or team.get("city") or "")
    nick  = _slugify_en(team.get("teamName") or team.get("name") or "")
    if place and nick: v.append(f"{place}-{nick}")
    if nick: v.append(nick)
    if place and place not in v: v.append(place)

    seen=set(); out=[]
    for x in v:
        if x and x not in seen:
            out.append(x); seen.add(x)
    return out

def gen_sportsru_match_urls(home_team: Dict[str, Any], away_team: Dict[str, Any]) -> List[str]:
    base = "https://www.sports.ru/hockey/match"
    hs = _team_slug_variants_for_sportsru(home_team)
    as_ = _team_slug_variants_for_sportsru(away_team)
    urls=[]
    for h in hs:
        for a in as_:
            urls += [
                f"{base}/{h}-vs-{a}/",
                f"{base}/{a}-vs-{h}/",
                f"{base}/{h}-vs-{a}/stat/",
                f"{base}/{a}-vs-{h}/stat/",
            ]
    seen=set(); out=[]
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def try_parse_sportsru_names(url: str) -> Dict[str,str]:
    try:
        html = http_get_text(url, timeout=25)
    except Exception as e:
        dbg(f"sports.ru fetch fail {url}: {e!r}")
        return {}
    soup = BeautifulSoup(html, "html.parser")
    ru_names: Dict[str,str] = {}
    for a in soup.select("a[href*='/hockey/players/'], a[href*='/hockey/player/']"):
        txt = (a.get_text(strip=True) or "")
        if not txt: continue
        parts = txt.split()
        ru_last = parts[-1]
        en = (a.get("title") or a.get("data-name") or a.get("data-player-name") or "").strip()
        if not en:
            href = a.get("href") or ""
            m = re.search(r"/players/([\w-]+)/", href)
            if m:
                en = m.group(1).replace("-", " ")
        en = en.strip()
        if en:
            en_last = en.split()[-1].title()
            if en_last and ru_last:
                ru_names[en_last] = ru_last
    if ru_names:
        dbg(f"sports.ru names extracted from {url}: {len(ru_names)}")
    return ru_names

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

# --- Telegram ---
def send_telegram_text(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram error: {js}")

# --- NHL helpers ---
def resolve_game_pk_from_query(q: str) -> Optional[int]:
    """
    q: 'YYYY-MM-DD HOME - AWAY' (HOME/away tri-codes)
    Ищет матч ±1 день от указанной даты.
    """
    try:
        ymd, pair = q.split(" ", 1)
        base = date.fromisoformat(ymd)
    except Exception:
        return None
    # HOME - AWAY
    home, away = "", ""
    if "@" in pair:
        # AWAY@HOME
        a, h = pair.split("@", 1)
        away, home = a.strip().upper(), h.strip().upper()
    elif "-" in pair:
        left, right = pair.split("-", 1)
        home, away = left.strip().upper(), right.strip().upper()
    else:
        return None

    def find_on_day(d: date) -> Optional[int]:
        js = http_get_json(SCHEDULE_FMT.format(ymd=d.isoformat()))
        games = js.get("games")
        if games is None:
            games = []
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

def fetch_summary(game_pk: int) -> Optional[Dict[str,Any]]:
    try:
        return http_get_json(GAME_SUMMARY_FMT.format(gamePk=game_pk))
    except Exception as e:
        dbg(f"summary 404/err for {game_pk}: {e!r}")
        return None

def fetch_pbp(game_pk: int) -> Dict[str,Any]:
    try:
        return http_get_json(GAME_PBP_FMT.format(gamePk=game_pk))
    except Exception as e:
        dbg(f"pbp err for {game_pk}: {e!r}")
        return {}

def fetch_box(game_pk: int) -> Dict[str,Any]:
    try:
        return http_get_json(GAME_BOX_FMT.format(gamePk=game_pk))
    except Exception as e:
        dbg(f"box err for {game_pk}: {e!r}")
        return {}

def mmss_to_ru(mmss: str) -> str:
    return mmss.replace(":", ".")

def ru_last_or_keep(en_last: str, ru_map: Dict[str,str]) -> str:
    if not en_last:
        return ""
    return ru_map.get(en_last, en_last)

def render_from_meta_or_pbp(game_pk: int) -> str:
    sumjs = fetch_summary(game_pk)
    pbpjs = fetch_pbp(game_pk) if not sumjs else {}
    boxjs = fetch_box(game_pk) if not sumjs else {}

    # teams (summary → box → pbp)
    def pick_teams() -> Tuple[Dict[str,Any], Dict[str,Any], int, int]:
        if sumjs:
            g = sumjs.get("game", {})
            home = (g.get("homeTeam") or {})
            away = (g.get("awayTeam") or {})
            hs = int(home.get("score", 0)); as_ = int(away.get("score", 0))
            return home, away, hs, as_
        if boxjs:
            home = (boxjs.get("homeTeam") or {}).get("team", {}) or {}
            away = (boxjs.get("awayTeam") or {}).get("team", {}) or {}
            hs   = int((boxjs.get("homeTeam") or {}).get("score", 0))
            as_  = int((boxjs.get("awayTeam") or {}).get("score", 0))
            return home, away, hs, as_
        # pbp
        g = (pbpjs or {}).get("gameInfo", {})
        home = (g.get("homeTeam") or {})
        away = (g.get("awayTeam") or {})
        # итоговый счёт попробуем по play-by-play
        hs = as_ = 0
        for ev in (pbpjs or {}).get("plays", {}).get("scoringPlays", []):
            owner = (ev.get("details") or {}).get("eventOwnerTeamAbbrev") or ev.get("teamAbbrev") or ""
            if owner.upper() == (home.get("abbrev") or "").upper():
                hs += 1
            elif owner.upper() == (away.get("abbrev") or "").upper():
                as_ += 1
        return home, away, hs, as_

    home, away, hscore, ascore = pick_teams()
    h_ab = (home.get("abbrev") or home.get("triCode") or "").upper()
    a_ab = (away.get("abbrev") or away.get("triCode") or "").upper()
    h_name = TEAM_RU.get(h_ab, home.get("name") or h_ab)
    a_name = TEAM_RU.get(a_ab, away.get("name") or a_ab)
    h_emoji = TEAM_EMOJI.get(h_ab, "")
    a_emoji = TEAM_EMOJI.get(a_ab, "")

    # records: summary->box->"—"
    def record_of(team: Dict[str,Any]) -> str:
        rec = (team.get("record") or {})
        if rec:
            return f"{rec.get('wins',0)}-{rec.get('losses',0)}-{rec.get('ot',0)}"
        return "—"

    h_rec = record_of(home)
    a_rec = record_of(away)

    # goals list (summary preferred, else pbp)
    goals: List[Dict[str,Any]] = []
    if sumjs:
        for ev in sumjs.get("summary", {}).get("scoring", {}).get("scoringPlays", []):
            try:
                per = ev["periodDescriptor"]["number"]
                tm  = ev["timeInPeriod"]
                owner = ev.get("teamAbbrev") or (ev.get("details") or {}).get("eventOwnerTeamAbbrev")
                scorer = (ev.get("scorer") or {}).get("lastName", "")
                assists = [a.get("lastName","") for a in (ev.get("assists") or [])]
                goals.append({"period":per,"time":tm,"teamAbbrev":owner,"scorer":scorer,"assists":assists})
            except Exception:
                continue
    else:
        for ev in (pbpjs or {}).get("plays", {}).get("scoringPlays", []):
            try:
                per = ev["periodDescriptor"]["number"]
                tm  = ev["timeInPeriod"]
                owner = ev.get("teamAbbrev") or (ev.get("details") or {}).get("eventOwnerTeamAbbrev")
                scorer = (ev.get("scorer") or {}).get("lastName", "")
                assists = [a.get("lastName","") for a in (ev.get("assists") or [])]
                goals.append({"period":per,"time":tm,"teamAbbrev":owner,"scorer":scorer,"assists":assists})
            except Exception:
                continue

    # RU-имена с sports.ru (Utah/Vegas fix by URL generator)
    ru_map = fetch_ru_name_map_for_match(home, away)

    # Рендер
    head = []
    head.append(f"{h_emoji} «{h_name}»")
    head.append(f"{a_emoji} «{a_name}»")
    head_txt = "\n".join(head)

    body_top = [
        f"<b>«{h_name}»: {hscore}</b> ({h_rec})",
        f"<b>«{a_name}»: {ascore}</b> ({a_rec})",
        "",
    ]

    per_goals = {1:[],2:[],3:[]}
    ot_goals: List[str] = []
    so_goals: List[str] = []

    h_c = a_c = 0
    for ev in goals:
        owner = (ev.get("teamAbbrev") or "").upper()
        if owner == h_ab:
            h_c += 1
        elif owner == a_ab:
            a_c += 1
        score_str = f"{h_c}:{a_c}"
        per = int(ev.get("period",0) or 0)
        tm = mmss_to_ru(ev.get("time","00:00"))
        scorer = ru_last_or_keep((ev.get("scorer") or "").title(), ru_map)
        assists = [ru_last_or_keep(x.title(), ru_map) for x in (ev.get("assists") or [])]
        who = f"{scorer} ({', '.join(assists)})" if assists else (scorer or "—")
        line = f"{score_str} – {tm} {who}"
        if per in (1,2,3):
            per_goals[per].append(line)
        elif per == 4:
            ot_goals.append(line)
        elif per >= 5:
            so_goals.append(line)

    def per_block(title: str, arr: List[str]) -> List[str]:
        out = [title]
        out += (arr or ["Голов не было"])
        out.append("")
        return out

    body_bottom: List[str] = []
    body_bottom += per_block("<i>1-й период</i>", per_goals[1])
    body_bottom += per_block("<i>2-й период</i>", per_goals[2])
    body_bottom += per_block("<i>3-й период</i>", per_goals[3])
    if ot_goals:
        body_bottom += per_block("<i>Овертайм</i>", ot_goals)
    if so_goals:
        body_bottom += per_block("<i>Буллиты</i>", so_goals)

    text = []
    text.append(head_txt)
    text.append("")
    text.append("<tg-spoiler>" + "\n".join(body_top + body_bottom).strip() + "</tg-spoiler>")
    return "\n".join(text).replace("\n\n\n","\n\n").strip()

# --- main ---
def main():
    global GAME_PK_ENV
    gid: Optional[int] = None

    if GAME_PK_ENV:
        try:
            gid = int(GAME_PK_ENV)
        except Exception:
            gid = None
    elif GAME_QUERY:
        gid = resolve_game_pk_from_query(GAME_QUERY)

    if not gid:
        print("[ERR] provide GAME_PK or GAME_QUERY", file=sys.stderr)
        return

    txt = render_from_meta_or_pbp(gid)
    dbg("Single match preview:\n" + txt[:500] + ("…" if len(txt) > 500 else ""))
    send_telegram_text(txt)
    print("OK (posted 1)")

if __name__ == "__main__":
    main()
