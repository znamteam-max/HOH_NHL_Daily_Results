#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Single Result Bot
Постит ОДИН завершённый матч в канал Telegram.

Требования формата (итог):
<b>«Хозяева»: N</b> (W-L-OT)
<b>«Гости»: M</b> (W-L-OT)

<i>1-й период</i>
...линии голов или "Голов не было"

<i>2-й период</i>
...

<i>3-й период</i>
...

<i>Овертайм</i>           # без №1 в регулярке; №2/№3 — если было несколько ОТ
...                      # только если были голы в ОТ

Победный буллит
H:A – Имя                # если матч решён в буллитах

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
- GAME_PK: обязательный (или передаётся из workflow-резолвера)
- GAME_DATE: (опц.) YYYY-MM-DD — для sports.ru url fallback
- DRY_RUN=0/1
"""

from __future__ import annotations
import os, re, json, time, textwrap
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

import requests

# ----------------- CONSTS -----------------
TG_API     = "https://api.telegram.org"
NHLE_BASE  = "https://api-web.nhle.com/v1"
STATS_BASE = "https://statsapi.web.nhl.com/api/v1"

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.8",
}

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
    "TOR":"Торонто","UTA":"UTA","VAN":"Ванкувер","VGK":"Вегас","WSH":"Вашингтон","WPG":"Виннипег",
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
    "UTA":"utah-hc","VAN":"vancouver-canucks","VGK":"vegas-golden-knights","WSH":"washington-capitals",
    "WPG":"winnipeg-jets",
}

# ----------------- ENV helpers -----------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

DRY_RUN = _env_bool("DRY_RUN", False)

# ----------------- HTTP with retries -----------------
def http_get_json(url: str, timeout: int = 25, retries: int = 3, backoff: float = 0.7) -> Any:
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout, headers=UA_HEADERS if "sports.ru" in url else None)
            r.raise_for_status()
            try:
                return r.json()
            except Exception:
                return json.loads(r.text or "{}")
        except Exception as e:
            last = e
            time.sleep(backoff * (i + 1))
    raise last

# ----------------- Data structures -----------------
@dataclass
class TeamRecord:
    wins: int
    losses: int
    ot: int
    points: int
    def as_str(self) -> str:
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
    time: str           # "MM.SS"
    team_for: str       # triCode
    home_goals: int
    away_goals: int
    scorer: str
    assists: List[str] = field(default_factory=list)

@dataclass
class SRUGoal:
    time: Optional[str]
    scorer_ru: Optional[str]
    assists_ru: List[str]

# ----------------- Standings -----------------
def fetch_standings_map() -> Dict[str, TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"
    data = http_get_json(url)
    teams: Dict[str, TeamRecord] = {}
    nodes = []
    if isinstance(data, dict):
        if isinstance(data.get("standings"), list): nodes = data["standings"]
        elif isinstance(data.get("records"), list): nodes = data["records"]
        elif isinstance(data.get("standings"), dict):
            nodes = data["standings"].get("overallRecords", []) or []
    elif isinstance(data, list):
        nodes = data
    for r in nodes:
        abbr = ""
        ta = r.get("teamAbbrev")
        if isinstance(ta, str): abbr = ta.upper()
        elif isinstance(ta, dict): abbr = (ta.get("default") or ta.get("tricode") or "").upper()
        if not abbr:
            abbr = (r.get("teamAbbrevTricode") or r.get("teamTriCode") or "").upper()
        rec = r.get("record") or r.get("overallRecord") or {}
        wins = int(rec.get("wins") or rec.get("gamesPlayedWins") or 0)
        losses = int(rec.get("losses") or rec.get("gamesPlayedLosses") or 0)
        ot = int(rec.get("ot") or rec.get("overtimeLosses") or 0)
        pts = int(r.get("points") or rec.get("points") or 0)
        if abbr:
            teams[abbr] = TeamRecord(wins, losses, ot, pts)
    return teams

# ----------------- Helpers -----------------
TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")

def _time_to_mmss(value: str) -> str:
    s = (value or "00:00").replace(" ", "").replace("‐","-")
    if ":" in s:
        mm, ss = s.split(":")[0:2]
    elif "." in s:
        mm, ss = s.split(".")[0:2]
    else:
        try:
            iv = int(s)
            mm, ss = divmod(iv, 60)
            return f"{mm:02d}.{ss:02d}"
        except Exception:
            return "00.00"
    try:
        return f"{int(mm):02d}.{int(ss):02d}"
    except Exception:
        return "00.00"

def _first_str(*cands):
    for c in cands:
        if isinstance(c, str) and c.strip():
            return c.strip()
        if isinstance(c, dict):
            for k in ("default","fullName","firstLastName","label"):
                v = c.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return ""

def _fmt_assists(assists: List[str]) -> str:
    if not assists: return ""
    return f" ({', '.join(assists)})"

# ----------------- sports.ru goals -----------------
def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or "")
    if not m: return None
    return f"{int(m.group(1)):02d}.{m.group(2)}"

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None

def parse_sportsru_goals_html(html: str, side: str) -> List[SRUGoal]:
    res: List[SRUGoal] = []
    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        ul = soup.select_one(f"ul.match-summary__goals-list--{side}") or \
             soup.select_one(f"ul.match-summary__goals-list.match-summary__goals-list--{side}")
        if not ul: return res
        for li in ul.find_all("li", recursive=False):
            anchors = [a.get_text(strip=True) for a in li.find_all("a")]
            scorer_ru = anchors[0] if anchors else None
            assists_ru = anchors[1:] if len(anchors) > 1 else []
            raw_text = li.get_text(" ", strip=True)
            time_ru = _extract_time(raw_text)
            res.append(SRUGoal(time_ru, scorer_ru, assists_ru))
        return res
    # fallback regex
    import re as _re
    ul_pat = _re.compile(
        r'<ul[^>]*class="[^"]*match-summary__goals-list[^"]*--%s[^"]*"[^>]*>(.*?)</ul>' % side,
        _re.S|_re.I
    )
    li_pat = _re.compile(r"<li\b[^>]*>(.*?)</li>", _re.S|_re.I)
    a_pat  = _re.compile(r"<a\b[^>]*>(.*?)</a>", _re.S|_re.I)
    m = ul_pat.search(html)
    if not m: return res
    ul_html = m.group(1)
    for li_html in li_pat.findall(ul_html):
        text = _re.sub(r"<[^>]+>", " ", li_html)
        time_ru = _extract_time(text)
        names = [_re.sub(r"\s+", " ", _re.sub(r"<[^>]+>", "", t)).strip()
                 for t in a_pat.findall(li_html)]
        scorer_ru = names[0] if names else None
        assists_ru = names[1:] if len(names) > 1 else []
        res.append(SRUGoal(time_ru, scorer_ru, assists_ru))
    return res

def fetch_sportsru_goals(home_tri: str, away_tri: str) -> Tuple[List[SRUGoal], List[SRUGoal], str]:
    hs = SPORTSRU_SLUG.get(home_tri); as_ = SPORTSRU_SLUG.get(away_tri)
    if not hs or not as_:
        return [], [], ""
    tried = []
    for a, b in ((hs, as_), (as_, hs)):
        url = f"https://www.sports.ru/hockey/match/{a}-vs-{b}/"
        tried.append(url)
        try:
            html = http_get_json(url) if False else requests.get(url, headers=UA_HEADERS, timeout=20).text
        except Exception:
            continue
        home_side = "home" if a == hs else "away"
        away_side = "away" if home_side == "home" else "home"
        hg = parse_sportsru_goals_html(html, home_side)
        ag = parse_sportsru_goals_html(html, away_side)
        if hg or ag:
            return hg, ag, url
    return [], [], ""

# ----------------- Game meta -----------------
def fetch_game_meta(gamePk: int) -> GameMeta:
    # 1) statsapi
    try:
        js = http_get_json(f"{STATS_BASE}/game/{gamePk}/feed/live", timeout=20)
        gd = js.get("gameData", {}) or
