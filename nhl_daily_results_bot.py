#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, time, textwrap, re
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup

API = "https://api-web.nhle.com"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NHLDailyBot/1.3; +github)",
    "Accept": "application/json, text/plain, */*",
}

SCHEDULE_FMT   = API + "/v1/schedule/{ymd}"
STANDINGS_NOW  = API + "/v1/standings/now"
GAME_PBP_FMT   = API + "/v1/gamecenter/{gamePk}/play-by-play"

REPORT_DATE_LOCAL = (os.getenv("REPORT_DATE_LOCAL") or "").strip()
REPORT_TZ         = (os.getenv("REPORT_TZ") or os.getenv("REPORT_DATE_TZ") or "Europe/Amsterdam").strip()
DRY_RUN           = (os.getenv("DRY_RUN") or "0").strip() == "1"
DEBUG_VERBOSE     = (os.getenv("DEBUG_VERBOSE") or "1").strip() == "1"

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID   = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

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
            dbg(f"retry {i+1}/3 for {url}: {repr(e)}")
            time.sleep(0.75 * (i+1))
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

# ---------- sports.ru URL генератор (Utah/Vegas, обе ориентации, /stat) ----------
def _slugify_en(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s

SPORTSRU_TEAM_SLUGS = {
    "VGK": ["vegas","vegas-golden-knights"],
    "UTA": ["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
    "UTH": ["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
    "UTAH":["utah-mammoth","utah","utah-hc","utah-hockey-club","utah-hc-nhl"],
}

def _team_slug_variants_for_sportsru(team: Dict[str,Any]) -> List[str]:
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

def gen_sportsru_match_urls(home_team: Dict[str,Any], away_team: Dict[str,Any]) -> List[str]:
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
        ru_last = txt.split()[-1]
        en = (a.get("title") or a.get("data-name") or a.get("data-player-name") or "").strip()
        if not en:
            href = a.get("href") or ""
            m = re.search(r"/players/([\w-]+)/", href)
            if m: en = m.group(1).replace("-", " ")
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

# ---------- Телеграм ----------
def send_telegram_text(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if DRY_RUN:
        print("[DRY RUN] " + textwrap.shorten(text.replace("\n"," "), 200, placeholder="…"))
        return
    r = requests.post(url, json=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram error: {js}")

# ---------- Вспомогательные ----------
def parse_ymd_in_tz(ymd: str, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    d = date.fromisoformat(ymd)
    start = datetime.combine(d, dtime(0,0), tzinfo=tz)
    end   = datetime.combine(d, dtime(23,59,59), tzinfo=tz)
    return start, end

def fetch_schedule_day(ymd: date) -> List[Dict[str,Any]]:
    js = http_get_json(SCHEDULE_FMT.format(ymd=ymd.isoformat()))
    games = js.get("games")
    if games is None:
        games=[]
        for w in (js.get("gameWeek") or []):
            games.extend(w.get("games") or [])
    return games or []

def start_dt_in_tz(g: Dict[str,Any], tz: ZoneInfo) -> Optional[datetime]:
    utc = g.get("startTimeUTC") or g.get("startTime") or g.get("gameDate")
    if not utc: return None
    try:
        return datetime.fromisoformat(utc.replace("Z","+00:00")).astimezone(tz)
    except Exception:
        return None

def is_final(g: Dict[str,Any]) -> bool:
    st = (g.get("gameState") or g.get("gameStatus") or "").upper()
    return st in ("FINAL","OFF")

def team_block(g: Dict[str,Any], side: str) -> Dict[str,Any]:
    t = (g.get(f"{side}Team") or {})
    ab = t.get("abbrev") or t.get("triCode")
    if not ab and t.get("id"):
        ab = t.get("teamAbbrev")
    t["abbrev"] = (ab or "").upper()
    return t

def fetch_standings_map() -> Dict[str,Tuple[int,int,int]]:
    try:
        js = http_get_json(STANDINGS_NOW)
    except Exception as e:
        dbg(f"standings fetch failed: {e!r}")
        return {}
    out={}
    for conf in js.get("standings", []):
        for div in conf.get("divisions", []):
            for team in div.get("teams", []):
                ab = (team.get("teamAbbrev") or team.get("abbrev") or "").upper()
                rec = team.get("record") or {}
                out[ab] = (rec.get("wins",0), rec.get("losses",0), rec.get("ot",0))
    dbg(f"standings map built: {len(out)}")
    return out

def fmt_record(rec: Tuple[int,int,int]) -> str:
    return f"{rec[0]}-{rec[1]}-{rec[2]}"

def mmss_to_ru(mmss: str) -> str:
    return (mmss or "00:00").replace(":", ".")

# ---------- PBP загрузка (нормализация list/dict БЕЗУСЛОВНО) ----------
def _extract_period(ev: Dict[str,Any]) -> int:
    return (
        (ev.get("periodDescriptor") or {}).get("number")
        or (ev.get("period") or {}).get("number")
        or (ev.get("about")  or {}).get("periodNumber")
        or 0
    ) or 0

def _extract_time(ev: Dict[str,Any]) -> str:
    return (
        ev.get("timeInPeriod")
        or (ev.get("about") or {}).get("periodTime")
        or "00:00"
    )

def _extract_team_abbrev(ev: Dict[str,Any]) -> str:
    return (
        (ev.get("details") or {}).get("eventOwnerTeamAbbrev")
        or ev.get("teamAbbrev")
        or (ev.get("team") or {}).get("abbrev")
        or ""
    )

def _extract_scorer_last(ev: Dict[str,Any]) -> str:
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

def _extract_assists_last_list(ev: Dict[str,Any]) -> List[str]:
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

def load_pbp_data(game_pk: int) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    js = http_get_json(GAME_PBP_FMT.format(gamePk=game_pk))

    # НОРМАЛИЗАЦИЯ: делаем plays_obj словарём при любом входе (dict/list/что угодно)
    plays_obj: Dict[str,Any] = {"scoringPlays": [], "allPlays": [], "shootoutPlays": []}
    if isinstance(js, dict):
        raw = js.get("plays")
        if isinstance(raw, dict):
            # обычный вариант
            plays_obj = {"scoringPlays": raw.get("scoringPlays") or [],
                         "allPlays":     raw.get("allPlays")     or [],
                         "shootoutPlays":raw.get("shootoutPlays") or []}
        elif isinstance(raw, list):
            # редкий вариант: "plays" это список
            plays_obj["allPlays"] = raw
        else:
            # иногда нужные события лежат на верхнем уровне
            plays_obj["allPlays"] = js.get("allPlays") or []
    elif isinstance(js, list):
        # полностью список
        plays_obj["allPlays"] = js
    else:
        # неизвестный формат — оставим пустым
        pass

    scoring = plays_obj.get("scoringPlays") or []
    allplays = plays_obj.get("allPlays") or []
    shootout_src = plays_obj.get("shootoutPlays") or []

    goals: List[Dict[str,Any]] = []
    for ev in scoring:
        per = _extract_period(ev)
        tm  = _extract_time(ev)
        owner = _extract_team_abbrev(ev)
        scorer = _extract_scorer_last(ev)
        assists = _extract_assists_last_list(ev)
        goals.append({
            "period": per, "time": tm, "teamAbbrev": owner,
            "scorer": scorer, "assists": assists
        })

    # Буллиты: если спец-список пуст, просканируем allPlays
    raw_so = list(shootout_src)
    if not raw_so:
        for ev in allplays:
            per = _extract_period(ev)
            ptype = (ev.get("periodDescriptor") or {}).get("periodType") \
                    or (ev.get("about") or {}).get("ordinalNum") or ""
            if per >= 5 or str(ptype).upper() == "SO":
                raw_so.append(ev)

    shootout: List[Dict[str,Any]] = []
    rnd = 0
    for ev in raw_so:
        team = _extract_team_abbrev(ev).upper()
        shooter = _extract_scorer_last(ev)
        tdk = (ev.get("typeDescKey") or "").lower()
        det = ev.get("details") or {}
        is_goal = bool(det.get("isGoal"))
        if "goal" in tdk: is_goal = True
        if "miss" in tdk or "no_goal" in tdk: is_goal = False
        round_no = det.get("shootoutRound") or det.get("round") or rnd + 1
        rnd = int(round_no)
        shootout.append({
            "round": rnd,
            "teamAbbrev": team,
            "shooter": shooter,
            "result": "goal" if is_goal else "miss",
        })

    return goals, shootout

# ---------- sports.ru имена ----------
def ru_last_or_keep(en_last: str, ru_map: Dict[str,str]) -> str:
    if not en_last:
        return ""
    return ru_map.get(en_last, en_last)

# ---------- Рендер одной игры ----------
def render_game_block(g: Dict[str,Any], standings: Dict[str,Tuple[int,int,int]]) -> str:
    home = team_block(g, "home")
    away = team_block(g, "away")
    h_ab, a_ab = home["abbrev"], away["abbrev"]
    h_emoji, a_emoji = TEAM_EMOJI.get(h_ab, "•"), TEAM_EMOJI.get(a_ab, "•")
    h_name, a_name   = TEAM_RU.get(h_ab, h_ab), TEAM_RU.get(a_ab, a_ab)

    h_score = (g.get("homeTeam") or {}).get("score", 0)
    a_score = (g.get("awayTeam") or {}).get("score", 0)
    h_rec = fmt_record(standings.get(h_ab, (0,0,0)))
    a_rec = fmt_record(standings.get(a_ab, (0,0,0)))

    goals, shootout = load_pbp_data(g["id"])
    ru_map = fetch_ru_name_map_for_match(home, away)

    header = [
        f"{h_emoji} «{h_name}» — {h_score} ({h_rec})",
        f"{a_emoji} «{a_name}» — {a_score} ({a_rec})",
        "",
    ]

    per_goals: Dict[int, List[str]] = {1:[],2:[],3:[]}
    ot_goals: List[str] = []
    so_lines: List[str] = []

    h_c, a_c = 0, 0
    for ev in goals:
        per = int(ev.get("period",0) or 0)
        tm  = mmss_to_ru(ev.get("time"))
        owner = (ev.get("teamAbbrev") or "").upper()
        if owner == h_ab: h_c += 1
        elif owner == a_ab: a_c += 1
        scorer = ru_last_or_keep((ev.get("scorer") or "").title(), ru_map)
        assists = [ru_last_or_keep(x.title(), ru_map) for x in (ev.get("assists") or [])]
        who = f"{scorer} ({', '.join(assists)})" if assists else (scorer or "—")
        line = f"{h_c}:{a_c} – {tm} {who}"
        if per in (1,2,3):
            per_goals[per].append(line)
        elif per == 4:
            ot_goals.append(line)

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

    def add_period(title: str, arr: List[str], out: List[str]):
        out.append(title)
        if arr: out.extend(arr)
        else:   out.append("Голов не было")
        out.append("")

    body: List[str] = []
    add_period("<i>1-й период</i>", per_goals[1], body)
    add_period("<i>2-й период</i>", per_goals[2], body)
    add_period("<i>3-й период</i>", per_goals[3], body)
    if ot_goals:
        add_period("<i>Овертайм</i>", ot_goals, body)
    if so_lines:
        add_period("<i>Буллиты</i>", so_lines, body)

    full = []
    full.append("<tg-spoiler>")
    full.extend(header)
    full.extend(body)
    full.append("</tg-spoiler>")
    return "\n".join(full).replace("\n\n\n","\n\n").strip()

# ---------- Рендер дня ----------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def month_ru(m: int) -> str: return RU_MONTHS.get(m, "")

def build_day_text(ymd: str, tz: str) -> List[str]:
    tzinfo = ZoneInfo(tz)
    if not ymd:
        base_local = datetime.now(tzinfo).date()
        ymd = base_local.isoformat()
    else:
        base_local = date.fromisoformat(ymd)

    dbg(f"Daily summary for {ymd} in {tz}")
    start = datetime.combine(base_local, dtime(0,0), tzinfo=tzinfo)
    end   = datetime.combine(base_local, dtime(23,59,59), tzinfo=tzinfo)

    raw = fetch_schedule_day(base_local - timedelta(days=1)) \
        + fetch_schedule_day(base_local) \
        + fetch_schedule_day(base_local + timedelta(days=1))

    games = []
    for g in raw:
        dt = start_dt_in_tz(g, tzinfo)
        if not dt: continue
        if start <= dt <= end and is_final(g):
            games.append(g)
    dbg(f"Collected FINAL games: {len(games)}")

    standings = fetch_standings_map()

    if not games:
        return [f"🗓 Регулярный чемпионат НХЛ • {base_local.day} {month_ru(base_local.month)} • матчей нет"]

    head = f"🗓 Регулярный чемпионат НХЛ • {base_local.day} {month_ru(base_local.month)} • {len(games)} матчей\n\nРезультаты надёжно спрятаны 👇"
    sep = "—" * 66

    blocks = []
    for g in games:
        blocks.append(sep)
        blocks.append(render_game_block(g, standings))

    txt = head + "\n" + "\n".join(blocks)
    parts: List[str] = []
    cur, cur_len = [], 0
    for line in txt.splitlines():
        n = len(line) + 1
        if cur_len + n > 3500:
            parts.append("\n".join(cur))
            cur, cur_len = [line], n
        else:
            cur.append(line); cur_len += n
    if cur: parts.append("\n".join(cur))
    dbg(f"Telegram parts: {len(parts)}")
    return parts

def main():
    parts = build_day_text(REPORT_DATE_LOCAL, REPORT_TZ)
    for i, part in enumerate(parts, 1):
        if i == 1:
            send_telegram_text(part)
        else:
            send_telegram_text(f"…продолжение (часть {i}/{len(parts)})\n\n{part}")

if __name__ == "__main__":
    main()
