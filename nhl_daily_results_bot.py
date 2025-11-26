#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HOH · NHL Daily Results Bot (robust daily summary)

Фичи:
— Устойчивый сбор событий: statsapi → fallback на gamecenter/play-by-play
— Фильтрация матчей по локальному дню (REPORT_DATE_LOCAL + REPORT_DATE_TZ)
— Шапка дня с правильной формой «матч/матча/матчей»
— Для каждого матча: снаружи только эмодзи+названия, остальное под спойлером
— Жирные строки со счётом и рекордом (W-L-OT), очки убраны
— Заголовки периодов курсивом, перед каждым пустая строка, «Голов не было» — обычным
— Буллиты: «Победный буллит» + «итоговый счёт – Имя»

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TELEGRAM_THREAD_ID
  REPORT_DATE_LOCAL — YYYY-MM-DD (если пусто: авто)
  REPORT_DATE_TZ    — IANA TZ (если пусто: Europe/Amsterdam)
  DRY_RUN           — "true"/"1" для предпросмотра без отправки
"""

from __future__ import annotations
import os, re, json, time, textwrap
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, time as dtime, timedelta, timezone

import requests

# -------- Consts
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
    "TOR":"Торонто","VAN":"Ванкувер","VGK":"Вегас","WSH":"Вашингтон","WPG":"Виннипег",
    "UTA":"UTA","QUE":"QUE"
}

TEAM_EMOJI = {
    "ANA":"🦆","ARI":"🦂","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️","CBJ":"💣",
    "DAL":"⭐️","DET":"🛡️","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦","NSH":"🐯",
    "NJD":"😈","NYI":"🏝️","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈","SEA":"🦑","STL":"🎵",
    "TBL":"⚡","TOR":"🍁","VAN":"🐳","VGK":"🎰","WSH":"🦅","WPG":"✈️","UTA":"🐍","QUE":"🦊"
}

# -------- ENV helpers
def _env_str(name: str, default: str="") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

DRY_RUN = _env_bool("DRY_RUN", False)

# -------- HTTP with retries
def http_get_json(url: str, timeout: int = 30, retries: int = 3, backoff: float = 0.6) -> Any:
    last = None
    for i in range(retries):
        try:
            hdrs = UA_HEADERS if "sports.ru" in url else None
            r = requests.get(url, timeout=timeout, headers=hdrs)
            r.raise_for_status()
            try:
                return r.json()
            except Exception:
                return json.loads(r.text or "{}")
        except Exception as e:
            last = e
            time.sleep(backoff * (i+1))
    raise last

# -------- Data types
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
    time: str         # "MM.SS" elapsed
    team_for: str
    home_goals: int
    away_goals: int
    scorer: str
    assists: List[str] = field(default_factory=list)

# -------- Standings
def fetch_standings_map() -> Dict[str, TeamRecord]:
    url = f"{NHLE_BASE}/standings/now"
    data = http_get_json(url, timeout=25)
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
    print(f"[DBG] standings map built: {len(teams)}")
    return teams

# -------- Time helpers
def _plural_matches(n: int) -> str:
    n10 = n % 10
    n100 = n % 100
    if n10 == 1 and n100 != 11: return "матч"
    if 2 <= n10 <= 4 and not (12 <= n100 <= 14): return "матча"
    return "матчей"

def _resolve_local_day() -> Tuple[date, str]:
    tz = _env_str("REPORT_DATE_TZ", "Europe/Amsterdam").strip() or "Europe/Amsterdam"
    want = _env_str("REPORT_DATE_LOCAL","").strip()
    try:
        from zoneinfo import ZoneInfo
    except Exception:
        ZoneInfo = None

    if want:
        base = date.fromisoformat(want)
        return base, tz

    # авто: «вчера» в заданной TZ после локальной 06:00 считаем предыдущим днём закончившегося гейм-дня
    if ZoneInfo is None:
        now = datetime.utcnow()
        base = (now.date() - timedelta(days=1))
        return base, tz
    now_tz = datetime.now(ZoneInfo(tz))
    base = now_tz.date() if now_tz.hour >= 6 else (now_tz.date() - timedelta(days=1))
    return base, tz

# -------- Schedule & meta
def _fetch_sched(ymd: date) -> List[Dict[str,Any]]:
    url = f"{NHLE_BASE}/schedule/{ymd.isoformat()}"
    js = http_get_json(url, timeout=25)
    games = js.get("games")
    if games is None:
        weeks = js.get("gameWeek") or []
        games = []
        for w in weeks:
            games.extend(w.get("games") or [])
    return games or []

def _parse_dt_iso(s: str) -> Optional[datetime]:
    if not s: return None
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception:
        return None

def list_final_games_for_local_day(base_local: date, tz_str: str) -> List[GameMeta]:
    # Берём два дня (base и base-1), фильтруем по окну локального дня
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(tz_str)

    start = datetime.combine(base_local, dtime(0,0), tzinfo=tz)
    end   = datetime.combine(base_local, dtime(23,59,59), tzinfo=tz)

    raw: List[Dict[str,Any]] = []
    for d in (base_local - timedelta(days=1), base_local):
        raw.extend(_fetch_sched(d))

    games: Dict[int, GameMeta] = {}
    for g in raw:
        utc_s = g.get("startTimeUTC") or g.get("startTime") or g.get("gameDate")
        dt_utc = _parse_dt_iso(utc_s)
        if not dt_utc: continue
        dt_local = dt_utc.astimezone(tz)
        if not (start <= dt_local <= end): 
            continue
        state = (g.get("gameState") or g.get("gameStatus") or "").upper()
        if state not in ("FINAL","OFF"):
            continue
        gid = int(g.get("id") or g.get("gameId") or g.get("gamePk") or 0)
        if gid == 0: continue
        home = g.get("homeTeam", {}) or {}
        away = g.get("awayTeam", {}) or {}
        htri = (home.get("abbrev") or home.get("triCode") or home.get("teamAbbrev") or "").upper()
        atri = (away.get("abbrev") or away.get("triCode") or away.get("teamAbbrev") or "").upper()
        hscore = int(home.get("score") or 0)
        ascore = int(away.get("score") or 0)
        games[gid] = GameMeta(
            gamePk=gid, gameDateUTC=dt_utc, state=state,
            home_tri=htri, away_tri=atri, home_score=hscore, away_score=ascore
        )
    out = sorted(games.values(), key=lambda m: m.gameDateUTC)
    print(f"[DBG] collected FINAL in local day: {len(out)}")
    return out

# -------- PBP helpers
def _mmss_elapsed_from_remaining(mmss_remain: str, period_type: str) -> str:
    try:
        m, s = mmss_remain.split(":")
        rem = int(m)*60 + int(s)
    except Exception:
        t = mmss_remain.replace(":", ".")
        if re.match(r"^\d{1,2}\.\d{2}$", t):
            return t if len(t.split(".")[0])==2 else f"0{t}"
        return "00.00"
    total = 300 if period_type.upper()=="OVERTIME" else 1200
    el = max(0, total - rem)
    return f"{el//60:02d}.{el%60:02d}"

def fetch_events_for_game(meta: GameMeta) -> List[ScoringEvent]:
    # 1) statsapi
    try:
        url = f"{STATS_BASE}/game/{meta.gamePk}/feed/live"
        data = http_get_json(url, timeout=25)
        plays = (data.get("liveData", {}).get("plays", {}) or {})
        allPlays = plays.get("allPlays", []) or []
        idxs = plays.get("scoringPlays", []) or []
        events: List[ScoringEvent] = []
        h=a=0
        for i in idxs:
            if not (0 <= i < len(allPlays)): continue
            p = allPlays[i]
            res = p.get("result", {}) or {}
            if (res.get("eventTypeId") or "").upper() != "GOAL":
                continue
            about = p.get("about", {}) or {}
            period = int(about.get("period") or 0)
            ptype  = (about.get("periodType") or "REGULAR").upper()
            t = (about.get("periodTime") or "00:00").replace(":", ".")
            team = ((p.get("team", {}) or {}).get("triCode") or "").upper()

            hg = about.get("goals", {}).get("home")
            ag = about.get("goals", {}).get("away")
            if isinstance(hg, int) and isinstance(ag, int):
                h, a = hg, ag
            else:
                if team == meta.home_tri: h += 1
                elif team == meta.away_tri: a += 1

            scorer = ""
            assists: List[str] = []
            for pp in p.get("players", []) or []:
                role = (pp.get("playerType") or "").upper()
                name = (pp.get("player", {}) or {}).get("fullName") or ""
                if role == "SCORER": scorer = name
                elif role == "ASSIST" and name: assists.append(name)

            events.append(ScoringEvent(period, ptype, t, team, h, a, scorer, assists))
        if events:
            print(f"[DBG] statsapi PBP goals: {len(events)} for game {meta.gamePk}")
            return events
    except Exception as e:
        print(f"[DBG] statsapi fail for {meta.gamePk}: {repr(e)}")

    # 2) fallback: gamecenter PBP
    url = f"{NHLE_BASE}/gamecenter/{meta.gamePk}/play-by-play"
    js = http_get_json(url, timeout=25)
    plays = js.get("plays") or []

    def team_from_play(p: Dict[str, Any]) -> str:
        det = p.get("details") or {}
        cands = [
            det.get("eventOwnerTeamAbbrev"),
            det.get("teamAbbrev"),
            det.get("scoringTeamAbbrev"),
            (p.get("team") or {}).get("abbrev"),
            (p.get("scoringTeam") or {}).get("abbrev"),
        ]
        for v in cands:
            if v: return str(v).upper()
        return ""

    def extract_name(det: Dict[str,Any], player_fallbacks: List[Dict[str,Any]]) -> str:
        cands = [
            (det.get("scorerName") or {}).get("default"),
            det.get("scorerFullName"),
            (det.get("shootingPlayerName") or {}).get("default"),
            det.get("shootingPlayerFullName"),
        ]
        for v in cands:
            if v: return str(v)
        for pl in player_fallbacks:
            typ = (pl.get("typeDescKey") or "").lower()
            if typ in ("scorer","shootoutscorer","shootoutgoal","goal"):
                nm = (pl.get("playerName") or {}).get("default") or pl.get("playerFullName") or ""
                if nm: return nm
        return ""

    events: List[ScoringEvent] = []
    h=a=0
    for p in plays:
        tcode = (p.get("typeDescKey") or p.get("typeCode") or "").lower()
        if tcode not in ("goal", "shootoutgoal"):
            continue
        pd = p.get("periodDescriptor") or {}
        period = int(pd.get("number") or 0)
        ptype  = (pd.get("periodType") or "REGULAR").upper()

        raw_t  = p.get("timeRemaining") or p.get("timeInPeriod") or "00:00"
        t = _mmss_elapsed_from_remaining(raw_t, ptype)

        det = p.get("details") or {}
        team = team_from_play(p)

        # Текущий счёт — только REG/OT
        if ptype != "SHOOTOUT":
            if team == meta.home_tri: h += 1
            elif team == meta.away_tri: a += 1

        scorer = extract_name(det, det.get("playersInvolved") or [])
        assists: List[str] = []
        if det.get("assists"):
            for aobj in det.get("assists") or []:
                nm = (aobj.get("playerName") or {}).get("default") or aobj.get("playerFullName") or ""
                if nm: assists.append(nm)
        else:
            aux: List[str] = []
            for pl in det.get("playersInvolved") or []:
                if (pl.get("typeDescKey") or "").lower() == "assist":
                    nm = (pl.get("playerName") or {}).get("default") or pl.get("playerFullName") or ""
                    if nm: aux.append(nm)
            if aux: assists = aux

        events.append(ScoringEvent(period, ptype, t, team, h, a, scorer, assists))

    print(f"[DBG] gamecenter PBP goals: {len(events)} for game {meta.gamePk}")
    return events

# -------- Formatting
def _fmt_assists(assists: List[str]) -> str:
    if not assists: return ""
    return f" ({', '.join(assists)})"  # всегда одиночные скобки

def period_title(num: int, ptype: str, ot_seen: Dict[str,int]) -> str:
    t = ptype.upper()
    if t == "REGULAR":
        return f"<i>{num}-й период</i>"
    if t == "OVERTIME":
        n = ot_seen.get("n",0) + 1
        ot_seen["n"] = n
        return "<i>Овертайм</i>" if n==1 else f"<i>Овертайм №{n}</i>"
    if t == "SHOOTOUT":
        return "<i>Буллиты</i>"
    return f"<i>Период {num}</i>"

def line_goal(ev: ScoringEvent) -> str:
    score = f"{ev.home_goals}:{ev.away_goals}"
    who = ev.scorer or "—"
    return f"{score} – {ev.time} {who}{_fmt_assists(ev.assists)}"

def shootout_winner_line(meta: GameMeta, evs: List[ScoringEvent]) -> Optional[str]:
    so = [e for e in evs if e.period_type.upper()=="SHOOTOUT" and e.scorer]
    if not so: return None
    if meta.home_score > meta.away_score:
        winner = meta.home_tri
    elif meta.away_score > meta.home_score:
        winner = meta.away_tri
    else:
        winner = so[-1].team_for
    last = None
    for e in reversed(so):
        if e.team_for == winner:
            last = e.scorer
            break
    if not last: return None
    final_score = f"{meta.home_score}:{meta.away_score}"
    return f"Победный буллит\n{final_score} – {last}"

def build_match_block(meta: GameMeta, standings: Dict[str,TeamRecord], events: List[ScoringEvent]) -> str:
    he = TEAM_EMOJI.get(meta.home_tri, "")
    ae = TEAM_EMOJI.get(meta.away_tri, "")
    hn = TEAM_RU.get(meta.home_tri, meta.home_tri)
    an = TEAM_RU.get(meta.away_tri, meta.away_tri)
    hrec = standings.get(meta.home_tri).as_str() if meta.home_tri in standings else "0-0-0"
    arec = standings.get(meta.away_tri).as_str() if meta.away_tri in standings else "0-0-0"

    # Внешняя шапка (без счёта)
    outer = f"{he} «{hn}»\n{ae} «{an}»\n"

    # Внутренний спойлер: жирные счёт+рекорды
    inner_lines: List[str] = []
    inner_lines.append(f"<b>«{hn}»: {meta.home_score}</b> ({hrec})")
    inner_lines.append(f"<b>«{an}»: {meta.away_score}</b> ({arec})")

    # Группировка по периодам
    groups: Dict[Tuple[int,str], List[ScoringEvent]] = {}
    for ev in events:
        groups.setdefault((ev.period, ev.period_type), []).append(ev)

    ot_seen = {"n":0}
    for key in sorted(groups.keys(), key=lambda x: (x[0], 0 if x[1].upper()=="REGULAR" else 1 if x[1].upper()=="OVERTIME" else 2)):
        inner_lines.append("\n" + period_title(key[0], key[1], ot_seen))
        g = groups[key]
        if key[1].upper() == "SHOOTOUT":
            # Самих попыток не расписываем — ниже победный буллит
            continue
        if not g:
            inner_lines.append("Голов не было")
        else:
            for ev in g:
                inner_lines.append(line_goal(ev))

    so_line = shootout_winner_line(meta, events)
    if so_line:
        inner_lines.append("\n" + so_line)

    inner = "<tg-spoiler>" + "\n".join(inner_lines).strip() + "</tg-spoiler>"
    return outer + "\n" + inner

def header_ru(day: date, n_games: int) -> str:
    return f"🗓 Регулярный чемпионат НХЛ • {day.day} {MONTHS_RU[day.month]} • {n_games} {_plural_matches(n_games)}"

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

# -------- Telegram
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
            "parse_mode": "HTML",
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
        print(f"[DBG] TG HTTP={resp.status_code} JSON keys={list(data.keys()) if isinstance(data,dict) else type(data)}")
        if resp.status_code!=200 or not data.get("ok",False):
            print(f"[ERR] sendMessage failed: {data}")

# -------- Main
def main():
    base_local, tz = _resolve_local_day()
    print(f"[DBG] Daily summary for local day {base_local} in {tz}")
    games = list_final_games_for_local_day(base_local, tz)
    if not games:
        print("OK (нет FINAL игр в окне)"); return

    standings = fetch_standings_map()

    blocks: List[str] = []
    for meta in games:
        evs = fetch_events_for_game(meta)
        block = build_match_block(meta, standings, evs)
        blocks.append(block)

    text = header_ru(base_local, len(blocks)) + "\n\n" + "Результаты надёжно спрятаны 👇" + "\n\n" + "——————————————————\n" + ("\n\n——————————————————\n".join(blocks)).strip()
    print("[DBG] Preview 500:\n" + text[:500].replace("\n","¶") + "…")
    send_telegram_text(text)
    print("OK")

if __name__ == "__main__":
    main()
