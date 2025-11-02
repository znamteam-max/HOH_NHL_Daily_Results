# nhl_daily_results_bot.py
# -*- coding: utf-8 -*-

import os, sys, re, json
import datetime as dt
from zoneinfo import ZoneInfo
from html import unescape as html_unescape
from typing import List, Dict, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# =========================
# Конфиг
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Принудительная дата отчёта (UTC, YYYY-MM-DD). Если пусто — берём сегодня UTC и охватываем +/-1 день.
REPORT_DATE_UTC = os.getenv("REPORT_DATE_UTC", "").strip()

# Громкость логов
DEBUG = os.getenv("DEBUG", "1").strip() not in ("0", "false", "False")

# Статусы завершённых матчей в NHL API
COMPLETED_STATES = {"FINAL", "OFF"}

TEAM_EMOJI = {
    "VGK": "🎰","COL": "⛰️","WSH":"🦅","NYI":"🟠","ANA":"🦆","DET":"🔴",
    "BOS":"🐻","CAR":"🌪️","WPG":"✈️","PIT":"🐧","NSH":"🐯","CGY":"🔥","SJS":"🦈"
}
TEAM_RU = {
    "VGK": "«Вегас»","COL":"«Колорадо»","WSH":"«Вашингтон»","NYI":"«Айлендерс»",
    "ANA":"«Анахайм»","DET":"«Детройт»","BOS":"«Бостон»","CAR":"«Каролина»",
    "WPG":"«Виннипег»","PIT":"«Питтсбург»","NSH":"«Нэшвилл»","CGY":"«Калгари»",
    "SJS":"«Сан-Хосе»"
}

# teamId -> slug sports.ru
SPORTSRU_SLUG_BY_TEAMID = {
    54:"vegas-golden-knights", 21:"colorado-avalanche",
    15:"washington-capitals",   2:"new-york-islanders",
    24:"anaheim-ducks",        17:"detroit-red-wings",
    6:"boston-bruins",         12:"carolina-hurricanes",
    52:"winnipeg-jets",        5:"pittsburgh-penguins",
    18:"nashville-predators",  20:"calgary-flames",
    28:"san-jose-sharks",
}

RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

# =========================
# Лог и сессия
# =========================
def dbg(msg: str):
    if DEBUG:
        print(f"[DBG] {msg}")

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=6, connect=6, read=6, backoff_factor=0.5,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["GET","POST"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent":"HOH-NHL-Daily-Results/1.6"})
    return s

SESSION = make_session()

# =========================
# Утилиты
# =========================
def iso2date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

def day_span(center_utc: dt.date) -> List[dt.date]:
    return [center_utc - dt.timedelta(days=1), center_utc, center_utc + dt.timedelta(days=1)]

def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def mmss_to_seconds(mmss: str) -> int:
    m, s = mmss.split(":")
    return int(m)*60 + int(s)

def gameclock_to_pretty(total_seconds: int) -> str:
    m = total_seconds // 60
    s = total_seconds % 60
    return f"{m}.{s:02d}"

def period_number_to_title(n: int) -> str:
    if n == 1: return "<i>1-й период</i>"
    if n == 2: return "<i>2-й период</i>"
    if n == 3: return "<i>3-й период</i>"
    if n > 3:  return f"<i>Овертайм №{n-3}</i>"
    return "<i>Период</i>"

def ru_clean(name: str) -> str:
    if not name:
        return "—"
    txt = html_unescape(name)
    txt = re.sub(r"<[^>]+>", "", txt)
    txt = txt.replace("\xa0", " ").replace("&nbsp;", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    # оставляем кириллицу, дефис, пробел, апостроф
    txt = re.sub(r"[^А-Яа-яЁё \-ʼ'–]", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip(" -–")
    if not re.search(r"[А-Яа-яЁё]", txt):
        return "—"
    return txt

# =========================
# NHL API
# =========================
def fetch_schedule(date_ymd: str) -> List[dict]:
    url = f"https://api-web.nhle.com/v1/schedule/{date_ymd}"
    dbg(f"GET {url}")
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    games = []
    for day in data.get("gameWeek", []):
        games.extend(day.get("games", []))
    return games

def collect_completed_games(center_utc: dt.date) -> List[dict]:
    uniq = {}
    for d in day_span(center_utc):
        ds = d.strftime("%Y-%m-%d")
        for g in fetch_schedule(ds):
            st = g.get("gameState")
            gid = g.get("id")
            if st in COMPLETED_STATES:
                dbg(f"take completed: {gid} {st}")
                uniq[gid] = g
            else:
                dbg(f"skip not final: {gid} {st}")
    out = list(uniq.values())
    dbg(f"Collected unique FINAL games: {len(out)}")
    return out

def fetch_standings_now() -> Dict[int, Tuple[int,int,int,int]]:
    url = "https://api-web.nhle.com/v1/standings/now"
    dbg(f"GET {url}")
    out = {}
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        dbg(f"records error: {repr(e)}")
        return out

    buckets = []
    for key in ("standings","teamRecords","records","wildCard","overallStandings"):
        v = data.get(key)
        if isinstance(v, list):
            buckets.extend(v)
    if not buckets:
        for v in data.values():
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and "teamRecords" in item:
                        buckets.extend(item["teamRecords"])

    for rec in buckets:
        try:
            team = rec.get("team", {})
            tid = team.get("id")
            w  = int(rec.get("wins", rec.get("w",0)) or 0)
            l  = int(rec.get("losses", rec.get("l",0)) or 0)
            ot = int(rec.get("otLosses", rec.get("ot",0)) or 0)
            pts= int(rec.get("points", rec.get("p",0)) or 0)
            if tid:
                out[int(tid)] = (w,l,ot,pts)
        except Exception:
            pass
    dbg(f"records loaded: {len(out)}")
    return out

def _normalize_plays(pbp: dict) -> List[dict]:
    # разные версии структуры
    if isinstance(pbp, dict):
        if isinstance(pbp.get("plays"), list):
            return pbp["plays"]
        if isinstance(pbp.get("plays"), dict):
            for key in ("allPlays","plays","items","events"):
                if isinstance(pbp["plays"].get(key), list):
                    return pbp["plays"][key]
        for key in ("allPlays","playByPlay","events","items"):
            if isinstance(pbp.get(key), list):
                return pbp[key]
    if isinstance(pbp, list):
        return pbp
    return []

def fetch_pbp(game_id: int) -> dict:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    dbg(f"GET {url}")
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def extract_goals_from_pbp(pbp: dict, home_id: int, away_id: int) -> Tuple[List[dict], bool]:
    raw = _normalize_plays(pbp)
    goals = []
    shootout = False

    def parse_period(ev) -> Optional[int]:
        pd = ev.get("periodDescriptor") or {}
        if isinstance(pd, dict) and "number" in pd:
            try: return int(pd["number"])
            except: pass
        if "period" in ev:
            try: return int(ev["period"])
            except: pass
        det = ev.get("details") or {}
        if "period" in det:
            try: return int(det["period"])
            except: pass
        return None

    def parse_time_in_period(ev) -> Optional[str]:
        for k in ("timeInPeriod","clock","time"):
            v = ev.get(k)
            if isinstance(v,str) and ":" in v:
                return v
        det = ev.get("details") or {}
        v = det.get("timeInPeriod")
        if isinstance(v,str) and ":" in v:
            return v
        return None

    def parse_team_id(ev) -> Optional[int]:
        det = ev.get("details") or {}
        for k in ("eventOwnerTeamId","teamId","scoringTeamId"):
            val = det.get(k)
            if isinstance(val,int): return val
            if isinstance(val,str) and val.isdigit(): return int(val)
        team = ev.get("team") or {}
        if isinstance(team,dict):
            tid = team.get("id")
            if isinstance(tid,int): return tid
        return None

    def is_goal_event(ev) -> bool:
        td = str(ev.get("typeDescKey","")).lower()
        tc = str(ev.get("typeCode","")).lower()
        t  = str(ev.get("type","")).lower()
        det= ev.get("details") or {}
        et = str(det.get("eventTypeId","")).lower()
        return any(x == "goal" for x in (td, tc, t, et))

    for ev in raw:
        try:
            if not is_goal_event(ev):
                continue
            per  = parse_period(ev)
            mmss = parse_time_in_period(ev)
            tid  = parse_team_id(ev)
            if per is None or not mmss or tid is None:
                continue
            sec = mmss_to_seconds(mmss) + (per-1)*20*60
            goals.append({"period": per, "tsec": sec, "team_id": tid})
        except Exception:
            continue

    goals.sort(key=lambda x: x["tsec"])
    dbg(f"PBP goals: {len(goals)}")
    return goals, shootout

# =========================
# sports.ru парсер голов
# =========================
SPORTSRU_HOST = "https://www.sports.ru"

def _fix_encoding(resp: requests.Response) -> None:
    try:
        if not resp.encoding or "utf" not in (resp.encoding or "").lower():
            resp.encoding = resp.apparent_encoding or "utf-8"
    except Exception:
        resp.encoding = "utf-8"

def get_sportsru_match_goals_from_html(html: str) -> List[Tuple[int, str, List[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=False)
    lines = [l for l in text.splitlines()]
    time_re   = re.compile(r"\b(\d{1,2}:\d{2})\b")
    goal_re   = re.compile(r"Гол!\s*([^\n\r]+)")
    ass1_re   = re.compile(r"Ассистент:\s*([^\n\r]+)")
    assN_re   = re.compile(r"Ассистенты:\s*([^\n\r]+)")

    found = []
    for i, line in enumerate(lines):
        tm = time_re.search(line)
        if not tm:
            continue
        mmss = tm.group(1)
        window = "\n".join(lines[i:i+6])
        gm = goal_re.search(window)
        if not gm:
            continue
        raw_author = gm.group(1).strip()
        assists = []
        m1 = ass1_re.search(window)
        if m1:
            assists = [ru_clean(m1.group(1))]
        else:
            mN = assN_re.search(window)
            if mN:
                assists = [ru_clean(x) for x in mN.group(1).split(",") if x.strip()]

        tsec = mmss_to_seconds(mmss)
        author = ru_clean(raw_author)
        assists = [a for a in assists if a and a != "—"]
        found.append((tsec, author if author else "—", assists))

    found.sort(key=lambda x: x[0])
    uniq = []
    seen = set()
    for t,a,ass in found:
        key = (t, a, tuple(ass))
        if key in seen: 
            continue
        seen.add(key)
        uniq.append((t,a,ass))

    if uniq:
        ex = " | ".join([f"{gameclock_to_pretty(t)} {a}" for t,a,_ in uniq[:3]])
        dbg(f"sports.ru goals parsed: {len(uniq)} (ex: {ex})")
    return uniq

def get_sportsru_match_goals(slug: str) -> List[Tuple[int, str, List[str]]]:
    url1 = f"{SPORTSRU_HOST}/hockey/match/{slug}/lineups/"
    dbg(f"GET {url1}")
    r = SESSION.get(url1, timeout=20)
    if r.status_code == 200:
        _fix_encoding(r)
        data = get_sportsru_match_goals_from_html(r.text)
        if data:
            return data

    url2 = f"{SPORTSRU_HOST}/hockey/match/{slug}/"
    dbg(f"GET {url2}")
    r = SESSION.get(url2, timeout=20)
    if r.status_code == 200:
        _fix_encoding(r)
        data = get_sportsru_match_goals_from_html(r.text)
        if data:
            return data
    return []

def sportsru_goals_for_pair(home_id: int, away_id: int) -> List[Tuple[int, str, List[str]]]:
    sh = SPORTSRU_SLUG_BY_TEAMID.get(home_id)
    sa = SPORTSRU_SLUG_BY_TEAMID.get(away_id)
    if not sh or not sa:
        return []
    tried = []
    for slug in (f"{sa}-vs-{sh}", f"{sh}-vs-{sa}"):
        tried.append(slug)
        goals = get_sportsru_match_goals(slug)
        if goals:
            dbg(f"sports.ru matched: {slug} goals: {len(goals)}")
            return goals
    dbg(f"sports.ru no goals for pair {sa.upper()} {sh.upper()} tried: {tried}")
    return []

# =========================
# Формирование сообщения
# =========================
def build_game_block(g: dict, standings: Dict[int, Tuple[int,int,int,int]]) -> str:
    gid = int(g.get("id"))
    home = g.get("homeTeam", {})
    away = g.get("awayTeam", {})
    home_id = int(home.get("id"))
    away_id = int(away.get("id"))
    home_tri = home.get("abbrev") or home.get("triCode") or ""
    away_tri = away.get("abbrev") or away.get("triCode") or ""

    home_emoji = TEAM_EMOJI.get(home_tri, "🏒")
    away_emoji = TEAM_EMOJI.get(away_tri, "🏒")
    home_ru    = TEAM_RU.get(home_tri, f"«{home.get('name','Хозяева')}»")
    away_ru    = TEAM_RU.get(away_tri, f"«{away.get('name','Гости')}»")

    home_score = int(home.get("score", 0))
    away_score = int(away.get("score", 0))

    def rec_str(team_id: int) -> str:
        w,l,ot,pts = standings.get(team_id, (None,None,None,None))
        if w is None: return ""
        return f" ({w}-{l}-{ot}, {pts} о.)"

    header = [
        f"{home_emoji} {home_ru}: {home_score}{rec_str(home_id)}",
        f"{away_emoji} {away_ru}: {away_score}{rec_str(away_id)}",
    ]

    pbp = fetch_pbp(gid)
    goals_pbp, shootout = extract_goals_from_pbp(pbp, home_id, away_id)
    sr_goals = sportsru_goals_for_pair(home_id, away_id)

    # сопоставление по времени (±3 сек)
    def match_name_by_time(tsec: int) -> Tuple[str, List[str]]:
        if not sr_goals:
            return "—", []
        best = None; best_dd = 999
        for ts, author, assists in sr_goals:
            dd = abs(ts - tsec)
            if dd < best_dd:
                best = (author, assists); best_dd = dd
        if best and best_dd <= 3:
            a = ru_clean(best[0])
            b = [ru_clean(x) for x in (best[1] or []) if ru_clean(x) != "—"]
            return (a if a else "—"), b
        return "—", []

    home_run = 0; away_run = 0
    by_period: Dict[int, List[str]] = {}

    for ev in goals_pbp:
        per  = int(ev["period"])
        tsec = int(ev["tsec"])
        tid  = ev.get("team_id")
        if tid == home_id: home_run += 1
        elif tid == away_id: away_run += 1

        pretty = gameclock_to_pretty(tsec)
        author, assists = match_name_by_time(tsec)

        if assists:
            line = f"{home_run}:{away_run} – {pretty} {author} ({', '.join(assists)})"
        else:
            line = f"{home_run}:{away_run} – {pretty} {author}"
        by_period.setdefault(per, []).append(line)

    out = []
    out.extend(header)

    if not goals_pbp:
        out.append("")
        out.append("— события матча недоступны")
        return "\n".join(out)

    for per in sorted(by_period.keys()):
        out.append("")
        out.append(period_number_to_title(per))
        out.extend(by_period[per])

    return "\n".join(out)

def build_message() -> str:
    base = iso2date(REPORT_DATE_UTC) if REPORT_DATE_UTC else dt.datetime.now(dt.timezone.utc).date()
    games = collect_completed_games(base)
    standings = fetch_standings_now()

    tz_hel = ZoneInfo("Europe/Helsinki")
    today = dt.datetime.now(tz_hel).date()
    head_date = f"{today.day} {RU_MONTHS[today.month]}"

    title = f"🗓 Регулярный чемпионат НХЛ • {head_date} • {len(games)} матчей"
    lines = [title, "", "Результаты надёжно спрятаны 👇", "", "——————————————————", ""]

    if not games:
        return "\n".join(lines)

    def game_start_ts(g: dict) -> float:
        when = g.get("startTimeUTC")
        if when:
            try:
                return dt.datetime.fromisoformat(when.replace("Z","+00:00")).timestamp()
            except: pass
        return float(g.get("id", 0))
    games.sort(key=game_start_ts)

    for i,g in enumerate(games):
        lines.append(build_game_block(g, standings))
        if i != len(games)-1:
            lines.append("")
            lines.append("")

    return "\n".join(lines)

# =========================
# Telegram
# =========================
def _chunks(text: str, limit: int = 4000) -> List[str]:
    """Режем по пустым строкам, чтобы не ломать блоки."""
    if len(text) <= limit:
        return [text]
    parts, cur = [], []
    cur_len = 0
    blocks = text.split("\n\n")
    for b in blocks:
        add = (b if cur_len == 0 else "\n\n"+b)
        if cur_len + len(add) > limit:
            parts.append("".join(cur))
            cur = [b]
            cur_len = len(b)
        else:
            cur.append(add if cur_len == 0 else b)
            cur_len += len(add)
    if cur:
        parts.append("".join(cur))
    # safety — если вдруг блок монструозный
    flat = []
    for p in parts:
        if len(p) <= limit:
            flat.append(p)
        else:
            # грубо режем по \n
            s = p.split("\n")
            buf = ""
            for line in s:
                add = (line if not buf else "\n"+line)
                if len(buf) + len(add) > limit:
                    flat.append(buf)
                    buf = line
                else:
                    buf += add
            if buf:
                flat.append(buf)
    return flat

def send_telegram(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        print("No TELEGRAM_BOT_TOKEN/CHAT_ID in env", file=sys.stderr)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for idx, chunk in enumerate(_chunks(text, 4000), 1):
        dbg("POST Telegram sendMessage")
        r = SESSION.post(url, json={
            "chat_id": CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML",  # мы используем <i>…</i>
            "disable_web_page_preview": True
        }, timeout=20)
        if r.status_code >= 400:
            # Показать, что именно не понравилось Telegram
            try:
                err = r.json()
            except Exception:
                err = {"raw": r.text}
            print(f"ERROR: HTTP {r.status_code} Telegram sendMessage: {err}", file=sys.stderr)
            r.raise_for_status()

# =========================
# main
# =========================
if __name__ == "__main__":
    try:
        msg = build_message()
        send_telegram(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
