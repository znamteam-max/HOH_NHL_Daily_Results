# nhl_daily_results_bot.py
# -*- coding: utf-8 -*-
import os, sys, re
import datetime as dt
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ===== Конфиг =====
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
FORCE_MSK_DATE = os.getenv("REPORT_DATE_MSK", "").strip()  # YYYY-MM-DD вручную
DEBUG = True

MSK = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")

RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}

def ru_plural(n:int, forms:tuple[str,str,str]) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

TEAM_EMOJI = {
    "VGK":"🎰","COL":"⛰️","WSH":"🦅","NYI":"🟠","ANA":"🦆","DET":"🔴",
    "MIN":"🌲","SJS":"🦈","WPG":"✈️","UTA":"🦣","CHI":"🦅","LAK":"👑",
    "NSH":"🐯","DAL":"⭐️","CGY":"🔥","NYR":"🗽","VAN":"🐳","EDM":"🛢️",
    "BOS":"🐻","CAR":"🌪️","PIT":"🐧"
}
TEAM_RU = {
    "VGK":"«Вегас»","COL":"«Колорадо»","WSH":"«Вашингтон»","NYI":"«Айлендерс»",
    "ANA":"«Анахайм»","DET":"«Детройт»","MIN":"«Миннесота»","SJS":"«Сан-Хосе»",
    "WPG":"«Виннипег»","UTA":"«Юта»","CHI":"«Чикаго»","LAK":"«Лос-Анджелес»",
    "NSH":"«Нэшвилл»","DAL":"«Даллас»","CGY":"«Калгари»","NYR":"«Рейнджерс»",
    "VAN":"«Ванкувер»","EDM":"«Эдмонтон»","BOS":"«Бостон»","CAR":"«Каролина»",
    "PIT":"«Питтсбург»"
}
SPORTS_SLUG = {
    "VGK":"vegas-golden-knights","COL":"colorado-avalanche","WSH":"washington-capitals",
    "NYI":"new-york-islanders","ANA":"anaheim-ducks","DET":"detroit-red-wings",
    "MIN":"minnesota-wild","SJS":"san-jose-sharks","WPG":"winnipeg-jets",
    "UTA":"utah-hc","CHI":"chicago-blackhawks","LAK":"los-angeles-kings",
    "NSH":"nashville-predators","DAL":"dallas-stars","CGY":"calgary-flames",
    "NYR":"new-york-rangers","VAN":"vancouver-canucks","EDM":"edmonton-oilers",
    "BOS":"boston-bruins","CAR":"carolina-hurricanes","PIT":"pittsburgh-penguins"
}

def dbg(*a):
    if DEBUG: print("[DBG]", *a, flush=True)

def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent":"HOH NHL Daily Results/1.2"})
    return s
S = make_session()

def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_iso_z(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z","+00:00"))

def sec_to_mmss(sec: int) -> str:
    m = sec // 60
    s = sec % 60
    return f"{m}.{s:02d}"

def period_from_abs(sec: int) -> int:
    if sec < 20*60: return 1
    if sec < 40*60: return 2
    if sec < 60*60: return 3
    return 3 + ((sec - 60*60) // (5*60) + 1)

def period_caption(idx: int) -> str:
    if idx == 1: return "_1-й период_"
    if idx == 2: return "_2-й период_"
    if idx == 3: return "_3-й период_"
    return f"_Овертайм №{idx-3}_"

# ---------- отчётная дата ----------
def resolve_report_date_msk() -> dt.date:
    if FORCE_MSK_DATE:
        try:
            d = dt.date.fromisoformat(FORCE_MSK_DATE)
            dbg("FORCE_MSK_DATE =", d)
            return d
        except Exception:
            print("ERROR: REPORT_DATE_MSK must be YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    return dt.datetime.now(MSK).date()

def fetch_schedule_dates_for_msk(report_d: dt.date):
    msk_start = dt.datetime(report_d.year, report_d.month, report_d.day, 0,0,tzinfo=MSK)
    msk_end   = dt.datetime(report_d.year, report_d.month, report_d.day,23,59,tzinfo=MSK)
    prev_msk  = msk_start - dt.timedelta(days=1)
    border    = dt.time(15,0)  # 15:00 МСК
    utc_dates = sorted({ msk_start.astimezone(UTC).date(),
                         msk_end.astimezone(UTC).date(),
                         prev_msk.astimezone(UTC).date() })
    return utc_dates, border

def load_nhl_schedule(utc_date: dt.date):
    url = f"https://api-web.nhle.com/v1/schedule/{ymd(utc_date)}"
    dbg("GET", url)
    r = S.get(url, timeout=20); r.raise_for_status()
    return r.json()

def collect_games_for_msk_day(report_d: dt.date):
    utc_dates, border = fetch_schedule_dates_for_msk(report_d)
    seen = set(); out = []
    for d in utc_dates:
        j = load_nhl_schedule(d)
        week = j.get("gameWeek") or []
        for day in week:
            for ev in (day.get("games") or []):
                try:
                    start_utc = parse_iso_z(ev.get("startTimeUTC"))
                except Exception:
                    continue
                start_msk = start_utc.astimezone(MSK)
                msk_date  = start_msk.date()
                take = (msk_date == report_d) or (
                    msk_date == (report_d - dt.timedelta(days=1)) and start_msk.time() >= border
                )
                if not take:
                    continue
                state = (ev.get("gameState") or "").upper()
                if state not in ("FINAL","OFF"):
                    dbg("skip not final:", ev.get("id") or ev.get("gameId"), state)
                    continue
                gid = ev.get("id") or ev.get("gameId")
                if not gid: continue
                if gid in seen: continue
                seen.add(gid)
                out.append(ev)
    dbg("Collected unique FINAL games:", len(out))
    return out

# ---------- standings (records) ----------
def load_team_records() -> dict:
    """
    Возвращает { 'VGK': '7-3-1', ... }
    """
    url = "https://api-web.nhle.com/v1/standings/now"
    try:
        dbg("GET", url)
        r = S.get(url, timeout=20); r.raise_for_status()
        j = r.json() or {}
        m = {}
        for row in (j.get("standings") or []):
            tri = row.get("teamAbbrev") or row.get("teamAbbrevDefault") or ""
            w = row.get("wins", 0)
            l = row.get("losses", 0)
            ot = row.get("otLosses", 0)
            if tri:
                m[tri] = f"{w}-{l}-{ot}"
        dbg("records loaded:", len(m))
        return m
    except Exception as e:
        dbg("records error:", repr(e))
        return {}

# ---------- NHL PBP ----------
def load_pbp(game_id: int):
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    dbg("GET", url)
    r = S.get(url, timeout=20); r.raise_for_status()
    return r.json()

def _iter_plays(pbp_json):
    plays = pbp_json.get("plays")
    if isinstance(plays, list): return plays
    plays = (pbp_json.get("playByPlay") or {}).get("plays")
    if isinstance(plays, list): return plays
    return []

def extract_goal_events(pbp_json, home_abbr, away_abbr):
    out = []
    for p in _iter_plays(pbp_json):
        t = (p.get("typeDescKey") or "").lower()
        if t != "goal": continue
        clock = p.get("timeInPeriod") or "00:00"
        try:
            mm, ss = [int(x) for x in clock.split(":")]
        except Exception:
            mm, ss = 0, 0
        pd = p.get("periodDescriptor") or {}
        per = int((pd.get("number") if isinstance(pd, dict) else 0) or p.get("period") or 0) or 1
        abs_sec = (per-1)*20*60 + mm*60 + ss
        tri = ((p.get("team") or {}).get("abbrev") or (p.get("team") or {}).get("triCode") or "")
        who = "HOME" if tri == home_abbr else "AWAY"
        out.append({
            "abs_sec": abs_sec,
            "period": per,
            "team": who,
            "tri": tri
        })
    out.sort(key=lambda x: x["abs_sec"])
    # считаем "дома:в гостях" нарастающим итогом
    home = away = 0
    for e in out:
        if e["team"] == "HOME": home += 1
        else: away += 1
        e["home_score"] = home
        e["away_score"] = away
    return out

def detect_shootout(pbp_json) -> bool:
    summary = pbp_json.get("summary") or {}
    so = summary.get("shootout") or {}
    return bool(so) or (summary.get("hasShootout") is True)

# ---------- Sports.ru /lineups/ ----------
def sports_slug_for_pair(away_tri, home_tri):
    a = SPORTS_SLUG.get(away_tri); h = SPORTS_SLUG.get(home_tri)
    if not a or not h: return None, None
    return f"{a}-vs-{h}", f"{h}-vs-{a}"

def fetch_sports_lineups(slug):
    url = f"https://www.sports.ru/hockey/match/{slug}/lineups/"
    dbg("GET", url)
    r = S.get(url, timeout=25)
    if r.status_code != 200: return None
    return r.text

# Ищем строки с временем и кириллицей. Пример: "24:48 Уилсон (Чикран, Рой)"
GOAL_LINE_RE = re.compile(r"(\d{1,2}:\d{2})\s*([А-ЯЁ][^()\n]+?)(?:\s*\(([^)]+)\))?(?:\s|$)")

def parse_sports_lineups_goals(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text("\n", strip=True)
    seen=set(); res=[]
    for m in GOAL_LINE_RE.finditer(text):
        tmm = m.group(1)
        who = (m.group(2) or "").strip()
        ass = (m.group(3) or "").strip()
        # пропускаем мусор без кириллицы
        if not re.search(r"[А-ЯЁа-яё]", who): continue
        who = re.sub(r"\s+", " ", who)
        ass = re.sub(r"\s+", " ", ass)
        mm, ss = [int(x) for x in tmm.split(":")]
        abs_sec = mm*60 + ss
        k=(abs_sec, who)
        if k in seen: continue
        seen.add(k)
        res.append({"abs_sec": abs_sec, "scorer_ru": who, "assists_ru": ass})
    res.sort(key=lambda x: x["abs_sec"])
    dbg("sports.ru goals parsed:", len(res))
    return res

def attach_ru_names_to_pbp(pbp_events, ru_events):
    # матчим по ближайшему времени ±2 сек
    for e in pbp_events:
        best=None; diff_best=999
        for r in ru_events:
            d = abs(r["abs_sec"] - e["abs_sec"])
            if d < diff_best:
                best=r; diff_best=d
        if best and diff_best <= 2:
            e["scorer_ru"]  = best["scorer_ru"]
            e["assists_ru"] = best["assists_ru"]
        else:
            e["scorer_ru"]  = ""
            e["assists_ru"] = ""

# ---------- Формирование текста ----------
def fmt_team_line(tri_home, tri_away, home_score, away_score, rec_home, rec_away):
    eh = TEAM_EMOJI.get(tri_home,"🏒"); ea = TEAM_EMOJI.get(tri_away,"🏒")
    th = TEAM_RU.get(tri_home, tri_home); ta = TEAM_RU.get(tri_away, tri_away)
    hbold = home_score>away_score; abold = away_score>home_score
    sh = f"**{home_score}**" if hbold else f"{home_score}"
    sa = f"**{away_score}**" if abold else f"{away_score}"
    rec_h = f" ({rec_home})" if rec_home else ""
    rec_a = f" ({rec_away})" if rec_away else ""
    return f"{eh} {th}: {sh}{rec_h}\n{ea} {ta}: {sa}{rec_a}\n"

def build_match_block(ev, goals, has_shootout, rec_map):
    tri_home = (ev.get("homeTeam") or {}).get("abbrev")
    tri_away = (ev.get("awayTeam") or {}).get("abbrev")
    home_score = int((ev.get("homeTeam") or {}).get("score") or 0)
    away_score = int((ev.get("awayTeam") or {}).get("score") or 0)

    lines = [ fmt_team_line(tri_home, tri_away, home_score, away_score,
                            rec_map.get(tri_home,""), rec_map.get(tri_away,"")) ]

    if not goals:
        lines.append("— события матча недоступны\n")
        return "\n".join(lines)

    cur_p=None
    for g in goals:
        p = period_from_abs(g["abs_sec"])
        if p!=cur_p:
            cur_p=p
            lines.append(period_caption(p))
        t = sec_to_mmss(g["abs_sec"])
        who = g.get("scorer_ru") or "—"
        ass = g.get("assists_ru") or ""
        a = f" ({ass})" if ass else ""
        # ВАЖНО: всегда распечатываем счёт как "дома:в гостях"
        score_str = f"{g['home_score']}:{g['away_score']}"
        lines.append(f"{score_str} – {t} {who}{a}")

    if has_shootout:
        lines.append("Победный буллит — (Sports.ru)")
    lines.append("")
    return "\n".join(lines)

# ---------- Основной поток ----------
def build_report():
    report_d = resolve_report_date_msk()
    games = collect_games_for_msk_day(report_d)
    title = f"{len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}"
    header = f"🗓 Регулярный чемпионат НХЛ • {ru_date(report_d)} • {title}\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n"

    if not games:
        return header.rstrip()

    rec_map = load_team_records()

    blocks=[header]
    for ev in games:
        gid = ev.get("id") or ev.get("gameId")
        tri_home = (ev.get("homeTeam") or {}).get("abbrev")
        tri_away = (ev.get("awayTeam") or {}).get("abbrev")
        dbg(f"Game {gid}: {tri_home} vs {tri_away}")

        goals=[]; has_so=False
        try:
            pbp = load_pbp(gid)
            goals = extract_goal_events(pbp, tri_home, tri_away)
            has_so = detect_shootout(pbp)
            dbg(f"PBP goals: {len(goals)} shootout:{has_so}")
        except Exception as e:
            dbg("PBP error:", repr(e))

        # Sports.ru имена
        ru_events=[]
        try:
            slug_a = slug_b = None
            if tri_away and tri_home:
                slug_a, slug_b = sports_slug_for_pair(tri_away, tri_home)
            for slug in (slug_a, slug_b):
                if not slug: continue
                html = fetch_sports_lineups(slug)
                if not html: 
                    continue
                ru_events = parse_sports_lineups_goals(html)
                if ru_events:
                    dbg("sports.ru matched:", slug, "goals:", len(ru_events))
                    break
        except Exception as e:
            dbg("Sports.ru parse error:", repr(e))

        if goals and ru_events:
            attach_ru_names_to_pbp(goals, ru_events)

        blocks.append(build_match_block(ev, goals, has_so, rec_map))

    return "\n".join(blocks).rstrip()

def send_telegram(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        print("No TELEGRAM_BOT_TOKEN/CHAT_ID", file=sys.stderr)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text,
               "parse_mode": "Markdown", "disable_web_page_preview": True}
    dbg("POST Telegram sendMessage")
    r = S.post(url, json=payload, timeout=25)
    r.raise_for_status()
    dbg("Telegram OK")

if __name__ == "__main__":
    try:
        msg = build_report()
        print(msg)
        send_telegram(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
