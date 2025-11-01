# nhl_daily_results_bot.py
# -*- coding: utf-8 -*-
import os, sys, re
import datetime as dt
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
FORCE_MSK_DATE = os.getenv("REPORT_DATE_MSK", "").strip()
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
# slugs sports.ru
SPORTS_SLUG = {
    "VGK":"vegas-golden-knights","COL":"colorado-avalanche","WSH":"washington-capitals",
    "NYI":"new-york-islanders","ANA":"anaheim-ducks","DET":"detroit-red-wings",
    "MIN":"minnesota-wild","SJS":"san-jose-sharks","WPG":"winnipeg-jets",
    "UTA":"utah-hc","CHI":"chicago-blackhawks","LAK":"los-angeles-kings",
    "NSH":"nashville-predators","DAL":"dallas-stars","CGY":"calgary-flames",
    "NYR":"new-york-rangers","VAN":"vancouver-canucks","EDM":"edmonton-oilers",
    "BOS":"boston-bruins","CAR":"carolina-hurricanes","PIT":"pittsburgh-penguins"
}
# названия для championat (поиск в тексте календаря)
CHAMP_TEAM_RU = {
    "VGK":["Вегас"],
    "COL":["Колорадо","Эвеланш"],
    "WSH":["Вашингтон","Кэпиталз","Кэпиталс"],
    "NYI":["Айлендерс","Нью-Йорк Айлендерс"],
    "ANA":["Анахайм","Дакс"],
    "DET":["Детройт","Ред Уингз","Ред Уингс"],
    "MIN":["Миннесота","Уайлд"],
    "SJS":["Сан-Хосе","Шаркс"],
    "WPG":["Виннипег","Джетс"],
    "UTA":["Юта"],
    "CHI":["Чикаго","Блэкхокс","Блэкхоукс","Блэкхокс"],
    "LAK":["Лос-Анджелес","Кингз","Кингс"],
    "NSH":["Нэшвилл","Предаторз","Предаторс"],
    "DAL":["Даллас","Старз"],
    "CGY":["Калгари","Флэймз","Флеймз","Флэймс"],
    "NYR":["Рейнджерс","Нью-Йорк Рейнджерс"],
    "VAN":["Ванкувер","Кэнакс","Кэнакс","Кэнакс"],
    "EDM":["Эдмонтон","Ойлерз","Ойлерс"],
    "BOS":["Бостон","Брюинз","Брюинс"],
    "CAR":["Каролина","Харрикейнз","Харрикейнс"],
    "PIT":["Питтсбург","Пингвинз","Пингвинс"]
}

def dbg(*a):
    if DEBUG: print("[DBG]", *a, flush=True)

def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent":"HOH NHL Daily Results/1.5"})
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

# ---------------- отчётная дата ----------------
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

# ---------------- standings / records ----------------
def _norm_tri(v):
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        for k in ("default","en","EN","ru"):
            if k in v and isinstance(v[k], str) and v[k]:
                return v[k].strip()
        for x in v.values():
            if isinstance(x, str) and x:
                return x.strip()
    return ""

def load_team_records() -> dict:
    url = "https://api-web.nhle.com/v1/standings/now"
    try:
        dbg("GET", url)
        r = S.get(url, timeout=20); r.raise_for_status()
        j = r.json() or {}
        m = {}
        for row in (j.get("standings") or []):
            tri = _norm_tri(row.get("teamAbbrev")) or (row.get("teamAbbrevDefault") or "")
            if not isinstance(tri, str): tri = _norm_tri(tri)
            w = int(row.get("wins", 0) or 0)
            l = int(row.get("losses", 0) or 0)
            ot = int(row.get("otLosses", 0) or 0)
            pts = int(row.get("points", 2*w + ot) or 2*w + ot)
            tri = tri.strip()
            if tri:
                m[tri] = (w,l,ot,pts)
        dbg("records loaded:", len(m))
        return m
    except Exception as e:
        dbg("records error:", repr(e))
        return {}

# ---------------- PBP ----------------
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

def extract_goal_events(pbp_json, home_tri, away_tri, home_id, away_id):
    out = []
    for p in _iter_plays(pbp_json):
        if (p.get("typeDescKey") or "").lower() != "goal":
            continue

        clock = p.get("timeInPeriod") or "00:00"
        try:
            mm, ss = [int(x) for x in clock.split(":")]
        except Exception:
            mm, ss = 0, 0

        pd = p.get("periodDescriptor") or {}
        per = int((pd.get("number") if isinstance(pd, dict) else 0) or p.get("period") or 0) or 1
        abs_sec = (per-1)*20*60 + mm*60 + ss

        ev_team_id = None
        if isinstance(p.get("teamId"), int):
            ev_team_id = p["teamId"]
        elif isinstance(p.get("team"), dict) and isinstance(p["team"].get("id"), int):
            ev_team_id = p["team"]["id"]
        elif isinstance(p.get("details"), dict) and isinstance(p["details"].get("eventOwnerTeamId"), int):
            ev_team_id = p["details"]["eventOwnerTeamId"]

        tri = (p.get("team") or {}).get("abbrev") or (p.get("team") or {}).get("triCode") or ""

        if ev_team_id == home_id:
            who = "HOME"
        elif ev_team_id == away_id:
            who = "AWAY"
        else:
            if tri == home_tri: who = "HOME"
            elif tri == away_tri: who = "AWAY"
            else:
                dbg("WARN: cannot map team -> assume AWAY", {"tri":tri,"hid":home_id,"aid":away_id})
                who = "AWAY"

        hs = p.get("homeScore")
        as_ = p.get("awayScore")

        out.append({
            "abs_sec": abs_sec,
            "period": per,
            "team": who,
            "tri": tri,
            "home_score_event": int(hs) if isinstance(hs, int) else None,
            "away_score_event": int(as_) if isinstance(as_, int) else None
        })

    out.sort(key=lambda x: x["abs_sec"])

    home = away = 0
    for e in out:
        if e["home_score_event"] is not None and e["away_score_event"] is not None:
            e["home_score"] = e["home_score_event"]
            e["away_score"] = e["away_score_event"]
            home, away = e["home_score"], e["away_score"]
        else:
            if e["team"] == "HOME": home += 1
            else: away += 1
            e["home_score"] = home
            e["away_score"] = away
    return out

def detect_shootout(pbp_json) -> bool:
    summary = pbp_json.get("summary") or {}
    so = summary.get("shootout") or {}
    return bool(so) or (summary.get("hasShootout") is True)

# ---------------- Sports.ru ----------------
def sports_slug_for_pair(away_tri, home_tri):
    a = SPORTS_SLUG.get(away_tri); h = SPORTS_SLUG.get(home_tri)
    if not a or not h: return None, None
    return f"{a}-vs-{h}", f"{h}-vs-{a}"

def fetch_url(url):
    dbg("GET", url)
    r = S.get(url, timeout=25)
    if r.status_code != 200: return None
    return r.text

GOAL_LINE_RE_LINEUPS = re.compile(r"(\d{1,2}:\d{2})\s*([А-ЯЁ][^()\n]+?)(?:\s*\(([^)]+)\))?(?:\s|$)")
def parse_lineups_goals(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text("\n", strip=True)
    seen=set(); res=[]
    for m in GOAL_LINE_RE_LINEUPS.finditer(text):
        tmm = m.group(1)
        who = (m.group(2) or "").strip()
        ass = (m.group(3) or "").strip()
        if not re.search(r"[А-ЯЁа-яё]", who): continue
        who = re.sub(r"\s+", " ", who)
        ass = re.sub(r"\s+", " ", ass)
        mm, ss = [int(x) for x in tmm.split(":")]
        abs_sec = mm*60 + ss
        k=(abs_sec, who)
        if k in seen: continue
        seen.add(k)
        res.append({"abs_sec": abs_sec, "scorer_ru": who, "assists_ru": ass, "src":"sports"})
    res.sort(key=lambda x: x["abs_sec"])
    return res

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2})\b")
def parse_matchpage_goals(html_text, src_tag="sports"):
    soup = BeautifulSoup(html_text, "html.parser")
    items = soup.get_text("\n", strip=True).split("\n")
    res, seen = [], set()
    last_time = None
    for line in items:
        line = line.strip()
        tm = TIME_RE.fullmatch(line)
        if tm:
            last_time = tm.group(1)
            continue
        if "Гол!" in line:
            author = line.split("Гол!",1)[1].strip()
            if last_time and re.search(r"[А-ЯЁа-яё]", author):
                mm, ss = [int(x) for x in last_time.split(":")]
                abs_sec = mm*60 + ss
                k=(abs_sec, author)
                if k in seen: continue
                seen.add(k)
                res.append({"abs_sec": abs_sec, "scorer_ru": author, "assists_ru": "", "src":src_tag})
            last_time = None
        elif line.startswith("Ассист"):
            if res:
                assists = line.split(":",1)[1].strip()
                res[-1]["assists_ru"] = assists
    res.sort(key=lambda x: x["abs_sec"])
    return res

def parse_matchpage_shootout_winner(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    txt = soup.get_text("\n", strip=True)
    m = re.search(r"Победный буллит\s*—\s*([А-ЯЁ][\w\-\s\.]+)", txt)
    if m: return m.group(1).strip()
    return ""

def find_match_slug_via_club_calendar(team_slug:str, opp_slug:str):
    url = f"https://www.sports.ru/hockey/club/{team_slug}/calendar/"
    html = fetch_url(url)
    if not html: return None
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("/hockey/match/"): continue
        slug = href.strip("/").split("/", 2)[-1]
        if opp_slug in slug and (team_slug in slug):
            return slug.rstrip("/")
    return None

def get_ru_goals_from_sports(away_tri, home_tri):
    slug_a, slug_b = sports_slug_for_pair(away_tri, home_tri)
    tried = []

    for slug in (slug_a, slug_b):
        if not slug: continue
        tried.append(slug)
        html = fetch_url(f"https://www.sports.ru/hockey/match/{slug}/lineups/")
        if html:
            g = parse_lineups_goals(html)
            if g:
                dbg("sports.ru matched lineups:", slug, "goals:", len(g))
                return g, ""
        html2 = fetch_url(f"https://www.sports.ru/hockey/match/{slug}/")
        if html2:
            g2 = parse_matchpage_goals(html2, src_tag="sports")
            if g2:
                dbg("sports.ru matched matchpage:", slug, "goals:", len(g2))
                so = parse_matchpage_shootout_winner(html2)
                return g2, so

    a_slug = SPORTS_SLUG.get(away_tri); h_slug = SPORTS_SLUG.get(home_tri)
    for team_slug, opp_slug in ((a_slug, h_slug), (h_slug, a_slug)):
        if not team_slug or not opp_slug: continue
        match_slug = find_match_slug_via_club_calendar(team_slug, opp_slug)
        if match_slug and match_slug not in tried:
            html = fetch_url(f"https://www.sports.ru/hockey/match/{match_slug}/lineups/")
            if html:
                g = parse_lineups_goals(html)
                if g:
                    dbg("sports.ru matched via club calendar (lineups):", match_slug, "goals:", len(g))
                    return g, ""
            html2 = fetch_url(f"https://www.sports.ru/hockey/match/{match_slug}/")
            if html2:
                g2 = parse_matchpage_goals(html2, src_tag="sports")
                if g2:
                    dbg("sports.ru matched via club calendar (matchpage):", match_slug, "goals:", len(g2))
                    so = parse_matchpage_shootout_winner(html2)
                    return g2, so

    dbg("sports.ru no goals for pair", away_tri, home_tri)
    return [], ""

# ---------------- Championat fallback ----------------
CHAMP_TOURNAMENT_ID = "6606"  # текущая НХЛ у Чемпа

def champ_calendar_urls_for_date(msk_date: dt.date):
    # смотрим текущий и предыдущий месяц
    months = sorted({msk_date.month, (msk_date.month-1) or 12})
    urls = [f"https://www.championat.com/hockey/_nhl/tournament/{CHAMP_TOURNAMENT_ID}/calendar/?m={m}" for m in months]
    return urls

def champ_text_has_any(text, keys):
    t = text.lower()
    for k in keys:
        if k.lower() in t:
            return True
    return False

def champ_find_match_links(msk_date: dt.date, away_tri, home_tri):
    cand = []
    team_a = CHAMP_TEAM_RU.get(away_tri, [away_tri])
    team_h = CHAMP_TEAM_RU.get(home_tri, [home_tri])

    for url in champ_calendar_urls_for_date(msk_date):
        dbg("GET", url)
        r = S.get(url, timeout=25)
        if r.status_code != 200: continue
        soup = BeautifulSoup(r.text, "html.parser")
        for tr in soup.find_all("tr"):
            txt = tr.get_text(" ", strip=True)
            if not (champ_text_has_any(txt, team_a) and champ_text_has_any(txt, team_h)):
                continue
            a_tags = tr.find_all("a", href=True)
            for a in a_tags:
                href = a["href"]
                if "/match/" in href:
                    if href.startswith("http"):
                        cand.append(href)
                    else:
                        cand.append("https://www.championat.com" + href)
                    break
    return cand

def champ_parse_match_goals(match_url):
    dbg("GET", match_url)
    r = S.get(match_url, timeout=25)
    if r.status_code != 200: return [], ""
    # Чемп часто грузит события сразу на странице; парсим так же, как sports (универсальный)
    goals = parse_matchpage_goals(r.text, src_tag="champ")
    # победный буллит, если встречается
    so_name = ""
    m = re.search(r"Победный буллит\s*—\s*([А-ЯЁ][\w\-\s\.]+)", r.text)
    if m: so_name = m.group(1).strip()
    return goals, so_name

def get_ru_goals_from_champ(msk_date: dt.date, away_tri, home_tri):
    links = champ_find_match_links(msk_date, away_tri, home_tri)
    for u in links:
        goals, so = champ_parse_match_goals(u)
        if goals:
            dbg("champ matched:", u, "goals:", len(goals))
            return goals, so
    dbg("champ no goals for pair", away_tri, home_tri)
    return [], ""

# ---------------- склейка имён с PBP ----------------
def attach_ru_names_to_pbp(pbp_events, ru_events, tolerance_sec:int):
    for e in pbp_events:
        best=None; diff_best=10**9
        for r in ru_events:
            d = abs(r["abs_sec"] - e["abs_sec"])
            if d < diff_best:
                best=r; diff_best=d
        if best and diff_best <= tolerance_sec:
            e["scorer_ru"]  = best["scorer_ru"]
            e["assists_ru"] = best["assists_ru"]
        else:
            e["scorer_ru"]  = ""
            e["assists_ru"] = ""

# ---------------- вывод ----------------
def fmt_team_line(tri_home, tri_away, home_score, away_score, rec_map):
    eh = TEAM_EMOJI.get(tri_home,"🏒"); ea = TEAM_EMOJI.get(tri_away,"🏒")
    th = TEAM_RU.get(tri_home, tri_home); ta = TEAM_RU.get(tri_away, tri_away)
    wlh_home = rec_map.get(tri_home)
    wlh_away = rec_map.get(tri_away)
    rec_h = f" ({wlh_home[0]}-{wlh_home[1]}-{wlh_home[2]}, {wlh_home[3]} о.)" if wlh_home else ""
    rec_a = f" ({wlh_away[0]}-{wlh_away[1]}-{wlh_away[2]}, {wlh_away[3]} о.)" if wlh_away else ""
    # жирным выделять счёт победителя не просили здесь, но можно добавить при желании
    return f"{eh} {th}: {home_score}{rec_h}\n{ea} {ta}: {away_score}{rec_a}\n"

def build_match_block(ev, goals, has_shootout, rec_map, so_winner_ru=""):
    tri_home = (ev.get("homeTeam") or {}).get("abbrev")
    tri_away = (ev.get("awayTeam") or {}).get("abbrev")
    home_score = int((ev.get("homeTeam") or {}).get("score") or 0)
    away_score = int((ev.get("awayTeam") or {}).get("score") or 0)

    lines = [ fmt_team_line(tri_home, tri_away, home_score, away_score, rec_map) ]

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
        score_str = f"{g['home_score']}:{g['away_score']}"
        lines.append(f"{score_str} – {t} {who}{a}")

    if has_shootout:
        if so_winner_ru:
            lines.append(f"Победный буллит — {so_winner_ru}")
        else:
            lines.append("Победный буллит —")
    lines.append("")
    return "\n".join(lines)

# ---------------- основной поток ----------------
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
        id_home = int((ev.get("homeTeam") or {}).get("id") or 0)
        id_away = int((ev.get("awayTeam") or {}).get("id") or 0)

        dbg(f"Game {gid}: {tri_home} (id:{id_home}) vs {tri_away} (id:{id_away})")

        goals=[]; has_so=False; so_winner_ru=""
        try:
            pbp = load_pbp(gid)
            goals = extract_goal_events(pbp, tri_home, tri_away, id_home, id_away)
            has_so = detect_shootout(pbp)
            dbg(f"PBP goals: {len(goals)} shootout:{has_so} "
                f"sample_has_score_fields={any(g.get('home_score_event') is not None for g in goals)}")
        except Exception as e:
            dbg("PBP error:", repr(e))

        # 1) пробуем Sports.ru
        ru_events, so1 = get_ru_goals_from_sports(tri_away, tri_home)

        # 2) если пусто — Чемпионат
        if not ru_events:
            ru_events, so2 = get_ru_goals_from_champ(report_d, tri_away, tri_home)
            if so2: so_winner_ru = so2
        else:
            so_winner_ru = so1

        # Склейка имён
        if goals and ru_events:
            src = ru_events[0].get("src","sports")
            tol = 2 if src == "sports" else 90
            attach_ru_names_to_pbp(goals, ru_events, tolerance_sec=tol)

        blocks.append(build_match_block(ev, goals, has_so, rec_map, so_winner_ru))

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
