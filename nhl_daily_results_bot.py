# nhl_daily_results_bot.py
# -*- coding: utf-8 -*-

import os, sys, re, json, math
from html import escape
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

DEBUG = True

def dbg(*a):
    if DEBUG:
        print("[DBG]", *a)

# ---------- HTTP session with retries ----------
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=0.7,
        allowed_methods=["GET", "POST"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "HOH-NHL-Daily/1.3 (merge sports+champ; python-requests)"
    })
    return s

SESSION = make_session()

def http_get_json(url: str, timeout: int = 25) -> dict:
    dbg("GET", url)
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def http_get_html(url: str, timeout: int = 25) -> BeautifulSoup:
    dbg("GET", url)
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

# ---------- NHL team helpers ----------
# tri -> RU name & emoji (минимальный необходимый набор + дефолты)
TEAM_RU_NAME = {
    "VGK": "Вегас",
    "COL": "Колорадо",
    "WSH": "Вашингтон",
    "NYI": "Айлендерс",
    "ANA": "Анахайм",
    "DET": "Детройт",
    "MIN": "Миннесота",
    "SJS": "Сан-Хосе",
    "WPG": "Виннипег",
    "UTA": "Юта",     # новая команда (бывш. ARI)
    "CHI": "Чикаго",
    "LAK": "Лос-Анджелес",
    "NSH": "Нэшвилл",
    "DAL": "Даллас",
    "CGY": "Калгари",
    "NYR": "Рейнджерс",
    "VAN": "Ванкувер",
    "EDM": "Эдмонтон",
    "BOS": "Бостон",
    "CAR": "Каролина",
    # дефолт
}

TEAM_EMOJI = {
    "VGK": "🎰", "COL": "⛰️", "WSH": "🦅", "NYI": "🟠",
    "ANA": "🦆", "DET": "🔴", "MIN": "🌲", "SJS": "🦈",
    "WPG": "✈️", "UTA": "🦣", "CHI": "🦅", "LAK": "👑",
    "NSH": "🐯", "DAL": "⭐️", "CGY": "🔥", "NYR": "🗽",
    "VAN": "🐳", "EDM": "🛢️", "BOS": "🐻", "CAR": "🌪️",
}

def ru_team(tri: str) -> str:
    return TEAM_RU_NAME.get(tri, tri)

def team_emoji(tri: str) -> str:
    return TEAM_EMOJI.get(tri, "🏒")

# sports.ru слуги для клубов (чтобы формировать матч-URL)
SPORTS_RU_SLUG = {
    "ANA": "anaheim-ducks",
    "ARI": "arizona-coyotes",   # исторический
    "UTA": "utah-hc",           # возможный slug для Юты (если не сработает, подхватит championat)
    "BOS": "boston-bruins",
    "BUF": "buffalo-sabres",
    "CGY": "calgary-flames",
    "CAR": "carolina-hurricanes",
    "CHI": "chicago-blackhawks",
    "COL": "colorado-avalanche",
    "CBJ": "columbus-blue-jackets",
    "DAL": "dallas-stars",
    "DET": "detroit-red-wings",
    "EDM": "edmonton-oilers",
    "FLA": "florida-panthers",
    "LAK": "los-angeles-kings",
    "MIN": "minnesota-wild",
    "MTL": "montreal-canadiens",
    "NSH": "nashville-predators",
    "NJD": "new-jersey-devils",
    "NYI": "new-york-islanders",
    "NYR": "new-york-rangers",
    "OTT": "ottawa-senators",
    "PHI": "philadelphia-flyers",
    "PIT": "pittsburgh-penguins",
    "SEA": "seattle-kraken",
    "SJS": "san-jose-sharks",
    "STL": "st-louis-blues",
    "TBL": "tampa-bay-lightning",
    "TOR": "toronto-maple-leafs",
    "VAN": "vancouver-canucks",
    "VGK": "vegas-golden-knights",
    "WSH": "washington-capitals",
    "WPG": "winnipeg-jets",
}

# ---------- Data models ----------
@dataclass
class GameId:
    id: int
    home_tricode: str
    away_tricode: str
    home_id: int
    away_id: int
    home_score: int
    away_score: int
    status: str  # "OFF", "FUT", "LIVE", "FINAL"
    start_utc: datetime

@dataclass
class GoalPBP:
    abs_sec: int
    period: int
    mmss_in_period: str
    team_id: int
    team_tricode: str
    scorer_ru: str = ""
    assists_ru: str = ""
    score_after: Tuple[int, int] = (0, 0) # (away, home) AFTER goal

# ---------- NHL schedule & PBP ----------
def load_nhl_schedule(d: date) -> List[GameId]:
    url = f"https://api-web.nhle.com/v1/schedule/{d.isoformat()}"
    js = http_get_json(url)
    out = []
    for day in (js.get("gameWeek") or []):
        for g in (day.get("games") or []):
            gid = int(g.get("id"))
            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})
            status = (g.get("gameState") or "").upper()
            dt_iso = (g.get("startTimeUTC") or "").replace("Z", "+00:00")
            try:
                start_utc = datetime.fromisoformat(dt_iso)
            except Exception:
                start_utc = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            out.append(GameId(
                id=gid,
                home_tricode=home.get("abbrev") or "",
                away_tricode=away.get("abbrev") or "",
                home_id=int(home.get("id") or 0),
                away_id=int(away.get("id") or 0),
                home_score=int(g.get("homeTeam",{}).get("score",0)),
                away_score=int(g.get("awayTeam",{}).get("score",0)),
                status=status,
                start_utc=start_utc
            ))
    return out

def load_final_games_around(today_utc: date, days_back:int=1, days_fwd:int=1) -> List[GameId]:
    games = []
    for off in range(-days_back, days_fwd+1):
        d = today_utc + timedelta(days=off)
        js = load_nhl_schedule(d)
        for g in js:
            if g.status == "FINAL":
                games.append(g)
            else:
                dbg("skip not final:", g.id, g.status)
    # unique by game id
    uniq = {}
    for g in games:
        uniq[g.id] = g
    dbg("Collected unique FINAL games:", len(uniq))
    return list(uniq.values())

def load_pbp(game_id: int) -> dict:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    js = http_get_json(url)
    return js

def mmss_to_sec(mmss: str) -> int:
    m, s = mmss.split(":")
    return int(m)*60 + int(s)

def period_to_base(period:int) -> int:
    # 1->0, 2->1200, 3->2400, OT(4)->3600 (20*60=1200)
    return (period - 1) * 1200

def abs_time_str(abs_sec:int) -> str:
    m = abs_sec // 60
    s = abs_sec % 60
    return f"{m}.{s:02d}"

def extract_goals_from_pbp(pbp_js: dict, home_id:int, away_id:int, home_tri:str, away_tri:str) -> Tuple[List[GoalPBP], bool, Optional[str]]:
    """
    Возвращает список голевых событий в хронологии,
    флаг shootout и (если есть) победный буллит автор на RU (заполняется позже).
    """
    plays = (pbp_js.get("plays") or [])
    is_shootout = False
    goals: List[GoalPBP] = []

    # PBP формат отличается от сезона к сезону; поддержим 2 популярных поля
    for p in plays:
        # тип "goal"?
        etype = (p.get("typeDescKey") or p.get("typeDesc") or "").lower()
        if "goal" in etype:
            period = int(p.get("period", 0))
            if period == 5:  # иногда SO как 5
                is_shootout = True
                continue
            t = p.get("timeInPeriod") or p.get("timeRemaining") or "00:00"
            try:
                sec = period_to_base(period) + mmss_to_sec(t)
            except Exception:
                continue

            # чей гол: попробуем несколько полей
            team_id = int( (p.get("details") or {}).get("eventOwnerTeamId") or p.get("teamId") or 0 )
            if team_id == 0:
                # запасной вариант
                if (p.get("details") or {}).get("home") == True:
                    team_id = home_id
                elif (p.get("details") or {}).get("away") == True:
                    team_id = away_id

            team_tri = home_tri if team_id == home_id else (away_tri if team_id == away_id else "")
            goals.append(GoalPBP(
                abs_sec=sec,
                period=period,
                mmss_in_period=t,
                team_id=team_id,
                team_tricode=team_tri
            ))

        # shootout?
        if (p.get("typeDescKey") or "").lower() in ("shootoutgoal","shootout") or (p.get("periodType") or "").upper()=="SO":
            is_shootout = True

    goals.sort(key=lambda g: g.abs_sec)
    # победный буллит — позже определяем по текстовым источникам (sports/champ)
    return goals, is_shootout, None

# ---------- Standings / records ----------
def load_records_map() -> Dict[int, Tuple[int,int,int,int]]:
    """
    Возвращает map teamId -> (W,L,OT,PTS)
    """
    url = "https://api-web.nhle.com/v1/standings/now"
    js = http_get_json(url)
    # структура: {"standings":[{...}]}
    stand = (js.get("standings") or [])
    out = {}
    for row in stand:
        tid = int(row.get("teamId") or row.get("team",{}).get("id") or 0)
        if not tid: 
            continue
        w = int(row.get("wins") or 0)
        l = int(row.get("losses") or 0)
        ot = int(row.get("otLosses") or row.get("ot") or 0)
        pts = int(row.get("points") or 0)
        out[tid] = (w,l,ot,pts)
    dbg("records loaded:", len(out))
    return out

# ---------- Sports.ru parsing ----------
def sports_slug_for_pair(away_tri:str, home_tri:str) -> List[str]:
    a = SPORTS_RU_SLUG.get(away_tri, away_tri.lower())
    h = SPORTS_RU_SLUG.get(home_tri, home_tri.lower())
    # обе возможные ориентации
    return [
        f"{a}-vs-{h}",
        f"{h}-vs-{a}",
    ]

# парсим блок «Трансляция» (Гол! Фамилия ... Ассистент(ы): ...)
SPORTS_GOAL_RE = re.compile(r"Гол!\s+([А-ЯЁA-Z][^,\n\r]+)", re.U)
SPORTS_ASSISTS_RE = re.compile(r"Ассистент(?:ы)?:\s+([^\n\r]+)", re.U)
SPORTS_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})", re.U)

def parse_sports_goals_from_matchpage(soup:BeautifulSoup) -> List[Dict]:
    out=[]
    # В «трансляции» часто события лежат в блоках с временем и текстом
    # Идём по всем узлам, где встречается «Гол!»
    candidates = soup.find_all(string=re.compile("Гол!", re.U))
    for node in candidates:
        text_block = node.parent.get_text(" ", strip=True)
        # найдём время ближайшее выше по структуре
        # у sports.ru время часто рядом в соседних <span> или текстом перед
        # возьмём первое \d+:\d\d в text_block
        t_match = SPORTS_TIME_RE.search(text_block)
        if not t_match:
            # попробуем у родителя-родителя
            p2 = node.parent.parent
            t_match = SPORTS_TIME_RE.search(p2.get_text(" ", strip=True)) if p2 else None
        if not t_match:
            continue
        mm, ss = int(t_match.group(1)), int(t_match.group(2))
        # автор
        g_match = SPORTS_GOAL_RE.search(text_block)
        if not g_match:
            continue
        scorer = g_match.group(1).strip()
        # ассисты
        a_match = SPORTS_ASSISTS_RE.search(text_block)
        assists = a_match.group(1).strip() if a_match else ""
        out.append({
            "mm": mm, "ss": ss,
            "scorer_ru": scorer,
            "assists_ru": assists
        })
    return out

def get_ru_goals_from_sports(away_tri:str, home_tri:str) -> Tuple[List[Dict], str]:
    """
    Возвращает события с именами на кириллице из sports.ru и победный буллит RU (если нашёлся).
    """
    tried = []
    winner_so = ""
    for slug in sports_slug_for_pair(away_tri, home_tri):
        tried.append(slug)
        # сначала пробуем страницу трансляции (без /lineups/)
        url1 = f"https://www.sports.ru/hockey/match/{slug}/"
        soup1 = http_get_html(url1)
        ev1 = parse_sports_goals_from_matchpage(soup1)
        if ev1:
            dbg("sports.ru matched matchpage:", slug, "goals:", len(ev1))
            # найти победный буллит (по ключевому слову — редко есть)
            # sports.ru иногда пишет "Победный буллит — Фамилия", но часто нет. Оставим пустым.
            return normalize_events_times(ev1), winner_so

        # иногда основная страница пустая, но есть /lineups/ (с текстовой лентой ниже)
        url2 = f"https://www.sports.ru/hockey/match/{slug}/lineups/"
        soup2 = http_get_html(url2)
        ev2 = parse_sports_goals_from_matchpage(soup2)
        if ev2:
            dbg("sports.ru matched lineups:", slug, "goals:", len(ev2))
            return normalize_events_times(ev2), winner_so

    dbg("sports.ru no goals for pair", away_tri, home_tri, "tried:", tried)
    return [], winner_so

# ---------- Championat parsing ----------
CHAMP_CAL_URL = "https://www.championat.com/hockey/_nhl/tournament/6606/calendar/?m={MM}"

def champ_month_pages_for(d:date) -> List[str]:
    months = sorted({d.month, (d + timedelta(days=1)).month, (d - timedelta(days=1)).month})
    return [CHAMP_CAL_URL.format(MM=f"{m:02d}") for m in months]

def find_champ_match_url(d:date, away_ru:str, home_ru:str) -> Optional[str]:
    """
    Пытаемся найти матч на чемпе по месяцу и русским названиям команд.
    """
    # Чемп хранит названия как «Колорадо», «Вегас» и т.д.
    # Попробуем обе ориентации и допускаем, что матч мог быть «на следующий день» по МСК.
    names = [ (away_ru, home_ru), (home_ru, away_ru) ]
    for url in champ_month_pages_for(d):
        soup = http_get_html(url)
        rows = soup.select("table.calendar-table tr")
        for tr in rows:
            a_tags = tr.select("a.player")
            if len(a_tags) < 2:
                continue
            left = a_tags[0].get_text(strip=True)
            right = a_tags[1].get_text(strip=True)
            when_tag = tr.find("a")
            # ссылка с датой
            if not when_tag or not when_tag.get("href"):
                continue
            # матч-линк обычно на .score-td > a.score
            score_a = tr.select_one("td.score-td a.score")
            if not score_a or not score_a.get("href"):
                continue
            match_url = score_a["href"]
            # проверим пары
            for L, R in names:
                if left == L and right == R:
                    if not match_url.startswith("http"):
                        match_url = "https://www.championat.com" + match_url
                    return match_url
    return None

def parse_champ_goals(match_url:str) -> Tuple[List[Dict], str]:
    """
    Разбираем страницу матча Чемпа (обычно /match/{id}/ или /online).
    Берём «Гол!» + фамилия, «Ассистент(ы)» и время (ММ:СС).
    Возвращаем (events, shootout_winner_ru?)
    """
    soup = http_get_html(match_url)
    events=[]
    so_winner=""

    # Чемп часто кладёт события в блоки .event__item или текст ленты
    texts = soup.get_text("\n", strip=True).split("\n")
    # Ищем строки вида "Гол!", "Ассистенты:" и рядом время "MM:SS"
    for i, line in enumerate(texts):
        if "Гол!" in line:
            # Поиск времени в соседних строках
            t_mm = t_ss = None
            # линейно просмотрим пару соседних строк
            window = " ".join(texts[max(0,i-1): i+2])
            mtime = re.search(r"(\d{1,2}):(\d{2})", window)
            if not mtime:
                # иногда время стоит дальше
                for j in range(i, min(i+5,len(texts))):
                    mtime = re.search(r"(\d{1,2}):(\d{2})", texts[j])
                    if mtime:
                        break
            if mtime:
                mm, ss = int(mtime.group(1)), int(mtime.group(2))
            else:
                continue

            # Автор и ассисты
            # Простейший способ: взять след. 2-3 строки в поиске «Ассистент»
            block = " ".join(texts[i:i+6])
            # Автор после "Гол!" до перевода строки/двух пробелов
            ma = re.search(r"Гол!\s+([А-ЯЁA-Z][^,\n\r]+)", block, flags=re.U)
            scorer = ma.group(1).strip() if ma else ""
            mas = re.search(r"Ассистент(?:ы)?:\s+([^\n\r]+)", block, flags=re.U)
            assists = mas.group(1).strip() if mas else ""
            if scorer:
                events.append({"mm": mm, "ss": ss, "scorer_ru": scorer, "assists_ru": assists})

        if "Победный буллит" in line:
            # «Победный буллит — Фамилия»
            mw = re.search(r"Победный буллит\s*[—-]\s*([А-ЯЁA-Z][^,\n\r]+)", line, flags=re.U)
            if mw:
                so_winner = mw.group(1).strip()

    return normalize_events_times(events), so_winner

def get_ru_goals_from_champ(d_for_cal:date, away_tri:str, home_tri:str) -> Tuple[List[Dict], str]:
    # для поиска по чемпу нужны русские названия
    away_ru = ru_team(away_tri)
    home_ru = ru_team(home_tri)
    url = find_champ_match_url(d_for_cal, away_ru, home_ru)
    if not url:
        # Возможно ориентация команд обратная по дню — попробуем соседние дни
        for d_shift in (-1, +1):
            alt = find_champ_match_url(d_for_cal + timedelta(days=d_shift), away_ru, home_ru)
            if alt:
                url = alt
                break
    if not url:
        dbg("champ no match url for pair", away_tri, home_tri)
        return [], ""
    ev, so = parse_champ_goals(url)
    if ev:
        dbg("champ matched:", url, "goals:", len(ev))
    return ev, so

# ---------- Events utilities ----------
def normalize_events_times(ev: List[Dict]) -> List[Dict]:
    """
    На входе события с полями mm, ss (время на табло), конвертируем в abs_sec (время матча).
    WARNING: страницы Sports/Champ дают только табло-время, без периода.
    Мы сопоставляем их позже к PBP по «ближайшему времени» (через tolerance_sec).
    Здесь просто вшиваем абстрактный abs_sec = mm*60+ss — будет приведено позже.
    """
    out=[]
    for e in ev:
        mm = int(e.get("mm", 0))
        ss = int(e.get("ss", 0))
        out.append({
            "abs_sec": mm*60 + ss,
            "scorer_ru": e.get("scorer_ru","").strip(),
            "assists_ru": e.get("assists_ru","").strip()
        })
    # сортируем по времени табло (от меньшего к большему)
    out.sort(key=lambda x: x["abs_sec"])
    return out

def merge_ru_events(ev_a:List[Dict], ev_b:List[Dict], join_tol_sec:int=3) -> List[Dict]:
    """
    Склеивает два списка событий (sports/champ) по «времени табло».
    При коллизии в пределах join_tol_sec берём непустые поля scorer_ru/assists_ru.
    """
    all_ev = (ev_a or []) + (ev_b or [])
    if not all_ev:
        return []
    all_ev.sort(key=lambda x: x["abs_sec"])
    used = [False]*len(all_ev)
    merged = []
    for i, a in enumerate(all_ev):
        if used[i]:
            continue
        cur = dict(a)
        for j in range(i+1, len(all_ev)):
            if used[j]:
                continue
            b = all_ev[j]
            if abs(b["abs_sec"] - a["abs_sec"]) <= join_tol_sec:
                if not cur.get("scorer_ru") and b.get("scorer_ru"):
                    cur["scorer_ru"] = b["scorer_ru"]
                if not cur.get("assists_ru") and b.get("assists_ru"):
                    cur["assists_ru"] = b["assists_ru"]
                used[j] = True
            else:
                break
        used[i] = True
        merged.append(cur)
    merged.sort(key=lambda x: x["abs_sec"])
    return merged

def attach_ru_names_to_pbp(goals:List[GoalPBP], ru_events:List[Dict], tolerance_sec:int=2):
    """
    Привязывает русские имена к PBP-голам по времени.
    В PBP у нас abs_sec = период * 1200 + mm:ss, а в ru_events — просто «минуты-матча».
    Чтобы свести, нормализуем ru_time в «минуты-матча» относительно каждого периода:
    Для каждого PBP-голa ищем ближайшее ru-событие (по минутам матча % 1200) c допуском tolerance_sec.
    """
    if not goals or not ru_events:
        return
    # создадим копию с модом времени в пределах 20 минут
    # (т.е. 24:32 -> 4:32, 44:10 -> 4:10, etc.)
    ru_buckets = {}
    for e in ru_events:
        t = int(e["abs_sec"])
        mod = t % 1200
        ru_buckets.setdefault(mod, []).append(e)

    # для ускорения — сорт ключей
    mods_sorted = sorted(ru_buckets.keys())

    def find_near(mod_val:int) -> Optional[Dict]:
        # бинпоиск ближайшего в mods_sorted
        lo, hi = 0, len(mods_sorted)-1
        best=None
        best_diff=10**9
        while lo<=hi:
            mid=(lo+hi)//2
            mv = mods_sorted[mid]
            d = abs(mv - mod_val)
            if d < best_diff:
                best_diff=d; best=mv
            if mv < mod_val: lo=mid+1
            elif mv > mod_val: hi=mid-1
            else: break
        if best is None or best_diff>tolerance_sec:
            return None
        # возьмём первый из списка по этому ключу, и сразу «исчерпаем» его
        lst = ru_buckets[best]
        ev = lst.pop(0)
        if not lst:
            del ru_buckets[best]
        return ev

    for g in goals:
        mod = g.abs_sec % 1200
        ev = find_near(mod)
        if ev:
            g.scorer_ru = ev.get("scorer_ru","")
            g.assists_ru = ev.get("assists_ru","")

# ---------- Formatting ----------
def compute_score_evolution(goals:List[GoalPBP], away_id:int, home_id:int):
    a = h = 0
    for g in goals:
        if g.team_id == away_id:
            a += 1
        elif g.team_id == home_id:
            h += 1
        g.score_after = (a, h)

def period_title(p:int) -> str:
    if p == 1: return "1-й период"
    if p == 2: return "2-й период"
    if p == 3: return "3-й период"
    if p >= 4: return f"Овертайм №{p-3}"
    return f"Период {p}"

def format_goal_line(g:GoalPBP, away_tri:str, home_tri:str) -> str:
    # Преобразуем счёт в формате A:H, но в тексте показываем «l:r – time Имя (ассисты)»
    a,h = g.score_after
    # кто забил — автора на RU, если нет — «—»
    who = g.scorer_ru.strip() if g.scorer_ru.strip() else "—"
    assists = g.assists_ru.strip()
    t = abs_time_str(g.abs_sec)
    return f"{a}:{h} – {t} {who}" + (f" ({assists})" if assists else "")

def format_match_block(g:GameId, goals:List[GoalPBP], records:Dict[int,Tuple[int,int,int,int]], shootout:bool, so_winner_ru:Optional[str]) -> str:
    em_home = team_emoji(g.home_tricode)
    em_away = team_emoji(g.away_tricode)
    name_home = ru_team(g.home_tricode)
    name_away = ru_team(g.away_tricode)
    rec_home = records.get(g.home_id, (0,0,0,0))
    rec_away = records.get(g.away_id, (0,0,0,0))
    rhome = f"({rec_home[0]}-{rec_home[1]}-{rec_home[2]}, {rec_home[3]} о.)"
    raway = f"({rec_away[0]}-{rec_away[1]}-{rec_away[2]}, {rec_away[3]} о.)"

    # Заголовок матча
    parts = [
        f"{em_home} «{name_home}»: {g.home_score} {rhome}",
        f"{em_away} «{name_away}»: {g.away_score} {raway}",
        ""
    ]

    if not goals:
        parts.append("— события матча недоступны")
        return "\n".join(parts)

    # группируем по периоду
    byp: Dict[int, List[GoalPBP]] = {}
    for x in goals:
        byp.setdefault(x.period, []).append(x)

    for p in sorted(byp.keys()):
        parts.append(f"_{period_title(p)}_")
        for goal in byp[p]:
            parts.append(format_goal_line(goal, g.away_tricode, g.home_tricode))

    if shootout and so_winner_ru:
        parts.append("")
        parts.append(f"Победный буллит — {so_winner_ru}")

    return "\n".join(parts)

# ---------- Telegram ----------
def send_telegram(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat  = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat:
        print("No TELEGRAM_BOT_TOKEN/CHAT_ID in env", file=sys.stderr)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    dbg("POST Telegram sendMessage")
    r = SESSION.post(url, json={
        "chat_id": chat,
        "text": text,
        "parse_mode": "Markdown",  # используем подчёркивания для курсивов
        "disable_web_page_preview": True
    }, timeout=25)
    r.raise_for_status()
    dbg("Telegram OK")

# ---------- Main report ----------
def build_report() -> str:
    # Берём игры «вчера/сегодня/завтра» UTC — как и было (в ранних логах: 30,31,01)
    today_utc = datetime.utcnow().date()
    finals = load_final_games_around(today_utc, days_back=1, days_fwd=1)
    if not finals:
        # fallback — если вдруг пусто
        finals = load_final_games_around(today_utc, days_back=2, days_fwd=0)

    # Заголовок по локальному (Европа/Берлин)
    tz_berlin = ZoneInfo("Europe/Berlin")
    title_date = datetime.now(tz=tz_berlin).strftime("%-d %B")
    header = [f"🗓 Регулярный чемпионат НХЛ • {title_date} • {len(finals)} матчей", "", "Результаты надёжно спрятаны 👇", "", "——————————————————", ""]
    records = load_records_map()

    blocks = []

    for g in finals:
        # PBP
        pbp = load_pbp(g.id)
        goals, shootout, so_winner_ru = extract_goals_from_pbp(pbp, g.home_id, g.away_id, g.home_tricode, g.away_tricode)
        dbg("PBP goals:", len(goals), "shootout:", shootout, "sample_has_score_fields=False")

        # DATES for championat calendars
        report_d = g.start_utc.astimezone(ZoneInfo("Europe/Moscow")).date()

        # 1) Всегда тянем Sports и Championat
        ru_sports, so1 = get_ru_goals_from_sports(g.away_tricode, g.home_tricode)
        ru_champ , so2 = get_ru_goals_from_champ(report_d, g.away_tricode, g.home_tricode)

        # 2) Склейка
        if ru_sports and ru_champ:
            ru_events = merge_ru_events(ru_sports, ru_champ, join_tol_sec=3)
            so_winner_ru = so1 or so2 or so_winner_ru or ""
            dbg(f"merge RU events: sports={len(ru_sports)} champ={len(ru_champ)} -> merged={len(ru_events)}")
        elif ru_sports:
            ru_events = ru_sports
            so_winner_ru = so1 or so_winner_ru or ""
        elif ru_champ:
            ru_events = ru_champ
            so_winner_ru = so2 or so_winner_ru or ""
        else:
            ru_events = []
            # so_winner_ru оставляем как было (в PBP обычно нет человека-названия)

        # 3) Толеранс: если покрытие имен меньше числа голов — расширим
        tol = 2
        if ru_events and len(ru_events) < len(goals):
            tol = 120
            dbg(f"low RU coverage vs PBP: {len(ru_events)} < {len(goals)} -> tolerance {tol}s")

        # 4) Привязать имена
        if goals and ru_events:
            attach_ru_names_to_pbp(goals, ru_events, tolerance_sec=tol)

        # 5) Посчитать счёт по ходу
        compute_score_evolution(goals, g.away_id, g.home_id)

        # 6) Оформить блок
        block = format_match_block(g, goals, records, shootout, so_winner_ru)
        blocks.append(block)
        blocks.append("")  # пробел между матчами

    body = "\n".join(blocks).strip()
    return "\n".join(header) + "\n" + body

if __name__ == "__main__":
    try:
        text = build_report()
        send_telegram(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
