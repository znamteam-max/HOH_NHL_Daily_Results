#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL → Telegram (RU): время и события из NHL, имена с sports.ru (поиск через Календарь)

Логика:
1) Берём окно игрового дня по МСК:
   - все матчи REPORT_DATE,
   - и матчи REPORT_DATE-1, начавшиеся >= 15:00 МСК.
2) Для каждого матча:
   - тянем play-by-play из api-web.nhle.com (период, timeInPeriod, счёт после гола, итоговый счёт, ОТ/Б).
   - находим этот же матч на sports.ru:
       СНАЧАЛА — через календарь турнира https://www.sports.ru/hockey/tournament/nhl/calendar/
       (ищем строку, где home/away совпадают и дата/время совпадают);
       если не нашлось — пробуем общий поиск sports.ru.
   - со страницы матча sports.ru берём список голов, строим карту:
       key = (period, "MM:SS") → value = (Автор_«И. Фамилия», [ассистенты_«И. Фамилия»])
   - склеиваем с NHL по ключу (period,timeInPeriod). Если не нашли — скрипт падает (чтобы не просочился английский).
   - буллиты: берём только «Победный буллит».
3) Печать: жирным победитель, по периодам, время в абсолюте (mm.ss), только победный буллит.

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  REPORT_DATE=YYYY-MM-DD (опционально, МСК)
"""

import os, sys, re, json, time, random, datetime as dt
from zoneinfo import ZoneInfo
from html import escape
from typing import Dict, List, Tuple, Any, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───── Настройки
TZ_MSK = ZoneInfo("Europe/Moscow")
API = "https://api-web.nhle.com"
SPORTS_CAL = "https://www.sports.ru/hockey/tournament/nhl/calendar/"
SPORTS_SEARCH = "https://www.sports.ru/search/"

TEAM_META = {
    "NJD": ("😈", "Нью-Джерси"),
    "NYI": ("🟠", "Айлендерс"),
    "NYR": ("🗽", "Рейнджерс"),
    "PHI": ("🛩", "Филадельфия"),
    "PIT": ("🐧", "Питтсбург"),
    "BOS": ("🐻", "Бостон"),
    "BUF": ("🦬", "Баффало"),
    "MTL": ("🇨🇦", "Монреаль"),
    "OTT": ("🛡", "Оттава"),
    "TOR": ("🍁", "Торонто"),
    "CAR": ("🌪️", "Каролина"),
    "FLA": ("🐆", "Флорида"),
    "TBL": ("⚡", "Тампа-Бэй"),
    "WSH": ("🦅", "Вашингтон"),
    "CHI": ("🦅", "Чикаго"),
    "DET": ("🔴", "Детройт"),
    "NSH": ("🐯", "Нэшвилл"),
    "STL": ("🎵", "Сент-Луис"),
    "CGY": ("🔥", "Калгари"),
    "EDM": ("🛢️", "Эдмонтон"),
    "VAN": ("🐳", "Ванкувер"),
    "ANA": ("🦆", "Анахайм"),
    "DAL": ("⭐", "Даллас"),
    "LAK": ("👑", "Лос-Анджелес"),
    "SJS": ("🦈", "Сан-Хосе"),
    "CBJ": ("💣", "Коламбус"),
    "COL": ("⛰️", "Колорадо"),
    "MIN": ("🌲", "Миннесота"),
    "WPG": ("✈️", "Виннипег"),
    "ARI": ("🦣", "Юта"),   # Аризона → Юта
    "SEA": ("🦑", "Сиэтл"),
    "VGK": ("🎰", "Вегас"),
}

RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}
def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

# ───── HTTP с ретраями
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.6,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-RU-Merger/1.1",
        "Accept": "text/html,application/json,*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    })
    return s

S = make_session()

def get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code == 200:
        return r.json()
    raise RuntimeError(f"GET {url} -> {r.status_code}")

def get_html(url: str) -> str:
    r = S.get(url, timeout=25)
    if r.status_code == 200:
        return r.text
    raise RuntimeError(f"GET {url} -> {r.status_code}")

# ───── Окно игрового дня
def report_date() -> dt.date:
    env = os.getenv("REPORT_DATE","").strip()
    if env:
        return dt.date.fromisoformat(env)
    return dt.datetime.now(TZ_MSK).date()

def window_msk(d: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.combine(d - dt.timedelta(days=1), dt.time(15,0), tzinfo=TZ_MSK)
    end   = dt.datetime.combine(d, dt.time(23,59,59,999000), tzinfo=TZ_MSK)
    return start, end

def to_msk(utc_iso: str) -> dt.datetime:
    return dt.datetime.fromisoformat(utc_iso.replace("Z","+00:00")).astimezone(TZ_MSK)

# ───── NHL schedule & PBP
def pick_games(d: dt.date) -> List[dict]:
    start, end = window_msk(d)
    games = []
    for day in (d - dt.timedelta(days=1), d):
        js = get_json(f"{API}/v1/schedule/{day.isoformat()}")
        lst = js.get("games") or js.get("gameWeek",[{}])[0].get("games",[])
        games.extend(lst)
    picked = []
    seen = set()
    for g in games:
        gid = int(g.get("id") or 0)
        if not gid or gid in seen: continue
        seen.add(gid)
        utc = g.get("startTimeUTC") or g.get("startTime")
        if not utc: continue
        msk = to_msk(utc)
        if start <= msk <= end:
            picked.append({
                "id": gid,
                "msk": msk,
                "home": g["homeTeam"]["abbrev"],
                "away": g["awayTeam"]["abbrev"],
            })
    picked.sort(key=lambda x: x["msk"])
    return picked

def nhl_play_by_play(gid: int) -> dict:
    return get_json(f"{API}/v1/gamecenter/{gid}/play-by-play")

# ───── Абсолютное время
def abs_time(period: int, mmss: str) -> str:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", mmss)
    if not m:
        return mmss.replace(":", ".")
    mm, ss = int(m.group(1)), int(m.group(2))
    base = (period-1)*20 if period<=3 else 60 + 5*(period-4)
    return f"{base + mm}.{ss:02d}"

# ───── Поиск матча на sports.ru через Календарь
def find_sportsru_match_url_via_calendar(home_ru: str, away_ru: str, start_msk: dt.datetime) -> Optional[str]:
    """
    Парсим таблицу календаря и ищем строку, где:
      - owner-td a.player содержит home_ru,
      - guests-td a.player содержит away_ru,
      - столбец дата содержит дату и время старта (совпадает до минут).
    Возвращаем ссылку со столбца score-td (сама страница матча).
    """
    html = get_html(SPORTS_CAL)
    soup = BeautifulSoup(html, "html.parser")

    # Нормализаторы
    def norm_team(s: str) -> str:
        return re.sub(r"\s+", " ", s).strip().lower()

    home_key = norm_team(home_ru)
    away_key = norm_team(away_ru)
    date_key = start_msk.strftime("%d.%m.%Y")
    time_key = start_msk.strftime("%H:%M")

    for tr in soup.find_all("tr"):
        try:
            td_name = tr.find("td", class_=re.compile(r"name-td"))
            td_home = tr.find("td", class_=re.compile(r"owner-td"))
            td_away = tr.find("td", class_=re.compile(r"guests-td"))
            td_score = tr.find("td", class_=re.compile(r"score-td"))
            if not (td_name and td_home and td_away and td_score):
                continue

            # дата|время
            a_dt = td_name.find("a")
            dt_text = a_dt.get_text(" ", strip=True) if a_dt else ""
            if date_key not in dt_text or time_key not in dt_text:
                continue

            # команды
            a_home = td_home.find("a", class_=re.compile(r"player"))
            a_away = td_away.find("a", class_=re.compile(r"player"))
            home_txt = a_home.get("title") or a_home.get_text(" ", strip=True) if a_home else ""
            away_txt = a_away.get("title") or a_away.get_text(" ", strip=True) if a_away else ""
            if norm_team(home_txt) != home_key or norm_team(away_txt) != away_key:
                continue

            # ссылка на матч — в score-td
            a_score = td_score.find("a", href=True)
            if not a_score:
                continue
            href = a_score["href"]
            if not href.startswith("http"):
                href = "https://www.sports.ru" + href
            return href
        except Exception:
            continue

    return None

# ───── Запасной поиск sports.ru (если календарь не дал ссылку)
def find_sportsru_match_url_via_search(home_ru: str, away_ru: str, d: dt.date) -> Optional[str]:
    query = f"{home_ru} {away_ru} НХЛ {ru_date(d)} {d.year}"
    r = S.get(SPORTS_SEARCH, params={"q": query}, timeout=25)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    cands = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = a.get_text(" ", strip=True)
        if "/hockey/match/" in href and href.endswith(".html"):
            if not href.startswith("http"):
                href = "https://www.sports.ru" + href
            # в тексте результата присутствуют обе команды (хотя бы первым словом)
            if (home_ru.split()[0] in txt) and (away_ru.split()[0] in txt):
                cands.append(href)
    if not cands:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/hockey/match/" in href and href.endswith(".html"):
                if not href.startswith("http"):
                    href = "https://www.sports.ru" + href
                cands.append(href)
    return cands[0] if cands else None

def find_sportsru_match_url(home_ru: str, away_ru: str, start_msk: dt.datetime) -> Optional[str]:
    # 1) пробуем календарь
    u = find_sportsru_match_url_via_calendar(home_ru, away_ru, start_msk)
    if u:
        return u
    # 2) пробуем поиск на дату матча
    u = find_sportsru_match_url_via_search(home_ru, away_ru, start_msk.date())
    if u:
        return u
    # 3) пробуем поиск на соседнюю дату (на всякий случай)
    u = find_sportsru_match_url_via_search(home_ru, away_ru, (start_msk - dt.timedelta(days=1)).date())
    return u

# ───── Парсинг голов на sports.ru -> карта (period, "MM:SS") -> (Автор, [ассистенты])
GOAL_LINE_RE = re.compile(
    r"(?P<score>\d+:\d+)\s*[—–-]\s*(?P<time>\d{1,2}[:.]\d{2})\s+(?P<who>[А-ЯЁ][^()\n\r]*?)(?:\s*\((?P<ass>[^)]*)\))?(?=\s|$)",
    re.U
)
PERIOD_HEADERS = [
    (re.compile(r"\b1[-–]?й\s+период\b", re.I | re.U), 1),
    (re.compile(r"\b2[-–]?й\s+период\b", re.I | re.U), 2),
    (re.compile(r"\b3[-–]?й\s+период\b", re.I | re.U), 3),
    (re.compile(r"\bОвертайм(?:\s*№\s*(\d+))?\b", re.I | re.U), 4),  # 4 = OT1; если №N → 3+N
]

def ru_initial(full: str) -> str:
    t = re.sub(r"\s+", " ", (full or "").strip())
    if not t: return ""
    parts = t.split(" ")
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0][0]}. {parts[-1]}"

def parse_sportsru_goals(url: str) -> Tuple[Dict[Tuple[int,str], Tuple[str, List[str]]], Optional[str]]:
    html = get_html(url)
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text("\n", strip=True)
    txt = txt.replace("—", "–").replace("−", "–").replace("‒", "–")

    # выделим раздел с голами (между заголовками)
    start = None
    for m in re.finditer(r"(1[-–]?й\s+период|Голы|Ход матча)", txt, re.I):
        start = m.start(); break
    if start is None:
        start = 0
    endm = re.search(r"(Буллиты|Серия буллитов|Удаления|Статистика|Составы)", txt, re.I)
    end = endm.start() if endm else len(txt)
    section = txt[start:end]

    period = 1
    by_key: Dict[Tuple[int,str], Tuple[str, List[str]]] = {}
    lines = [ln.strip() for ln in section.split("\n") if ln.strip()]
    for ln in lines:
        # смена периода
        switched = False
        for rx, base in PERIOD_HEADERS:
            m = rx.search(ln)
            if m:
                if base == 4 and m.lastindex == 1 and m.group(1):
                    period = 3 + max(1, int(m.group(1)))
                else:
                    period = base
                switched = True
                break
        if switched:
            continue

        for m in GOAL_LINE_RE.finditer(ln):
            mmss = m.group("time").replace(".", ":")
            who = m.group("who").strip()
            ass = (m.group("ass") or "").strip()
            who_sh = ru_initial(re.split(r"\s+[–-]\s+", who)[0].strip())
            assists: List[str] = []
            if ass:
                for a in ass.split(","):
                    aa = ru_initial(re.split(r"\s+[–-]\s+", a.strip())[0].strip())
                    if aa:
                        assists.append(aa)
            by_key[(period, mmss)] = (who_sh, assists)

    # Победный буллит
    so_winner = None
    m = re.search(r"Победный\s+буллит[:\s–-]+([А-ЯЁ][^,\n\r]+)", txt, re.I)
    if m:
        so_winner = ru_initial(m.group(1).strip())

    return by_key, so_winner

# ───── Вывод одного матча
def build_match_block(g: dict) -> str:
    gid = g["id"]
    pbp = nhl_play_by_play(gid)

    # финальный счёт + решение
    final_home = pbp.get("homeTeam", {}).get("score", 0)
    final_away = pbp.get("awayTeam", {}).get("score", 0)
    decision = (pbp.get("gameOutcome") or {}).get("lastPeriodType")  # "REG"/"OT"/"SO"

    # голы из NHL
    nhl_goals = []
    for ev in pbp.get("plays", []):
        if (ev.get("typeDescKey") or "").lower() != "goal":
            continue
        per = int((ev.get("periodDescriptor") or {}).get("number") or 0)
        t_in = (ev.get("timeInPeriod") or ev.get("timeRemaining") or "00:00").strip()
        hs = ev.get("homeScore", 0)
        as_ = ev.get("awayScore", 0)
        nhl_goals.append({"period": per, "t": t_in, "score": f"{hs}:{as_}"})

    # русские имена со sports.ru: сначала найдём ссылку на матч через календарь
    h_emoji, h_ru = TEAM_META.get(g["home"], ("🏒", g["home"]))
    a_emoji, a_ru = TEAM_META.get(g["away"], ("🏒", g["away"]))

    url = find_sportsru_match_url(h_ru, a_ru, g["msk"])
    if not url:
        raise RuntimeError(f"Не найден матч на sports.ru для {h_ru} — {a_ru} ({g['msk']:%d.%m})")

    ru_map, so_winner = parse_sportsru_goals(url)

    # Заголовок + жирный победитель
    home_line = f"{h_emoji} «{h_ru}»: {final_home}"
    away_line = f"{a_emoji} «{a_ru}»: {final_away}"
    if final_home > final_away:
        home_line = f"<b>{home_line}</b>"
    elif final_away > final_home:
        away_line = f"<b>{away_line}</b>"
    suffix = ""
    if decision == "OT": suffix = " (ОТ)"
    if decision == "SO": suffix = " (Б)"

    parts = [home_line + suffix, away_line, ""]

    # Склейка голов по ключу (period, "MM:SS")
    missing = []
    goals_by_period: Dict[int, List[str]] = {}
    for ev in nhl_goals:
        key = (ev["period"], ev["t"])
        if key not in ru_map:
            # запасной ключ без ведущего нуля в минутах
            mm, ss = ev["t"].split(":")
            alt = f"{int(mm)}:{ss}"
            if (ev["period"], alt) in ru_map:
                key = (ev["period"], alt)
            else:
                missing.append(f"p{ev['period']} {ev['t']} ({ev['score']})")
                continue
        who, assists = ru_map[key]
        line = f"{ev['score']} – {abs_time(ev['period'], ev['t'])} {who}"
        if assists:
            line += f" ({', '.join(assists)})"
        goals_by_period.setdefault(ev["period"], []).append(line)

    if missing:
        raise RuntimeError("Не нашли соответствия по времени на sports.ru:\n" + "\n".join(missing))

    for p in sorted(goals_by_period.keys()):
        if p <= 3:
            parts.append(f"<i>{p}-й период</i>")
        else:
            parts.append(f"<i>Овертайм №{p-3}</i>")
        parts.extend(goals_by_period[p])

    if decision == "SO" and so_winner:
        parts.append("Победный буллит")
        parts.append(so_winner)

    return "\n".join(parts)

# ───── Пост целиком
def build_post(d: dt.date) -> str:
    games = pick_games(d)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(d)} • {len(games)} " + \
            ("матч" if len(games)==1 else "матча" if len(games)%10 in (2,3,4) and not 12<=len(games)%100<=14 else "матчей")
    head = f"{title}\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    if not games:
        return head.strip()

    blocks = []
    for i, g in enumerate(games, 1):
        blocks.append(build_match_block(g))
        if i < len(games):
            blocks.append("")
    return head + "\n".join(blocks).strip()

# ───── Telegram
def tg_send(text: str):
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN","").strip(), os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat:
        print(text)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = S.post(url, json={"chat_id": chat, "text": text, "parse_mode":"HTML", "disable_web_page_preview": True}, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:200]}")

# ───── main
if __name__ == "__main__":
    try:
        d = report_date()
        msg = build_post(d)
        tg_send(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
