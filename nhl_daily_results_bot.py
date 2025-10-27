#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL → Telegram (RU): NHL-время, sports.ru-имена

Логика:
1) Берём окно игрового дня по МСК:
   - все матчи REPORT_DATE,
   - и матчи REPORT_DATE-1, начавшиеся >= 15:00 МСК.
2) Для каждого матча:
   - тянем play-by-play из api-web.nhle.com (период, timeInPeriod, счёт после гола).
   - ищем этот матч на sports.ru через их поиск:
       https://www.sports.ru/search/?q=<Команда1> <Команда2> НХЛ <дата по-русски>
     из результатов берём первую ссылку вида /hockey/match/....html,
     в заголовке/крошках которой встречаются обе команды.
   - с матча sports.ru вытягиваем список голов по периодам в виде:
       "1:0 – 12:34 Имя Фамилия (Ассистент, ...)"
     и строим словарь: ключ = (period, "MM:SS"), значение = (Автор, [ассисты])
   - склеиваем: для каждого гола из NHL берём русские фамилии по ключу (period, timeInPeriod).
   - если не найдено — падаем (чтобы не было английских имен).
   - буллиты: ищем "Победный буллит" на sports.ru, берём имя.
3) Печать: жирным победитель, по периодам, время в абсолюте (mm.ss), только победный буллит.

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  REPORT_DATE=YYYY-MM-DD (опционально, МСК)
"""

import os, sys, re, json, time, random, datetime as dt
from zoneinfo import ZoneInfo
from html import escape
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ───── Настройки
TZ_MSK = ZoneInfo("Europe/Moscow")
API = "https://api-web.nhle.com"
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
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабра"
}
def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, connect=5, read=5, backoff_factor=0.6,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-RU-Merger/1.0",
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

# ───── NHL schedule & pbp
def pick_games(d: dt.date) -> List[dict]:
    start, end = window_msk(d)
    all_days = [d - dt.timedelta(days=1), d]
    games = []
    for day in all_days:
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

def pbp_and_scores(gid: int) -> Tuple[List[dict], dict]:
    pbp = get_json(f"{API}/v1/gamecenter/{gid}/play-by-play")
    # list of goals
    events = []
    for p in pbp.get("plays", []):
        if (p.get("typeDescKey") or "").lower() != "goal":
            continue
        per = int((p.get("periodDescriptor") or {}).get("number") or 0)
        t_in = (p.get("timeInPeriod") or p.get("timeRemaining") or "00:00").strip()
        hs = p.get("homeScore", 0); as_ = p.get("awayScore", 0)
        events.append({"period": per, "t": t_in, "score": f"{hs}:{as_}"})
    outcome = (pbp.get("gameOutcome") or {}).get("lastPeriodType")  # REG/OT/SO
    return events, {"decision": outcome}

# ───── Время: абсолютные минуты
def abs_time(period: int, mmss: str) -> str:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", mmss)
    if not m:
        return mmss.replace(":", ".")
    mm, ss = int(m.group(1)), int(m.group(2))
    base = (period-1)*20 if period<=3 else 60 + 5*(period-4)
    return f"{base + mm}.{ss:02d}"

# ───── Поиск матча на sports.ru
def build_search_query(home_ru: str, away_ru: str, d: dt.date) -> str:
    # Пример запроса: "Тампа-Бэй Анахайм НХЛ 27 октября 2025"
    return f"{home_ru} {away_ru} НХЛ {ru_date(d)} {d.year}"

def find_sportsru_match_url(home_ru: str, away_ru: str, d: dt.date) -> Optional[str]:
    q = build_search_query(home_ru, away_ru, d)
    r = S.get(SPORTS_SEARCH, params={"q": q}, timeout=25)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    # В результатах ищем первую ссылку на матч: /hockey/match/.....html
    cands = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = a.get_text(" ", strip=True)
        if "/hockey/match/" in href and href.endswith(".html"):
            if not href.startswith("http"):
                href = "https://www.sports.ru" + href
            # проверим, что в тексте встречаются обе команды
            if home_ru.split()[0] in txt and away_ru.split()[0] in txt:
                cands.append(href)
    # если пусто — возьмём вообще первую матчевую ссылку
    if not cands:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/hockey/match/" in href and href.endswith(".html"):
                if not href.startswith("http"):
                    href = "https://www.sports.ru" + href
                cands.append(href)
    return cands[0] if cands else None

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

    # Выделяем блок голов (между заголовками разделов)
    start = None
    for m in re.finditer(r"(1[-–]?й\s+период|Голы|Ход матча)", txt, re.I):
        start = m.start(); break
    if start is None:
        start = 0
    endm = re.search(r"(Буллиты|Серия буллитов|Удаления|Статистика|Составы)", txt, re.I)
    end = endm.start() if endm else len(txt)
    section = txt[start:end]

    # Разбираем
    period = 1
    by_key: Dict[Tuple[int,str], Tuple[str, List[str]]] = {}
    lines = [ln.strip() for ln in section.split("\n") if ln.strip()]
    for ln in lines:
        # смена периода
        sw = False
        for rx, base in PERIOD_HEADERS:
            m = rx.search(ln)
            if m:
                if base == 4 and m.lastindex == 1 and m.group(1):
                    period = 3 + max(1, int(m.group(1)))
                else:
                    period = base
                sw = True
                break
        if sw:
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

    # Победный буллит:
    so_winner = None
    m = re.search(r"Победный\s+буллит[:\s–-]+([А-ЯЁ][^,\n\r]+)", txt, re.I)
    if m:
        so_winner = ru_initial(m.group(1).strip())

    return by_key, so_winner

# ───── Вывод матча
def build_match_block(g: dict) -> str:
    gid = g["id"]
    nhl_goals, meta = pbp_and_scores(gid)
    decision = meta.get("decision")  # REG/OT/SO
    h_emoji, h_ru = TEAM_META.get(g["home"], ("🏒", g["home"]))
    a_emoji, a_ru = TEAM_META.get(g["away"], ("🏒", g["away"]))

    # Найдём страницу матча на sports.ru (ищем по дате отчёта, а не по старту — чаще покрывает случай)
    url = find_sportsru_match_url(h_ru, a_ru, g["msk"].date())
    if not url:
        # попробуем накануне (если матч попал из вчерашних)
        url = find_sportsru_match_url(h_ru, a_ru, (g["msk"] - dt.timedelta(days=1)).date())
    if not url:
        raise RuntimeError(f"Не найден матч на sports.ru для {h_ru} — {a_ru} ({g['msk']:%d.%m})")

    ru_map, so_winner = parse_sportsru_goals(url)

    # Итоговый счёт (возьмём из последнего гола NHL; если серии/ОТ — добавим пометку)
    if nhl_goals:
        last = nhl_goals[-1]["score"]
        hs, as_ = map(int, last.split(":"))
    else:
        # если голов не было — возьмём из страницы sports.ru заголовок (упустим — редкость)
        hs = as_ = 0

    # Жирным победителя
    home_line = f"{h_emoji} «{h_ru}»: {hs}"
    away_line = f"{a_emoji} «{a_ru}»: {as_}"
    if hs > as_:
        home_line = f"<b>{home_line}</b>"
    elif as_ > hs:
        away_line = f"<b>{away_line}</b>"
    suffix = ""
    if decision == "OT": suffix = " (ОТ)"
    if decision == "SO": suffix = " (Б)"

    parts = [home_line + suffix, away_line, ""]

    # Склейка: для каждого гола NHL найдём автора/ассистов по (period, timeInPeriod) на sports.ru
    # Если ключей не хватает — соберём список и упадём одной ошибкой
    missing = []
    goals_by_period: Dict[int, List[str]] = {}
    for ev in nhl_goals:
        key = (ev["period"], ev["t"])
        if key not in ru_map:
            # Иногда sports.ru пишет 09:05 vs 9:05 — у нас нормализовано, но подстрахуемся: уберём ведущий ноль в минутах
            mm, ss = ev["t"].split(":")
            alt = f"{int(mm)}:{ss}"
            if (ev["period"], alt) in ru_map:
                key = (ev["period"], alt)
            else:
                missing.append(f"p{ev['period']} {ev['t']} (score {ev['score']})")
                continue
        who, assists = ru_map[key]
        line = f"{ev['score']} – {abs_time(ev['period'], ev['t'])} {who}"
        if assists:
            line += f" ({', '.join(assists)})"
        goals_by_period.setdefault(ev["period"], []).append(line)

    if missing:
        raise RuntimeError("Не нашли соответствия по времени на sports.ru:\n" + "\n".join(missing))

    # Печать по периодам
    for p in sorted(goals_by_period.keys()):
        if p <= 3:
            parts.append(f"<i>{p}-й период</i>")
        else:
            parts.append(f"<i>Овертайм №{p-3}</i>")
        parts.extend(goals_by_period[p])

    # Победный буллит (если был и нашли имя)
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
