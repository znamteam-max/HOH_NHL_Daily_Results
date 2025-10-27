#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL → Telegram (RU)
- События (время, счёт, порядок) берём из api-web.nhle.com.
- СТРАНИЦУ МАТЧА и ФАМИЛИИ (кириллица) берём со sports.ru:
  1) сначала ищем ссылку в календаре турнира с допуском по дате (±1 день) и по минимальной разнице времени;
  2) если не нашли — запасной поиск по сайту.
- Склейка голов: (период, время) → если нет — по счёту после гола → если нет — ближайшее время в том же периоде (±15с) → если нет — по порядку.
- Время печатаем как абсолютные минуты матча (mm.ss).
- В буллитах печатаем только «Победный буллит».

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  REPORT_DATE=YYYY-MM-DD (необязательно; по умолчанию сегодня по Europe/Moscow)

Зависимости:
  requests==2.32.3
  beautifulsoup4==4.12.3
"""

import os
import sys
import re
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ───── Настройки/константы
TZ_MSK = ZoneInfo("Europe/Moscow")
API = "https://api-web.nhle.com"
SPORTS_CAL = "https://www.sports.ru/hockey/tournament/nhl/calendar/"
SPORTS_SEARCH = "https://www.sports.ru/search/"

TEAM_META: Dict[str, Tuple[str, str]] = {
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
    # UTA = Utah Hockey Club
    "UTA": ("🦣", "Юта"),
    # На всякий случай старое ARI → Юта
    "ARI": ("🦣", "Юта"),
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
        "User-Agent": "NHL-RU-Merger/1.4",
        "Accept": "text/html,application/json,*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
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

# ───── Окно игрового дня (по МСК)
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
    games: List[dict] = []
    for day in (d - dt.timedelta(days=1), d):
        js = get_json(f"{API}/v1/schedule/{day.isoformat()}")
        lst = js.get("games") or js.get("gameWeek",[{}])[0].get("games",[])
        games.extend(lst)
    picked: List[dict] = []
    seen = set()
    for g in games:
        gid = int(g.get("id") or 0)
        if not gid or gid in seen:
            continue
        seen.add(gid)
        utc = g.get("startTimeUTC") or g.get("startTime")
        if not utc:
            continue
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

# ───── Время: elapsed, абсолютные минуты
def to_elapsed_mmss(period: int, time_in_period: Optional[str], time_remaining: Optional[str]) -> str:
    """
    Если дано timeInPeriod — используем его (это прошедшее время).
    Иначе считаем из timeRemaining: elapsed = period_len - remaining.
    """
    if time_in_period and re.match(r"^\d{1,2}:\d{2}$", time_in_period):
        return time_in_period
    if time_remaining and re.match(r"^\d{1,2}:\d{2}$", time_remaining):
        pr_len = 20 if period <= 3 else 5
        mm, ss = map(int, time_remaining.split(":"))
        total = pr_len*60 - (mm*60 + ss)
        if total < 0:
            total = 0
        return f"{total//60}:{total%60:02d}"
    # на худой случай
    t = time_in_period or time_remaining or "0:00"
    return t.replace(".", ":")

def abs_time(period: int, mmss: str) -> str:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", mmss)
    if not m:
        return mmss.replace(":", ".")
    mm, ss = int(m.group(1)), int(m.group(2))
    base = (period-1)*20 if period <= 3 else 60 + 5*(period-4)
    return f"{base + mm}.{ss:02d}"

# ───── Нормализация и сравнение названий команд (чтобы ловить «Юта» vs «Юта ХК»)
def _norm_team_key(s: str) -> str:
    t = s.lower()
    t = re.sub(r"[^a-zа-яё]+", " ", t)
    t = t.replace(" хк ", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _teams_match(a: str, b: str) -> bool:
    ak = _norm_team_key(a)
    bk = _norm_team_key(b)
    return ak == bk or ak in bk or bk in ak

# ───── Календарь sports.ru: допуск по дате (±1 день) и минимальная разница времени
def _parse_dt_from_td(a_dt_text: str) -> Tuple[Optional[dt.date], Optional[dt.time]]:
    # пример "26.10.2025|20:00"
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4}).*?(\d{2}):(\d{2})", a_dt_text)
    if not m:
        return None, None
    d, mth, y, hh, mm = map(int, m.groups())
    try:
        return dt.date(y, mth, d), dt.time(hh, mm)
    except Exception:
        return None, None

def find_sportsru_match_url_via_calendar(home_ru: str, away_ru: str, start_msk: dt.datetime) -> Optional[str]:
    html = get_html(SPORTS_CAL)
    soup = BeautifulSoup(html, "html.parser")

    best: Optional[Tuple[int, str]] = None  # (abs_minutes_diff, href)
    fallback_same_day: List[str] = []

    for tr in soup.find_all("tr"):
        td_name = tr.find("td", class_=re.compile(r"name-td"))
        td_home = tr.find("td", class_=re.compile(r"owner-td"))
        td_away = tr.find("td", class_=re.compile(r"guests-td"))
        td_score = tr.find("td", class_=re.compile(r"score-td"))
        if not (td_name and td_home and td_away and td_score):
            continue

        a_dt = td_name.find("a")
        dt_text = a_dt.get_text(" ", strip=True) if a_dt else ""
        row_date, row_time = _parse_dt_from_td(dt_text)
        if row_date is None:
            continue

        # Допускаем сдвиг даты ±1 день относительно начала по МСК
        if abs((row_date - start_msk.date()).days) > 1:
            continue

        a_home = td_home.find("a", class_=re.compile(r"player"))
        a_away = td_away.find("a", class_=re.compile(r"player"))
        home_txt = (a_home.get("title") or a_home.get_text(" ", strip=True)) if a_home else ""
        away_txt = (a_away.get("title") or a_away.get_text(" ", strip=True)) if a_away else ""

        # допускаем любые вариации («Юта» ↔ «Юта ХК», «Нью-Джерси» ↔ «Нью-Джерси Дэвилз»)
        ok_direct = _teams_match(home_txt, home_ru) and _teams_match(away_txt, away_ru)
        ok_swapped = _teams_match(home_txt, away_ru) and _teams_match(away_txt, home_ru)
        if not (ok_direct or ok_swapped):
            continue

        a_score = td_score.find("a", href=True)
        if not a_score:
            continue
        href = a_score["href"]
        if not href.startswith("http"):
            href = "https://www.sports.ru" + href

        # если нет времени — просто запомним как кандидат на эту дату
        if row_time is None:
            fallback_same_day.append(href)
            continue

        row_dt = dt.datetime.combine(row_date, row_time, tzinfo=TZ_MSK)
        diff_min = abs(int((row_dt - start_msk).total_seconds() // 60))
        pair = (diff_min, href)
        if (best is None) or (pair[0] < best[0]):
            best = pair

    if best is not None:
        return best[1]
    if len(fallback_same_day) == 1:
        return fallback_same_day[0]
    return None

# ───── Запасной поиск sports.ru
def find_sportsru_match_url_via_search(home_ru: str, away_ru: str, d: dt.date) -> Optional[str]:
    query = f"{home_ru} {away_ru} НХЛ {ru_date(d)} {d.year}"
    r = S.get(SPORTS_SEARCH, params={"q": query}, timeout=25)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    cands: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = a.get_text(" ", strip=True)
        if "/hockey/match/" in href and href.endswith(".html"):
            if not href.startswith("http"):
                href = "https://www.sports.ru" + href
            # расслабим проверку: достаточно, чтобы встречалось первое слово команды
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
    u = find_sportsru_match_url_via_calendar(home_ru, away_ru, start_msk)
    if u:
        return u
    # пробуем поиск на дату старта и соседние
    for delta in (0, -1, 1):
        u = find_sportsru_match_url_via_search(home_ru, away_ru, (start_msk + dt.timedelta(days=delta)).date())
        if u:
            return u
    return None

# ───── Парсинг голов на sports.ru
GOAL_LINE_RE = re.compile(
    r"(?P<score>\d+:\d+)\s*[—–-]\s*(?P<time>\d{1,2}[:.]\d{2})\s+(?P<who>[А-ЯЁ][^()\n\r]*?)(?:\s*\((?P<ass>[^)]*)\))?(?=\s|$)",
    re.U
)
PERIOD_HEADERS = [
    (re.compile(r"\b1[-–]?й\s+период\b", re.I | re.U), 1),
    (re.compile(r"\b2[-–]?й\s+период\b", re.I | re.U), 2),
    (re.compile(r"\b3[-–]?й\s+период\b", re.I | re.U), 3),
    (re.compile(r"\bОвертайм(?:\s*№\s*(\d+))?\b", re.I | re.U), 4),  # 4=OT1; №N → 3+N
]

def ru_initial(full: str) -> str:
    t = re.sub(r"\s+", " ", (full or "").strip())
    if not t:
        return ""
    parts = t.split(" ")
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0][0]}. {parts[-1]}"

def parse_sportsru_goals(url: str) -> Tuple[List[dict], Optional[str]]:
    html = get_html(url)
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text("\n", strip=True)
    txt = txt.replace("—", "–").replace("−", "–").replace("‒", "–")

    # выделим раздел с голами
    start = None
    for m in re.finditer(r"(1[-–]?й\s+период|Голы|Ход матча)", txt, re.I):
        start = m.start()
        break
    if start is None:
        start = 0
    endm = re.search(r"(Буллиты|Серия буллитов|Удаления|Статистика|Составы)", txt, re.I)
    end = endm.start() if endm else len(txt)
    section = txt[start:end]

    period = 1
    goals: List[dict] = []
    lines = [ln.strip() for ln in section.split("\n") if ln.strip()]
    for ln in lines:
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
            who_full = m.group("who").strip()
            ass = (m.group("ass") or "").strip()
            who = ru_initial(re.split(r"\s+[–-]\s+", who_full)[0].strip())
            assists: List[str] = []
            if ass:
                for a in ass.split(","):
                    aa = ru_initial(re.split(r"\s+[–-]\s+", a.strip())[0].strip())
                    if aa:
                        assists.append(aa)
            goals.append({
                "period": period,
                "t": mmss,
                "score": m.group("score"),
                "who": who,
                "assists": assists,
            })

    # Победный буллит
    so_winner = None
    m = re.search(r"Победный\s+буллит[:\s–-]+([А-ЯЁ][^,\n\r]+)", txt, re.I)
    if m:
        so_winner = ru_initial(m.group(1).strip())

    return goals, so_winner

# ───── Сопоставление голов NHL ↔ sports.ru
def mmss_to_seconds(mmss: str) -> int:
    mm, ss = mmss.split(":")
    return int(mm) * 60 + int(ss)

def match_goals(nhl_goals: List[dict], ru_goals: List[dict]) -> List[dict]:
    """
    Возвращает список goal_rows в том же порядке, что nhl_goals,
    где каждая строка уже с русскими фамилиями.
    Алгоритм сопоставления:
      1) точное (period, time)
      2) по score
      3) ближайшее время в том же периоде (±15с)
      4) по порядку (следующая неиспользованная запись)
    """
    by_ptime: Dict[Tuple[int, str], List[int]] = {}
    by_score: Dict[str, List[int]] = {}
    for idx, g in enumerate(ru_goals):
        by_ptime.setdefault((g["period"], g["t"]), []).append(idx)
        by_score.setdefault(g["score"], []).append(idx)

    used = set()
    out: List[dict] = []

    def take(idx: int) -> dict:
        used.add(idx)
        rg = ru_goals[idx]
        return {"who": rg["who"], "assists": rg["assists"]}

    for ev in nhl_goals:
        p, t, sc = ev["period"], ev["t"], ev["score"]

        # 1) exact period+time
        cand = [j for j in by_ptime.get((p, t), []) if j not in used]
        if not cand:
            # вариант без ведущего нуля в минутах
            mm, ss = t.split(":")
            alt = f"{int(mm)}:{ss}"
            cand = [j for j in by_ptime.get((p, alt), []) if j not in used]
        if cand:
            out.append(take(cand[0]))
            continue

        # 2) by score
        cand = [j for j in by_score.get(sc, []) if j not in used]
        if cand:
            out.append(take(cand[0]))
            continue

        # 3) nearest time in same period (±15s)
        nhl_sec = mmss_to_seconds(t)
        best = None
        for j, rg in enumerate(ru_goals):
            if j in used or rg["period"] != p:
                continue
            diff = abs(mmss_to_seconds(rg["t"]) - nhl_sec)
            if diff <= 15:
                if (best is None) or diff < best[0]:
                    best = (diff, j)
        if best:
            out.append(take(best[1]))
            continue

        # 4) next unused by order
        fallback = next((j for j in range(len(ru_goals)) if j not in used), None)
        if fallback is not None:
            out.append(take(fallback))
            continue

        out.append({"who": "—", "assists": []})

    return out

# ───── Построение блока матча
def build_match_block(g: dict) -> str:
    gid = g["id"]
    pbp = nhl_play_by_play(gid)

    final_home = pbp.get("homeTeam", {}).get("score", 0)
    final_away = pbp.get("awayTeam", {}).get("score", 0)
    decision = (pbp.get("gameOutcome") or {}).get("lastPeriodType")  # REG/OT/SO

    # Голы NHL (в хронологическом порядке)
    nhl_goals: List[dict] = []
    for ev in pbp.get("plays", []):
        if (ev.get("typeDescKey") or "").lower() != "goal":
            continue
        per = int((ev.get("periodDescriptor") or {}).get("number") or 0)
        t_elapsed = to_elapsed_mmss(per, ev.get("timeInPeriod"), ev.get("timeRemaining"))
        hs = ev.get("homeScore", 0)
        as_ = ev.get("awayScore", 0)
        nhl_goals.append({"period": per, "t": t_elapsed, "score": f"{hs}:{as_}"})

    # Страница матча на sports.ru
    h_emoji, h_ru = TEAM_META.get(g["home"], ("🏒", g["home"]))
    a_emoji, a_ru = TEAM_META.get(g["away"], ("🏒", g["away"]))
    url = find_sportsru_match_url(h_ru, a_ru, g["msk"])
    if not url:
        raise RuntimeError(f"Не найден матч на sports.ru для {h_ru} — {a_ru} ({g['msk']:%d.%m})")

    ru_goals, so_winner = parse_sportsru_goals(url)

    # Сопоставим списки
    ru_rows = match_goals(nhl_goals, ru_goals)

    # Заголовок (жирным победителя)
    home_line = f"{h_emoji} «{h_ru}»: {final_home}"
    away_line = f"{a_emoji} «{a_ru}»: {final_away}"
    if final_home > final_away:
        home_line = f"<b>{home_line}</b>"
    elif final_away > final_home:
        away_line = f"<b>{away_line}</b>"
    suffix = " (ОТ)" if decision == "OT" else " (Б)" if decision == "SO" else ""
    parts = [home_line + suffix, away_line, ""]

    # Печать по периодам
    goals_by_period: Dict[int, List[str]] = {}
    for ev, names in zip(nhl_goals, ru_rows):
        line = f"{ev['score']} – {abs_time(ev['period'], ev['t'])} {names['who']}"
        if names["assists"]:
            line += f" ({', '.join(names['assists'])})"
        goals_by_period.setdefault(ev["period"], []).append(line)

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

    blocks: List[str] = []
    for i, g in enumerate(games, 1):
        blocks.append(build_match_block(g))
        if i < len(games):
            blocks.append("")
    return head + "\n".join(blocks).strip()

# ───── Telegram
def tg_send(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID","").strip()
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
