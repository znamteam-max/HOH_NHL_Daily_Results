#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram (sports.ru scraper)
- Берёт с https://www.sports.ru/hockey/tournament/nhl/calendar/ список завершённых матчей за нужную дату
- Заходит на страницу каждого матча и вытаскивает ленту голов (команда → [время, автор, ассистенты])
- Собирает пост в указанном формате и отправляет в телеграм-канал
"""

import os
import sys
import re
import time
from html import escape
from urllib.parse import urljoin
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ------ настройки окружения ------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BASE = "https://www.sports.ru"
CALENDAR_URL = f"{BASE}/hockey/tournament/nhl/calendar/"

# ------ HTTP с ретраями ------
def make_session():
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-DailyResultsBot/1.3 (+sports.ru; Telegram)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

# ------ вспомогалки ------
RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    # формы: ("матч", "матча", "матчей")
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

TEAM_EMOJI = {
    "Анахайм": "🦆", "Аризона": "🤠", "Бостон": "🐻", "Баффало": "🦬",
    "Калгари": "🔥", "Каролина": "🌪️", "Колорадо": "⛰", "Коламбус": "💣",
    "Даллас": "⭐", "Детройт": "🔴", "Эдмонтон": "🛢", "Флорида": "🐆",
    "Лос-Анджелес": "👑", "Миннесота": "🌲", "Монреаль": "🇨🇦", "Нэшвилл": "🐯",
    "Нью-Джерси": "😈", "Айлендерс": "🟠", "Рейнджерс": "🗽", "Оттава": "🛡",
    "Филадельфия": "🛩", "Питтсбург": "🐧", "Сан-Хосе": "🦈", "Сиэтл": "🦑",
    "Сент-Луис": "🎵", "Тампа-Бэй": "⚡", "Торонто": "🍁", "Ванкувер": "🐳",
    "Вегас": "🎰", "Вашингтон": "🦅", "Виннипег": "✈", "Юта": "🦣",
    "Чикаго": "🦅", "Колорадо Эвеланч": "⛰", "Лос-Анджелес Кингз": "👑"
}

def emj(team: str) -> str:
    for k, v in TEAM_EMOJI.items():
        if k.lower() in team.lower():
            return v
    return "🏒"

def canonical_team_name(raw: str) -> str:
    """
    Приводим название команды к «короткому» виду как в словаре эмодзи.
    Например, 'Нью-Джерси Девилз' → 'Нью-Джерси'
    """
    raw_l = raw.lower()
    for short in TEAM_EMOJI.keys():
        if short.lower() in raw_l or raw_l in short.lower():
            return short
    # как есть
    return raw.strip()

def get_london_today():
    return datetime.now(ZoneInfo("Europe/London")).date()

def log(*a):
    print(*a, file=sys.stderr)

# ------ парсинг календаря за нужную дату ------
def fetch_calendar_html() -> str:
    r = S.get(CALENDAR_URL, timeout=30)
    r.raise_for_status()
    return r.text

def parse_calendar_for_date(html: str, target: date) -> list[dict]:
    """
    Ищем строки таблицы, где дата == target и счёт уже не '- : -'
    Возвращаем список: {home, away, score, match_url}
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tr")
    target_str = target.strftime("%d.%m.%Y")

    out = []
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        dt_text = " ".join(tds[0].get_text(" ", strip=True).split())
        if target_str not in dt_text:
            continue

        score_cell = tds[2]
        score_text = score_cell.get_text(" ", strip=True)
        if "- : -" in score_text or score_text.strip() in {"-:-", "- : -"}:
            continue

        a_score = score_cell.find("a")
        if not a_score or "/hockey/match/" not in (a_score.get("href") or ""):
            continue

        home_team = tds[1].get_text(" ", strip=True)
        away_team = tds[3].get_text(" ", strip=True)
        match_url = urljoin(BASE, a_score["href"])

        # нормализуем счёт
        score_text = re.sub(r"\s+", "", score_text)
        out.append({
            "home": canonical_team_name(home_team),
            "away": canonical_team_name(away_team),
            "score": score_text,   # 5:3, 4:5от, 2:3б и т.п.
            "url": match_url
        })

    return out

# ------ парсинг страницы матча (устойчивая лента голов) ------
# Разные варианты буллитов/дефисов/пробелов, имя на кириллице/латинице, возможны апострофы, дефисы и т.д.
NAME_CHARS = r"A-Za-zÀ-ÿА-Яа-яЁёʼ’'`\-\. "
GOAL_LINE_RE = re.compile(
    rf"^[\s\-\•\*·—–]*"                      # буллит/дефис/тире (опционально)
    rf"(?P<time>\d{{1,2}}(?::\d{{2}})?)"     # 10 или 10:40
    rf"\s+"
    rf"(?P<scorer>[{NAME_CHARS}]+?)"         # фамилия/имя/комбинации
    rf"(?:\s*\((?P<ast>[^)]*)\))?"           # ассисты в скобках (опц.)
    rf"\s*$",
    re.M
)

def minute_from_timestr(timestr: str) -> int:
    timestr = timestr.strip()
    if ":" in timestr:
        m, _ = timestr.split(":", 1)
        return int(m)
    return int(timestr)

def detect_events_by_headers(full_text: str, home: str, away: str) -> list[dict]:
    """
    План A: на странице часто есть два блока вида:
        <Нью-Джерси>
          * 21 Хэмилтон (Хишир, Мерсер)
        <Сан-Хосе>
          * 2 Эклунд (Селебрини)
    Здесь мы сканируем весь текст построчно: когда встречаем заголовок,
    помечаем current_team; все строки-гoлы ниже (по regex) относим к этой команде
    до следующего заголовка.
    """
    known_headers = {home, away}
    events = []
    current = None

    for raw_line in full_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # заголовок-команда?
        if line in known_headers:
            current = line
            continue
        # гол?
        m = GOAL_LINE_RE.match(line)
        if m and current in known_headers:
            t = m.group("time")
            scorer = " ".join((m.group("scorer") or "").split())
            assists = " ".join((m.group("ast") or "").split())
            events.append({
                "team": "home" if current == home else "away",
                "min": minute_from_timestr(t),
                "scorer": scorer,
                "assists": assists
            })

    return events

def detect_events_anywhere(full_text: str, home: str, away: str) -> list[dict]:
    """
    План B: если явных заголовков нет, идём по всему тексту,
    но в качестве текущего заголовка используем последние встреченные короткие названия команд.
    """
    possible_headers = set(TEAM_EMOJI.keys())
    events = []
    current = None

    for raw_line in full_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # если строка совпала с любым известным коротким названием — переключаем текущую команду
        if line in possible_headers:
            # но учитываем только нужные в этом матче
            if line == home or line == away:
                current = line
            continue
        m = GOAL_LINE_RE.match(line)
        if m and current in (home, away):
            t = m.group("time")
            scorer = " ".join((m.group("scorer") or "").split())
            assists = " ".join((m.group("ast") or "").split())
            events.append({
                "team": "home" if current == home else "away",
                "min": minute_from_timestr(t),
                "scorer": scorer,
                "assists": assists
            })
    return events

def parse_match_goals(match_url: str, home: str, away: str) -> list[dict]:
    """
    Возвращает список голов с привязкой к команде в хронологии:
      [{"team":"home"/"away","min":int,"scorer":str,"assists":str}]
    """
    r = S.get(match_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    full_text = soup.get_text("\n")

    # 1) пробуем самый надёжный путь — явные заголовки-команды
    events = detect_events_by_headers(full_text, home, away)
    if events:
        events.sort(key=lambda x: x["min"])
        return events

    # 2) fallback — искать заголовки среди всех известных коротких названий
    events = detect_events_anywhere(full_text, home, away)
    if events:
        events.sort(key=lambda x: x["min"])
        return events

    # 3) Ничего не нашли — вернём пусто (верхний уровень добавит заглушку)
    return []

# ------ сборка сообщения ------
def build_post(target_day: date) -> str:
    cal_html = fetch_calendar_html()
    matches = parse_calendar_for_date(cal_html, target_day)

    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(target_day)} • {len(matches)} {ru_plural(len(matches), ('матч', 'матча', 'матчей'))}\n\n"
    title += "Результаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    chunks = [title]

    for i, m in enumerate(matches, 1):
        home, away, score = m["home"], m["away"], m["score"]

        # финальный счёт по числам для шапки
        m_sc = re.match(r"^\s*(\d+)\s*:\s*(\d+)", score)
        hs, as_ = ("?", "?")
        if m_sc:
            hs, as_ = m_sc.group(1), m_sc.group(2)

        # лента голов
        lines = []
        try:
            events = parse_match_goals(m["url"], home, away)
            h_go, a_go = 0, 0
            for ev in events:
                if ev["team"] == "home":
                    h_go += 1
                else:
                    a_go += 1
                # формат: 0:1 – 10 Назар (Терявяйнен, Бертуцци)
                assists = f" ({ev['assists']})" if ev["assists"] else ""
                lines.append(f"{h_go}:{a_go} – {ev['min']} {ev['scorer']}{assists}")
        except Exception as e:
            log(f"[WARN] goals parse failed for {home} vs {away}: {e}")

        if not lines:
            lines.append("— подробная запись голов недоступна")

        block = (
            f"{emj(home)} «{home}»: {hs}\n"
            f"{emj(away)} «{away}»: {as_}\n\n" +
            "\n".join(lines)
        )

        chunks.append(block.rstrip() + ("\n\n" if i < len(matches) else ""))

    return "".join(chunks).strip()

# ------ отправка в Telegram (деление на части для лимита 4096) ------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    # делим по ~3500, стараясь резать по пустой строке
    MAX = 3500
    parts = []
    t = text
    while t:
        if len(t) <= MAX:
            parts.append(t)
            break
        cut = t.rfind("\n\n", 0, MAX)
        if cut == -1:
            cut = MAX
        parts.append(t[:cut])
        t = t[cut:].lstrip()

    for part in parts:
        resp = S.post(url, json={
            "chat_id": CHAT_ID,
            "text": part,
            "disable_web_page_preview": True,
        }, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
        time.sleep(0.6)

# ------ main ------
if __name__ == "__main__":
    try:
        # Берём по Лондону «сегодняшнюю» дату.
        # Из календаря включаем только завершённые матчи (там уже есть финальный счёт).
        target = get_london_today()
        post = build_post(target)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
