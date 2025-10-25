#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram (sports.ru scraper)
- Берёт с https://www.sports.ru/hockey/tournament/nhl/calendar/ список завершённых матчей за нужную дату
- Заходит на страницу каждого матча и вытаскивает ленту голов (команда → [время, автор, ассистенты])
- Собирает пост в формате из ТЗ и отправляет в телеграм-канал
"""

import os
import sys
import re
import math
import time
import textwrap
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
        "User-Agent": "NHL-DailyResultsBot/1.0 (+https://sports.ru; telegram @nhl_results)",
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
    "Калгари": "🔥", "Каролина": "🌪️", "Колорадо": "⛰️", "Коламбус": "💣",
    "Даллас": "⭐", "Детройт": "🚗", "Эдмонтон": "🛢️", "Флорида": "🐆",
    "Лос-Анджелес": "🎥", "Миннесота": "🌲", "Монреаль": "🏒", "Нэшвилл": "🎸",
    "Нью-Джерси": "😈", "Айлендерс": "🏝️", "Рейнджерс": "🗽", "Оттава": "🏛️",
    "Филадельфия": "🟧", "Питтсбург": "🐧", "Сан-Хосе": "🦈", "Сиэтл": "🦑",
    "Сент-Луис": "🎼", "Тампа-Бэй": "⚡", "Торонто": "🍁", "Ванкувер": "🐋",
    "Вегас": "🎰", "Вашингтон": "🦅", "Виннипег": "✈️", "Юта": "⛰️",
}

def emj(team: str) -> str:
    for k, v in TEAM_EMOJI.items():
        if k.lower() in team.lower():
            return v
    return "🏒"

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
    Ищем все строки таблицы, где дата == target и счёт уже не '- : -'
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
        if "-" in score_text and ":" in score_text and score_text.strip().startswith("-"):
            # ещё не сыграно: "- : -"
            continue

        a_score = score_cell.find("a")
        if not a_score or "/hockey/match/" not in (a_score.get("href") or ""):
            continue

        home_team = tds[1].get_text(" ", strip=True)
        away_team = tds[3].get_text(" ", strip=True)
        match_url = urljoin(BASE, a_score["href"])

        # нормализуем пробелы и разделители счёта
        score_text = score_text.replace(" ", "").replace(":", ":")
        out.append({
            "home": home_team,
            "away": away_team,
            "score": score_text,   # вида 5:3, 4:5б, 2:3от и т.п.
            "url": match_url
        })

    return out

# ------ парсинг страницы матча (лента голов в верхнем блоке) ------
GOAL_LINE_RE = re.compile(
    r"^\*\s*(?P<time>\d{1,2}:\d{2})\s*(?P<scorer>[^(,\n]+?)\s*(?:\((?P<ast>[^)]+)\))?\s*$",
    re.M
)

def extract_goal_block(text: str, team_name: str) -> str | None:
    """
    Внутри плоского .get_text() страницы матчей на sports.ru
    наверху идёт:
      <Команда A>
        * 20:40 Хэмилтон (Мерсер, Хишир)
        * ...
      <Команда B>
        * 2:25 Эклунд (Селебрини)
    Возвращаем текстовый блок после лейбла team_name до следующей команды/заголовка.
    """
    # Ищем начало блока
    start = text.find(f"\n{team_name}\n")
    if start == -1:
        return None
    # Следующий заголовок — это либо имя другой команды, либо "## " (новый раздел)
    rest = text[start + len(team_name) + 2:]
    # обрежем по ближайшему "## " (например, "## Трансляция")
    cut = rest.find("\n##")
    if cut != -1:
        rest = rest[:cut]
    return rest

def parse_team_goals_from_block(block: str) -> list[dict]:
    goals = []
    for m in GOAL_LINE_RE.finditer(block):
        t = m.group("time")
        scorer = " ".join(m.group("scorer").split())
        ast = m.group("ast") or ""
        ast = " ".join(ast.split())
        goals.append({"time": t, "scorer": scorer, "assists": ast})
    return goals

def parse_match_goals(match_url: str, home: str, away: str) -> list[dict]:
    """
    Возвращает список голов по времени с привязкой к команде:
      [{"team":"home"/"away","sec":int,"min":int,"scorer":str,"assists":str}]
    """
    r = S.get(match_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n")

    home_block = extract_goal_block(text, home) or ""
    away_block = extract_goal_block(text, away) or ""
    home_goals = parse_team_goals_from_block(home_block)
    away_goals = parse_team_goals_from_block(away_block)

    # в блоках могли попасть чужие строки при неидеальной разметке — отфильтруем по наличию времени
    def t2sec(ts: str) -> int:
        m, s = ts.split(":")
        return int(m) * 60 + int(s)

    events = []
    for g in home_goals:
        sec = t2sec(g["time"])
        events.append({"team": "home", "sec": sec, "min": int(g["time"].split(":")[0]),
                       "scorer": g["scorer"], "assists": g["assists"]})
    for g in away_goals:
        sec = t2sec(g["time"])
        events.append({"team": "away", "sec": sec, "min": int(g["time"].split(":")[0]),
                       "scorer": g["scorer"], "assists": g["assists"]})

    # Хронология
    events.sort(key=lambda x: x["sec"])
    return events

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
                assists = f" ({ev['assists']})" if ev["assists"] else ""
                lines.append(f"{h_go}:{a_go} – {ev['min']} {ev['scorer']}{assists}")
        except Exception as e:
            log(f"[WARN] goals parse failed for {home} vs {away}: {e}")
            lines.append("— подробная запись голов недоступна")

        block = (
            f"{emj(home)} «{home}»: {hs}\n"
            f"{emj(away)} «{away}»: {as_}\n\n" +
            "\n".join(lines)
        )

        chunks.append(block.rstrip() + ("\n\n" if i < len(matches) else ""))

    return "".join(chunks).strip()

# ------ отправка в Telegram (с делением на части, чтобы не упереться в 4096 симв.) ------
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
        # маленькая пауза, чтобы не троттлило
        time.sleep(0.6)

# ------ main ------
if __name__ == "__main__":
    try:
        # Логика «после последнего матча игрового дня»: запускаем утром по Лондону и берём "сегодняшнюю" дату.
        # Календарь sports.ru содержит в течение дня и сыгранные, и будущие матчи той же даты.
        # Мы включаем ТОЛЬКО те строки, где счёт уже задан (не "- : -") — это и есть завершённые игры.
        target = get_london_today()
        post = build_post(target)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
