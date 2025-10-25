#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram
Голы/ассисты с api-web.nhle.com (официальный плей-бай-плей),
русские фамилии — через профили игроков на sports.ru.
"""

import os
import sys
import re
import time
import json
from html import escape
from datetime import date, datetime
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ===================== ПАРАМЕТРЫ =====================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

API_NHL = "https://api-web.nhle.com/v1"
SPORTS_RU_SEARCH = "https://www.sports.ru/search/?q="

# RU-месяцы для заголовка
RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

# Карта команд: аббревиатура → (русское короткое, эмодзи)
TEAM_RU = {
    "ANA": ("Анахайм", "🦆"), "ARI": ("Аризона", "🤠"), "BOS": ("Бостон", "🐻"), "BUF": ("Баффало", "🦬"),
    "CGY": ("Калгари", "🔥"), "CAR": ("Каролина", "🌪"), "COL": ("Колорадо", "⛰"), "CBJ": ("Коламбус", "💣"),
    "DAL": ("Даллас", "⭐"), "DET": ("Детройт", "🔴"), "EDM": ("Эдмонтон", "🛢"), "FLA": ("Флорида", "🐆"),
    "LAK": ("Лос-Анджелес", "👑"), "MIN": ("Миннесота", "🌲"), "MTL": ("Монреаль", "🇨🇦"), "NSH": ("Нэшвилл", "🐯"),
    "NJD": ("Нью-Джерси", "😈"), "NYI": ("Айлендерс", "🟠"), "NYR": ("Рейнджерс", "🗽"), "OTT": ("Оттава", "🛡"),
    "PHI": ("Филадельфия", "🛩"), "PIT": ("Питтсбург", "🐧"), "SJS": ("Сан-Хосе", "🦈"), "SEA": ("Сиэтл", "🦑"),
    "STL": ("Сент-Луис", "🎵"), "TBL": ("Тампа-Бэй", "⚡"), "TOR": ("Торонто", "🍁"), "VAN": ("Ванкувер", "🐳"),
    "VGK": ("Вегас", "🎰"), "WSH": ("Вашингтон", "🦅"), "WPG": ("Виннипег", "✈️"), "UTA": ("Юта", "🦣"),
    "CHI": ("Чикаго", "🦅")
}

def get_today_london() -> date:
    return datetime.now(ZoneInfo("Europe/London")).date()

def log(*args):
    print(*args, file=sys.stderr)

# HTTP с ретраями
def make_session():
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-DailyResultsBot/2.0 (+api-web.nhle.com; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

# ===================== ШАГ 1. РАСПИСАНИЕ/СПИСОК МАТЧЕЙ =====================

def fetch_schedule(day: date) -> list[dict]:
    """
    Возвращает список матчей за день:
    { gameId, gameState, homeAbbrev, awayAbbrev, homeScore, awayScore }
    Берём только завершённые (gameState == 'OFF').
    """
    url = f"{API_NHL}/schedule/{day.isoformat()}"
    r = S.get(url, timeout=25)
    r.raise_for_status()
    data = r.json()
    games_out = []
    for bucket in data.get("gameWeek", []):
        if bucket.get("date") != day.isoformat():
            continue
        for g in bucket.get("games", []):
            if g.get("gameState") != "OFF":
                continue
            game_id = g.get("id") or g.get("gameId")
            aw = g.get("awayTeam", {})
            hm = g.get("homeTeam", {})
            games_out.append({
                "gameId": int(game_id),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": hm.get("score", 0),
                "awayScore": aw.get("score", 0),
                "periodType": g.get("periodDescriptor", {}).get("periodType") or "",
                "periodNumber": g.get("periodDescriptor", {}).get("number") or 3
            })
    return games_out

# ===================== ШАГ 2. PLAY-BY-PLAY ПО МАТЧУ =====================

def fetch_box_map(game_id: int) -> dict[int, dict]:
    """
    Карта playerId -> {firstName, lastName, sweaterNumber}
    из boxscore, чтобы гарантированно получить имена.
    """
    url = f"{API_NHL}/gamecenter/{game_id}/boxscore"
    r = S.get(url, timeout=25)
    r.raise_for_status()
    data = r.json()
    out = {}

    def eat(team_block: dict):
        for group in ("forwards", "defense", "goalies"):
            for p in team_block.get(group, []):
                pid = p.get("playerId")
                if not pid: 
                    continue
                out[int(pid)] = {
                    "firstName": p.get("firstName", {}).get("default") or p.get("firstName") or "",
                    "lastName": p.get("lastName", {}).get("default") or p.get("lastName") or "",
                    "num": p.get("sweaterNumber")
                }

    player_stats = data.get("playerByGameStats", {})
    eat(player_stats.get("homeTeam", {}))
    eat(player_stats.get("awayTeam", {}))
    return out

def fetch_goals(game_id: int) -> list[dict]:
    """
    Возвращает события-голы в хронологии:
    [{minute:int, home:int, away:int, scorerId:int, a1:int|None, a2:int|None, periodType:str}]
    """
    url = f"{API_NHL}/gamecenter/{game_id}/play-by-play"
    r = S.get(url, timeout=25)
    r.raise_for_status()
    data = r.json()
    plays = data.get("plays", []) or []
    goals = []

    for ev in plays:
        if ev.get("typeDescKey") != "goal":
            continue
        det = ev.get("details", {}) or {}
        time_in = ev.get("timeInPeriod") or det.get("timeInPeriod") or "0:00"
        # время в формате "MM:SS" → берём минуту
        try:
            minute = int(str(time_in).split(":")[0])
        except Exception:
            minute = 0

        # счёт после взятия ворот
        hs = int(det.get("homeScore", 0))
        as_ = int(det.get("awayScore", 0))

        # участники: scorer + до 2 ассистов (в api-web обычно лежат как *_PlayerId)
        sid = det.get("scoringPlayerId")
        a1 = det.get("assist1PlayerId") or det.get("secondaryAssistPlayerId")
        a2 = det.get("assist2PlayerId") or det.get("tertiaryAssistPlayerId")

        # fallback: иногда участники лежат в playersInvolved
        if not sid and ev.get("playersInvolved"):
            for p in ev["playersInvolved"]:
                if p.get("playerType") in ("Scorer", "scorer"):
                    sid = p.get("playerId")
                elif p.get("playerType") in ("Assist", "assist"):
                    if not a1:
                        a1 = p.get("playerId")
                    elif not a2:
                        a2 = p.get("playerId")

        goals.append({
            "minute": minute,
            "home": hs, "away": as_,
            "scorerId": int(sid) if sid else None,
            "a1": int(a1) if a1 else None,
            "a2": int(a2) if a2 else None,
            "periodType": ev.get("periodDescriptor", {}).get("periodType") or ""
        })

    # на всякий — сортируем по (период, минута) если нужно
    goals.sort(key=lambda x: ({"REG":1,"OT":2,"SO":3}.get(x["periodType"], 1), x["minute"]))
    return goals

# ===================== ШАГ 3. РУССКИЕ ИМЕНА ЧЕРЕЗ SPORTS.RU =====================

_name_cache: dict[str, str] = {}  # "Connor McDavid" -> "Макдэвид"

def _ru_surname_from_profile(url: str) -> str | None:
    try:
        r = S.get(url, timeout=25)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        # у профилей игроков заголовок h1 — «Имя Фамилия»
        h = soup.find(["h1","h2"])
        if not h:
            return None
        ru_full = " ".join(h.get_text(" ", strip=True).split())
        # берём последнюю «слово-группу» как фамилию
        parts = [p for p in re.split(r"\s+", ru_full) if p]
        if parts:
            return parts[-1]
    except Exception as e:
        log("[sports.ru] profile parse fail:", e)
    return None

def ru_surname(eng_first: str, eng_last: str) -> str:
    """
    Ищем профиль игрока на sports.ru по латинскому имени, забираем русскую фамилию.
    Возвращаем только фамилию (как в образце). Если не нашли — отдаём латиницей.
    Кэшируем в памяти за один прогон.
    """
    key = f"{eng_first.strip()} {eng_last.strip()}".strip()
    if key in _name_cache:
        return _name_cache[key]

    # 1) прямой запрос к поиску sports.ru
    try:
        q = quote_plus(key)
        srch = S.get(SPORTS_RU_SEARCH + q, timeout=25)
        if srch.status_code == 200:
            soup = BeautifulSoup(srch.text, "html.parser")
            # берём первую ссылку на профиль игрока
            # чаще всего href вида /hockey/person/<slug>/
            link = soup.select_one('a[href*="/hockey/person/"]')
            if link and link.get("href"):
                profile = link["href"]
                if profile.startswith("/"):
                    profile = "https://www.sports.ru" + profile
                ru = _ru_surname_from_profile(profile)
                if ru:
                    _name_cache[key] = ru
                    return ru
    except Exception as e:
        log("[sports.ru] search fail:", key, e)

    # 2) запасной вариант — попытка на странице тега игрока (иногда редиректит)
    try:
        slug = "-".join((eng_first + " " + eng_last).lower().split())
        maybe = f"https://www.sports.ru/{slug}/"
        ru = _ru_surname_from_profile(maybe)
        if ru:
            _name_cache[key] = ru
            return ru
    except Exception:
        pass

    # 3) совсем уж fallback — оставить латиницей фамилию
    last = eng_last.strip() or key
    _name_cache[key] = last
    return last

# ===================== СБОРКА СООБЩЕНИЯ =====================

def team_ru_and_emoji(abbr: str) -> tuple[str, str]:
    abbr = (abbr or "").upper()
    if abbr in TEAM_RU:
        return TEAM_RU[abbr]
    return (abbr, "🏒")

def build_game_block(game: dict) -> str:
    """
    Собираем блок по матчу:
    <emoji> «Home»: X
    <emoji> «Away»: Y

    0:1 – 10 Фамилия (Фамилия, Фамилия)
    ...
    """
    game_id = game["gameId"]
    home_ab, away_ab = game["homeAbbrev"], game["awayAbbrev"]
    home_ru, emh = team_ru_and_emoji(home_ab)
    away_ru, ema = team_ru_and_emoji(away_ab)

    goals = fetch_goals(game_id)
    boxmap = fetch_box_map(game_id)  # id -> names

    # определить суффикс "(ОТ)" или "(Б)" по типу периода последнего гола
    suffix = ""
    if goals:
        last_pt = goals[-1].get("periodType", "")
        if last_pt == "OT":
            suffix = " (ОТ)"
        elif last_pt == "SO":
            suffix = " (Б)"

    lines = []
    for g in goals:
        # имена участников по boxmap
        def name(pid):
            if not pid or pid not in boxmap:
                return None
            f = boxmap[pid].get("firstName") or ""
            l = boxmap[pid].get("lastName") or ""
            return ru_surname(f, l)

        scorer = name(g["scorerId"])
        a1 = name(g["a1"])
        a2 = name(g["a2"])

        assists = []
        if a1: assists.append(a1)
        if a2: assists.append(a2)
        ast_txt = f" ({', '.join(assists)})" if assists else ""

        # формат: h:a – MIN Фамилия (ассисты)
        lines.append(f"{g['home']}:{g['away']} – {g['minute']} {scorer or ''}{ast_txt}")

    # финальный счёт (из расписания)
    head = (
        f"{emh} «{home_ru}»: {game['homeScore']}\n"
        f"{ema} «{away_ru}»: {game['awayScore']}{suffix}\n\n"
    )

    if not lines:
        lines = ["— подробная запись голов недоступна"]

    return head + "\n".join(lines)

def build_post(day: date) -> str:
    games = fetch_schedule(day)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(day)} • {len(games)} {ru_plural(len(games), ('матч', 'матча', 'матчей'))}\n\n"
    title += "Результаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    blocks = []
    for i, g in enumerate(games, 1):
        try:
            blocks.append(build_game_block(g))
        except Exception as e:
            log(f"[WARN] game {g.get('gameId')} failed:", e)
            # всё равно покажем шапку со счётом
            home_ru, emh = team_ru_and_emoji(g["homeAbbrev"])
            away_ru, ema = team_ru_and_emoji(g["awayAbbrev"])
            blocks.append(
                f"{emh} «{home_ru}»: {g['homeScore']}\n{ema} «{away_ru}»: {g['awayScore']}\n\n— события матча временно недоступны"
            )

        if i < len(games):
            blocks.append("")  # пустая строка-разделитель

    return title + "\n".join(blocks).strip()

# ===================== ОТПРАВКА В TELEGRAM =====================

def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    MAX = 3500  # запас до лимита 4096
    chunks = []
    t = text
    while t:
        if len(t) <= MAX:
            chunks.append(t)
            break
        cut = t.rfind("\n\n", 0, MAX)
        if cut == -1:
            cut = MAX
        chunks.append(t[:cut])
        t = t[cut:].lstrip()

    for part in chunks:
        resp = S.post(url, json={
            "chat_id": CHAT_ID,
            "text": part,
            "disable_web_page_preview": True,
        }, timeout=25)
        if resp.status_code != 200:
            raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
        time.sleep(0.5)

# ===================== MAIN =====================

if __name__ == "__main__":
    try:
        day = get_today_london()
        msg = build_post(day)
        tg_send(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
