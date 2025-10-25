#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram
Голы/ассисты — из api-web.nhle.com (официальный play-by-play),
Формат имён — «инициал + русская фамилия» с подстановкой через sports.ru.
"""

import os
import sys
import re
import time
from datetime import date, datetime, timedelta
from html import escape
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

# ===================== ПАРАМЕТРЫ =====================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

API_NHL = "https://api-web.nhle.com/v1"
SPORTS_RU_SEARCH = "https://www.sports.ru/search/?q="

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

# аббревиатура → (русское короткое, эмодзи)
TEAM_RU = {
    "ANA": ("Анахайм", "🦆"), "ARI": ("Аризона", "🤠"), "BOS": ("Бостон", "🐻"), "BUF": ("Баффало", "🦬"),
    "CGY": ("Калгари", "🔥"), "CAR": ("Каролина", "🌪️"), "COL": ("Колорадо", "⛰️"), "CBJ": ("Коламбус", "💣"),
    "DAL": ("Даллас", "⭐"), "DET": ("Детройт", "🔴"), "EDM": ("Эдмонтон", "🛢️"), "FLA": ("Флорида", "🐆"),
    "LAK": ("Лос-Анджелес", "👑"), "MIN": ("Миннесота", "🌲"), "MTL": ("Монреаль", "🇨🇦"), "NSH": ("Нэшвилл", "🐯"),
    "NJD": ("Нью-Джерси", "😈"), "NYI": ("Айлендерс", "🟠"), "NYR": ("Рейнджерс", "🗽"), "OTT": ("Оттава", "🛡"),
    "PHI": ("Филадельфия", "🛩"), "PIT": ("Питтсбург", "🐧"), "SJS": ("Сан-Хосе", "🦈"), "SEA": ("Сиэтл", "🦑"),
    "STL": ("Сент-Луис", "🎵"), "TBL": ("Тампа-Бэй", "⚡"), "TOR": ("Торонто", "🍁"), "VAN": ("Ванкувер", "🐳"),
    "VGK": ("Вегас", "🎰"), "WSH": ("Вашингтон", "🦅"), "WPG": ("Виннипег", "✈️"), "UTA": ("Юта", "🦣"),
    "CHI": ("Чикаго", "🦬" if False else "🦅"),  # попадались разные эмодзи — оставим орла
}

def log(*a): print(*a, file=sys.stderr)

# HTTP с ретраями
def make_session():
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"], raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-DailyResultsBot/2.1 (+api-web.nhle.com; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

# ===================== ДАТА ИГРОВОГО ДНЯ =====================

def pick_report_date() -> date:
    """
    Отчёт формируем по «североамериканскому» дню:
    если в Нью-Йорке раньше 07:00 — берём вчера; иначе — сегодня (ET).
    Если по выбранной дате игр 0, делаем запасной шаг на -1 день.
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    target = (now_et.date() - timedelta(days=1)) if now_et.hour < 7 else now_et.date()
    return target

# ===================== РАСПИСАНИЕ / ФИНАЛЫ =====================

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

def fetch_games_for_date(day: date) -> list[dict]:
    """
    Возвращает список завершённых матчей на день:
    [{gameId, homeAbbrev, awayAbbrev, homeScore, awayScore, periodType}]
    Пробуем несколько эндпоинтов: schedule, score, scoreboard.
    """
    out = []

    # 1) /v1/schedule/{date}
    j = _get_json(f"{API_NHL}/schedule/{day.isoformat()}")
    for bucket in j.get("gameWeek", []):
        if bucket.get("date") != day.isoformat():
            continue
        for g in bucket.get("games", []):
            if str(g.get("gameState", "")).upper() not in {"OFF", "FINAL"}:
                continue
            game_id = g.get("id") or g.get("gameId")
            hm = g.get("homeTeam", {}) or {}
            aw = g.get("awayTeam", {}) or {}
            out.append({
                "gameId": int(game_id),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": hm.get("score", 0),
                "awayScore": aw.get("score", 0),
                "periodType": (g.get("periodDescriptor") or {}).get("periodType") or "",
            })

    # 2) /v1/score/{date}
    if not out:
        j = _get_json(f"{API_NHL}/score/{day.isoformat()}")
        for g in j.get("games", []):
            if str(g.get("gameState", "")).upper() not in {"OFF", "FINAL"}:
                continue
            game_id = g.get("id") or g.get("gameId")
            hm = g.get("homeTeam", {}) or {}
            aw = g.get("awayTeam", {}) or {}
            pd = g.get("periodDescriptor", {}) or {}
            out.append({
                "gameId": int(game_id),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": pd.get("periodType") or "",
            })

    # 3) /v1/scoreboard/{date}
    if not out:
        j = _get_json(f"{API_NHL}/scoreboard/{day.isoformat()}")
        for g in j.get("games", []):
            if str(g.get("gameState", "")).upper() not in {"OFF", "FINAL"}:
                continue
            game_id = g.get("id") or g.get("gameId")
            hm = g.get("homeTeam", {}) or {}
            aw = g.get("awayTeam", {}) or {}
            pd = g.get("periodDescriptor", {}) or {}
            out.append({
                "gameId": int(game_id),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": pd.get("periodType") or "",
            })

    return out

# ===================== BOX + PLAY-BY-PLAY =====================

def fetch_box_map(game_id: int) -> dict[int, dict]:
    """
    Карта playerId -> {firstName, lastName}
    """
    url = f"{API_NHL}/gamecenter/{game_id}/boxscore"
    r = S.get(url, timeout=25); r.raise_for_status()
    data = r.json()
    out: dict[int, dict] = {}

    def eat(team_block: dict):
        for group in ("forwards", "defense", "goalies"):
            for p in team_block.get(group, []) or []:
                pid = p.get("playerId")
                if not pid:
                    continue
                out[int(pid)] = {
                    "firstName": (p.get("firstName") or {}).get("default") or p.get("firstName") or "",
                    "lastName":  (p.get("lastName")  or {}).get("default") or p.get("lastName")  or "",
                }

    stats = data.get("playerByGameStats", {}) or {}
    eat(stats.get("homeTeam", {}) or {})
    eat(stats.get("awayTeam", {}) or {})
    return out

def fetch_goals(game_id: int) -> list[dict]:
    """
    Возвращает голы в хронологии:
    [{minute:int, home:int, away:int, scorerId:int|None, a1:int|None, a2:int|None, periodType:str}]
    """
    url = f"{API_NHL}/gamecenter/{game_id}/play-by-play"
    r = S.get(url, timeout=25); r.raise_for_status()
    data = r.json()
    plays = data.get("plays", []) or []
    goals = []

    for ev in plays:
        if ev.get("typeDescKey") != "goal":
            continue
        det = ev.get("details", {}) or {}
        t = str(ev.get("timeInPeriod") or det.get("timeInPeriod") or "0:00")
        try:
            minute = int(t.split(":")[0])
        except Exception:
            minute = 0

        hs = int(det.get("homeScore", 0))
        as_ = int(det.get("awayScore", 0))
        sid = det.get("scoringPlayerId")
        a1 = det.get("assist1PlayerId") or det.get("secondaryAssistPlayerId")
        a2 = det.get("assist2PlayerId") or det.get("tertiaryAssistPlayerId")

        # запасной вариант из playersInvolved
        if not sid and ev.get("playersInvolved"):
            for p in ev["playersInvolved"]:
                tpe = (p.get("playerType") or "").lower()
                if tpe == "scorer":
                    sid = p.get("playerId")
                elif tpe == "assist":
                    if not a1: a1 = p.get("playerId")
                    elif not a2: a2 = p.get("playerId")

        goals.append({
            "minute": minute,
            "home": hs, "away": as_,
            "scorerId": int(sid) if sid else None,
            "a1": int(a1) if a1 else None,
            "a2": int(a2) if a2 else None,
            "periodType": (ev.get("periodDescriptor") or {}).get("periodType") or "",
        })

    goals.sort(key=lambda x: ({"REG":1,"OT":2,"SO":3}.get(x["periodType"], 1), x["minute"]))
    return goals

# ===================== РУССКИЕ ИМЕНА (ИНИЦИАЛ + ФАМИЛИЯ) =====================

_name_cache: dict[str, str] = {}  # "Connor McDavid" -> "К. Макдэвид" или "C. McDavid"

def _ru_initial_surname_from_profile(url: str) -> str | None:
    """
    Открываем профиль игрока на sports.ru и берём заголовок h1 → "Имя Фамилия".
    Возвращаем "И. Фамилия".
    """
    try:
        r = S.get(url, timeout=25)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1", "h2"])
        if not h:
            return None
        full = " ".join(h.get_text(" ", strip=True).split())
        parts = [p for p in re.split(r"\s+", full) if p]
        if len(parts) >= 2:
            ini = parts[0][0] + "."
            last = parts[-1]
            return f"{ini} {last}"
    except Exception as e:
        log("[sports.ru] profile parse fail:", e)
    return None

def ru_initial_surname(eng_first: str, eng_last: str) -> str:
    """
    Возвращает «И. Фамилия», где фамилия — русская с sports.ru, если нашли профиль.
    Если нет — «F. Lastname» (латиница).
    """
    first = (eng_first or "").strip()
    last  = (eng_last  or "").strip()
    key = f"{first} {last}".strip()
    if not key:
        return ""

    if key in _name_cache:
        return _name_cache[key]

    # 1) поиск sports.ru
    try:
        q = quote_plus(key)
        sr = S.get(SPORTS_RU_SEARCH + q, timeout=25)
        if sr.status_code == 200:
            soup = BeautifulSoup(sr.text, "html.parser")
            link = soup.select_one('a[href*="/hockey/person/"]') or soup.select_one('a[href*="/hockey/player/"]')
            if link and link.get("href"):
                href = link["href"]
                if href.startswith("/"):
                    href = "https://www.sports.ru" + href
                ru = _ru_initial_surname_from_profile(href)
                if ru:
                    _name_cache[key] = ru
                    return ru
    except Exception as e:
        log("[sports.ru] search fail:", key, e)

    # 2) fallback: латиницей
    lat = (first[:1] + ". " if first else "") + (last or key)
    _name_cache[key] = lat
    return lat

# ===================== СБОРКА БЛОКА МАТЧА =====================

def team_ru_and_emoji(abbr: str) -> tuple[str, str]:
    abbr = (abbr or "").upper()
    if abbr in TEAM_RU:
        return TEAM_RU[abbr]
    return (abbr, "🏒")

def build_game_block(game: dict) -> str:
    game_id = game["gameId"]
    home_ab, away_ab = game["homeAbbrev"], game["awayAbbrev"]
    home_ru, emh = team_ru_and_emoji(home_ab)
    away_ru, ema = team_ru_and_emoji(away_ab)

    goals = fetch_goals(game_id)
    box = fetch_box_map(game_id)

    suffix = ""
    last_pt = (goals[-1].get("periodType") if goals else "") or game.get("periodType") or ""
    if last_pt == "OT":
        suffix = " (ОТ)"
    elif last_pt == "SO":
        suffix = " (Б)"

    lines = []
    for g in goals:
        def pname(pid):
            if not pid or pid not in box: return None
            f = box[pid].get("firstName") or ""
            l = box[pid].get("lastName") or ""
            return ru_initial_surname(f, l)

        scorer = pname(g["scorerId"])
        a1 = pname(g["a1"])
        a2 = pname(g["a2"])

        assists = []
        if a1: assists.append(a1)
        if a2: assists.append(a2)
        ast_txt = f" ({', '.join(assists)})" if assists else ""

        lines.append(f"{g['home']}:{g['away']} – {g['minute']} {scorer or ''}{ast_txt}")

    head = f"{emh} «{home_ru}»: {game['homeScore']}\n{ema} «{away_ru}»: {game['awayScore']}{suffix}\n\n"
    if not lines:
        lines = ["— подробная запись голов недоступна"]

    return head + "\n".join(lines)

# ===================== ФОРМИРОВАНИЕ ПОСТА =====================

def build_post(day: date) -> str:
    games = fetch_games_for_date(day)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(day)} • {len(games)} {ru_plural(len(games), ('матч', 'матча', 'матчей'))}\n\n"
    title += "Результаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    blocks = []
    for i, g in enumerate(games, 1):
        try:
            blocks.append(build_game_block(g))
        except Exception as e:
            log(f"[WARN] game {g.get('gameId')} failed:", e)
            home_ru, emh = team_ru_and_emoji(g["homeAbbrev"])
            away_ru, ema = team_ru_and_emoji(g["awayAbbrev"])
            blocks.append(
                f"{emh} «{home_ru}»: {g['homeScore']}\n{ema} «{away_ru}»: {g['awayScore']}\n\n— события матча временно недоступны"
            )
        if i < len(games):
            blocks.append("")

    return title + "\n".join(blocks).strip()

# ===================== ОТПРАВКА В TELEGRAM =====================

def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    MAX = 3500
    t = text
    parts = []
    while t:
        if len(t) <= MAX:
            parts.append(t); break
        cut = t.rfind("\n\n", 0, MAX)
        if cut == -1: cut = MAX
        parts.append(t[:cut]); t = t[cut:].lstrip()

    for part in parts:
        resp = S.post(url, json={
            "chat_id": CHAT_ID,
            "text": part,
            "disable_web_page_preview": True,
        }, timeout=25)
        if resp.status_code != 200:
            raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
        time.sleep(0.4)

# ===================== MAIN =====================

if __name__ == "__main__":
    try:
        target = pick_report_date()
        games = fetch_games_for_date(target)
        # запасной шаг, если вдруг ноль матчей (редкий случай)
        if not games:
            target = target - timedelta(days=1)
        msg = build_post(target)
        tg_send(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
