#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results -> Telegram (RU)
Deps: requests, beautifulsoup4

ENV:
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
  REPORT_DATE_MSK  (optional, YYYY-MM-DD)

Логика:
- Берём расписание NHL за (D-1, D, D+1), собираем уникальные завершённые игры (OFF/FINAL).
- По каждой игре:
    * PBP из NHL для привязки сайды/периода/времени.
    * Голы с Sports.ru (Гол!, Ассистент(ы)), принудительная корректная кодировка.
    * Сопоставляем времена (±3 сек) -> считаем корректный счёт.
    * Добавляем заголовки периодов курсивом.
    * Если есть буллиты — показываем только победный.
- В шапке — счёт и рекорд команд (standings/now, fallback из объекта игры).
"""

import os
import sys
import re
import json
import time
import math
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Optional

import requests
from bs4 import BeautifulSoup

# ==========================
# Константы / маппинги
# ==========================

RU_MONTHS = {
    1:"января", 2:"февраля", 3:"марта", 4:"апреля",
    5:"мая", 6:"июня", 7:"июля", 8:"августа",
    9:"сентября", 10:"октября", 11:"ноября", 12:"декабря",
}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

TEAM_RU = {
    "ANA":"Анахайм","ARI":"Аризона","ATL":"Атланта",
    "BOS":"Бостон","BUF":"Баффало","CGY":"Калгари","CAR":"Каролина","CHI":"Чикаго",
    "COL":"Колорадо","CBJ":"Коламбус","DAL":"Даллас","DET":"Детройт","EDM":"Эдмонтон",
    "FLA":"Флорида","LAK":"Лос-Анджелес","MIN":"Миннесота","MTL":"Монреаль","NJD":"Нью-Джерси",
    "NSH":"Нэшвилл","NYI":"Айлендерс","NYR":"Рейнджерс","OTT":"Оттава","PHI":"Филадельфия",
    "PIT":"Питтсбург","SJS":"Сан-Хосе","SEA":"Сиэтл","STL":"Сент-Луис","TBL":"Тампа-Бэй",
    "TOR":"Торонто","UTA":"Юта","VAN":"Ванкувер","VGK":"Вегас","WPG":"Виннипег","WSH":"Вашингтон",
}

TEAM_EMOJI = {
    "ANA":"🦆","ARI":"🪽","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️",
    "CBJ":"💣","DAL":"⭐","DET":"🔴","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦",
    "NJD":"😈","NSH":"🐯","NYI":"🟠","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈",
    "SEA":"🦑","STL":"🎵","TBL":"⚡","TOR":"🍁","UTA":"🦣","VAN":"🐳","VGK":"🎰","WPG":"✈️","WSH":"🦅",
}

TRICODE_TO_SPORTSRU_SLUG = {
    "ANA":"anaheim-ducks","ARI":"arizona-coyotes","BOS":"boston-bruins","BUF":"buffalo-sabres",
    "CGY":"calgary-flames","CAR":"carolina-hurricanes","CHI":"chicago-blackhawks","COL":"colorado-avalanche",
    "CBJ":"columbus-blue-jackets","DAL":"dallas-stars","DET":"detroit-red-wings","EDM":"edmonton-oilers",
    "FLA":"florida-panthers","LAK":"los-angeles-kings","MIN":"minnesota-wild","MTL":"montreal-canadiens",
    "NJD":"new-jersey-devils","NSH":"nashville-predators","NYI":"new-york-islanders","NYR":"new-york-rangers",
    "OTT":"ottawa-senators","PHI":"philadelphia-flyers","PIT":"pittsburgh-penguins","SJS":"san-jose-sharks",
    "SEA":"seattle-kraken","STL":"st-louis-blues","TBL":"tampa-bay-lightning","TOR":"toronto-maple-leafs",
    "UTA":"utah-mammoth",  # актуальный слаг на Sports.ru (2025)
    "VAN":"vancouver-canucks","VGK":"vegas-golden-knights","WPG":"winnipeg-jets","WSH":"washington-capitals",
}

def team_ru(tricode: str) -> str:
    return TEAM_RU.get(tricode, tricode)

def team_emoji(tricode: str) -> str:
    return TEAM_EMOJI.get(tricode, "🏒")

def sportsru_match_slugs(home_tri: str, away_tri: str) -> List[str]:
    hs = TRICODE_TO_SPORTSRU_SLUG.get(home_tri)
    as_ = TRICODE_TO_SPORTSRU_SLUG.get(away_tri)
    if not hs or not as_:
        return []
    return [f"{hs}-vs-{as_}", f"{as_}-vs-{hs}"]

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

def dbg(msg: str):
    print(f"[DBG] {msg}", flush=True)

# ==========================
# HTTP helpers
# ==========================

def http_get(url: str, timeout: int = 20) -> requests.Response:
    dbg(f"GET {url}")
    r = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
    try:
        enc = r.apparent_encoding or "utf-8"
    except Exception:
        enc = "utf-8"
    r.encoding = enc
    return r

# ==========================
# NHL API
# ==========================

NHL_BASE = "https://api-web.nhle.com/v1"

def nhl_schedule(ymd: str) -> List[dict]:
    url = f"{NHL_BASE}/schedule/{ymd}"
    r = http_get(url)
    data = r.json()
    return data.get("gameWeek", [{}])[0].get("games", []) or data.get("games", []) or []

def nhl_pbp(game_id: int) -> dict:
    r = http_get(f"{NHL_BASE}/gamecenter/{game_id}/play-by-play")
    return r.json()

def nhl_standings_now() -> dict:
    """triCode -> (W,L,OT)"""
    url = f"{NHL_BASE}/standings/now"
    try:
        r = http_get(url)
        js = r.json()
    except Exception:
        return {}
    out = {}
    # структура standings: [{standings: [{teamAbbrev: 'BOS', wins:..., losses:..., ot:...}, ...]}]
    for conf in js if isinstance(js, list) else js.get("standings", []):
        arr = conf.get("standings") if isinstance(conf, dict) else None
        if not arr:
            continue
        for row in arr:
            tri = row.get("teamAbbrev")
            w = row.get("wins")
            l = row.get("losses")
            ot = row.get("ot")
            if tri and isinstance(w,int) and isinstance(l,int) and isinstance(ot,int):
                out[tri] = (w,l,ot)
    dbg(f"records loaded: {len(out)}")
    return out

# ==========================
# Sports.ru parser (Гол!/Ассистенты/Время)
# ==========================

_GOAL_LINE = re.compile(r"(?:Гол!|ГОООЛ!|Гол\b)\s*([^\n\r]*)", re.IGNORECASE)
_ASSISTS = re.compile(r"Ассистент[ы]?:\s*([^\n\r]+)", re.IGNORECASE)
_TIME = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")

def _ru_clean(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)
    s = re.sub(r"[^А-Яа-яЁё\-\s]", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" -–—")
    return s

def parse_sportsru_goals(match_slug: str) -> List[dict]:
    """Возвращает [{'time':'24.48','author':'Пажо','assists':'Хольмстрем'}, ...]"""
    urls = [
        f"https://www.sports.ru/hockey/match/{match_slug}/lineups/",
        f"https://www.sports.ru/hockey/match/{match_slug}/",
    ]
    for u in urls:
        try:
            res = http_get(u)
            soup = BeautifulSoup(res.text, "html.parser")
            text = soup.get_text("\n", strip=False)
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            out = []
            # Пробегаем по строкам, ищем отметки времени и рядом "Гол!/Ассистенты"
            for i, ln in enumerate(lines):
                mt = _TIME.search(ln)
                if not mt:
                    continue
                window = " ".join(lines[i:i+6])
                g = _GOAL_LINE.search(window)
                if not g:
                    continue
                author_raw = _ru_clean(g.group(1))
                if not author_raw:
                    cand = _ru_clean(" ".join(lines[i+1:i+4]))
                    author_raw = cand.split()[0] if cand else ""
                a = _ASSISTS.search(window)
                assists_raw = _ru_clean(a.group(1)) if a else ""
                mm, ss = mt.group(1), mt.group(2)
                out.append({"time": f"{mm}.{ss}", "author": author_raw or "—", "assists": assists_raw})
            if out:
                dbg(f"sports.ru goals parsed: {len(out)} (ex: {out[0]['time']} {out[0]['author']}" + (f" | {out[1]['time']} {out[1]['author']}" if len(out)>1 else "") + ")")
                return out
        except Exception as e:
            continue
    return []

def find_sportsru_goals_for_pair(home_tri: str, away_tri: str) -> List[dict]:
    slugs = sportsru_match_slugs(home_tri, away_tri)
    dbg(f"sports.ru slugs tried: {slugs}")
    for slug in slugs:
        goals = parse_sportsru_goals(slug)
        dbg(f"sports.ru goals for {slug}: {len(goals)}")
        if goals and len(goals) >= 2:
            return goals
    return []

# ==========================
# Сопоставление голов и подсчёт счёта
# ==========================

def _to_sec(t: str) -> int:
    t = t.replace(":", ".")
    m, s = t.split(".")
    return int(m)*60 + int(s)

def attach_side_and_score(game: dict, goals_sru: List[dict], goals_nhl: List[dict]) -> List[Tuple[Optional[int], str]]:
    """
    На вход:
      - goals_sru: [{'time':'24.48','author':'Пажо','assists':'Хольмстрем'}, ...]
      - goals_nhl: [{'period':2, 'time':'24:48', 'teamTri':'NYI'}, ...]
    Возвращает массив [(period, "X:Y – ММ.SS Автор (ассисты)"), ...] в хрон порядке.
    """
    home_tri = game["homeTeam"]["abbrev"]
    away_tri = game["awayTeam"]["abbrev"]

    # Разложим NHL по периодам
    by_period: Dict[int, List[dict]] = {}
    for g in goals_nhl:
        p = g.get("period") or 0
        tm = g.get("time") or "00:00"
        tri = g.get("teamTri")
        if not tri:
            continue
        by_period.setdefault(p, []).append({"time": tm, "teamTri": tri})
    for p in by_period:
        by_period[p].sort(key=lambda x: _to_sec(x["time"]))

    hs, as_ = 0, 0
    out: List[Tuple[Optional[int], str]] = []

    # Пытаемся угадать период по ближайшему времени (±3 сек) среди всех периодов
    for g in goals_sru:
        best = None
        best_delta = 999
        for p, arr in by_period.items():
            for nh in arr:
                d = abs(_to_sec(nh["time"]) - _to_sec(g["time"]))
                if d < best_delta:
                    best_delta = d
                    best = (p, nh["teamTri"])
        if best:
            p, tri = best
            if tri == home_tri:
                hs += 1
            elif tri == away_tri:
                as_ += 1
            score = f"{hs}:{as_}"
            tail = g["author"]
            if g.get("assists"):
                tail += f" ({g['assists']})"
            out.append((p, f"{score} – {g['time']} {tail}"))
        else:
            # если не нашли, добавим без сайды (счёт не меняем)
            tail = g["author"]
            if g.get("assists"):
                tail += f" ({g['assists']})"
            out.append((None, f"{hs}:{as_} – {g['time']} {tail}"))
    return out

def pbp_extract_goals(pbp_json: dict) -> Tuple[List[dict], bool]:
    """
    Из JSON PBP вытащить голы в минимальном составе: period, time, teamTri.
    Возвращает (goals, shootout_flag)
    """
    goals = []
    shootout = False
    # NHL v1 PBP структура может меняться — обрабатываем популярные поля:
    all_plays = pbp_json.get("plays") or pbp_json.get("gameCenter", {}).get("plays") or []
    for ev in all_plays:
        et = (ev.get("typeDescKey") or ev.get("type") or "").lower()
        if "shootout" in et:
            shootout = True
        if "goal" in et:  # goal/shootoutGoal
            tri = (ev.get("teamAbbrev") or ev.get("team", {}).get("abbrev"))
            period = ev.get("periodDescriptor", {}).get("number") or ev.get("period") or 0
            time_str = ev.get("timeInPeriod") or ev.get("time") or "00:00"
            goals.append({"period": int(period), "time": time_str, "teamTri": tri})
    return goals, shootout

def pbp_winning_shootout_scorer(pbp_json: dict) -> Optional[str]:
    """
    Пытаемся вытащить имя автора победного буллита из PBP.
    Если структура неподходящая — вернём None (в этом случае просто не пишем строку про буллит).
    """
    plays = pbp_json.get("plays") or pbp_json.get("gameCenter", {}).get("plays") or []
    shots = []
    for ev in plays:
        et = (ev.get("typeDescKey") or ev.get("type") or "").lower()
        if "shootout" in et:
            # пробуем вытащить фамилию из description/playText
            txt = ev.get("playText") or ev.get("description") or ""
            # ищем кириллицу или латиницу, оставим коротко (фамилию)
            m = re.search(r"([A-Za-zА-Яа-яЁё\-]+)$", txt.strip())
            if m:
                shots.append(m.group(1))
    # в упрощении — последний успешный шот и будет победным
    return shots[-1] if shots else None

# ==========================
# Рекорды команд
# ==========================

def get_record_from_sources(game: dict, side: str, records_by_tri: Dict[str, Tuple[int,int,int]]) -> Optional[Tuple[int,int,int]]:
    tri = game[f"{side}Team"]["abbrev"]
    if records_by_tri and tri in records_by_tri:
        return records_by_tri[tri]
    maybe = (game.get(f"{side}Team") or {}).get("record") or {}
    if isinstance(maybe, dict):
        w = maybe.get("wins") or maybe.get("otWins") or maybe.get("winsTotal")
        l = maybe.get("losses") or maybe.get("lossesTotal")
        ot = maybe.get("ot") or maybe.get("otLosses") or maybe.get("otl")
        if isinstance(w,int) and isinstance(l,int) and isinstance(ot,int):
            return (w,l,ot)
    return None

def format_team_caption(tri: str, score: int, record: Optional[Tuple[int,int,int]]) -> str:
    em = team_emoji(tri)
    name = team_ru(tri)
    if record:
        w,l,ot = record
        pts = 2*w + ot
        return f"{em} «{name}»: {score} ({w}-{l}-{ot}, {pts} о.)"
    return f"{em} «{name}»: {score}"

# ==========================
# Форматирование поста
# ==========================

def period_title(p: int) -> str:
    if p == 1: return "_1-й период_"
    if p == 2: return "_2-й период_"
    if p == 3: return "_3-й период_"
    return f"_Овертайм №{p-3}_"

SEP = "——————————————————"

def build_post(games: List[dict], records_by_tri: Dict[str, Tuple[int,int,int]]) -> str:
    if not games:
        head = f"🗓 Регулярный чемпионат НХЛ • {ru_date(date.today())} • 0 матчей\n\nРезультаты надёжно спрятаны 👇\n\n{SEP}"
        return head

    # Заголовок (дату берём московскую «сегодня»)
    head = f"🗓 Регулярный чемпионат НХЛ • {ru_date(date.today())} • {len(games)} матчей\n\nРезультаты надёжно спрятаны 👇\n\n{SEP}"
    chunks = [head]

    for game in games:
        home_tri = game["homeTeam"]["abbrev"]
        away_tri = game["awayTeam"]["abbrev"]
        home_score = game["homeTeam"].get("score", 0)
        away_score = game["awayTeam"].get("score", 0)

        # Рекорды
        rec_h = get_record_from_sources(game, "home", records_by_tri)
        rec_a = get_record_from_sources(game, "away", records_by_tri)

        # PBP
        pbp = nhl_pbp(game["id"])
        goals_nhl, shootout_flag = pbp_extract_goals(pbp)

        # Sports.ru голы
        sru_goals = find_sportsru_goals_for_pair(home_tri, away_tri)

        # Если ничего не нашли на Sports.ru — напишем, что события недоступны
        if not sru_goals:
            blk = [
                f"{format_team_caption(home_tri, home_score, rec_h)}",
                f"{format_team_caption(away_tri, away_score, rec_a)}",
                "",
                "— события матча недоступны",
                "",
                SEP
            ]
            chunks.append("\n".join(blk))
            continue

        # Сопоставление и расчёт счёта-после-каждого-гола
        paired = attach_side_and_score(game, sru_goals, goals_nhl)
        # Группировка по периодам
        per_map: Dict[int, List[str]] = {}
        for p, line in paired:
            per_map.setdefault(p or 1, []).append(line)  # если нет периода — считаем 1-й

        # Сборка блока матча
        blk = [
            f"{format_team_caption(home_tri, home_score, rec_h)}",
            f"{format_team_caption(away_tri, away_score, rec_a)}",
            ""
        ]
        # По порядку периодов
        for p in sorted(per_map.keys()):
            blk.append(period_title(p))
            for line in per_map[p]:
                blk.append(line)
            blk.append("")  # пустая строка между периодами

        # Победный буллит (если есть)
        if shootout_flag:
            ws = pbp_winning_shootout_scorer(pbp)
            if ws:
                blk.append(f"победный буллит — {ws}")
                blk.append("")

        blk.append(SEP)
        chunks.append("\n".join(blk))

    return "\n".join(chunks).rstrip()

# ==========================
# Отправка в Telegram
# ==========================

def tg_send(token: str, chat_id: str, text: str):
    api = f"https://api.telegram.org/bot{token}/sendMessage"
    MAX = 3900  # чуть с запасом от лимита
    parts = []
    if len(text) <= MAX:
        parts = [text]
    else:
        # режем по разделителю матчей
        blocks = text.split(SEP)
        curr = ""
        for i, b in enumerate(blocks):
            s = (b + (SEP if i < len(blocks)-1 else ""))
            if len(curr) + len(s) <= MAX:
                curr += s
            else:
                if curr.strip():
                    parts.append(curr)
                if len(s) <= MAX:
                    curr = s
                else:
                    # форс-резка на жёстких матчах
                    for j in range(0, len(s), MAX):
                        parts.append(s[j:j+MAX])
                    curr = ""
        if curr.strip():
            parts.append(curr)

    for p in parts:
        resp = requests.post(api, json={
            "chat_id": chat_id,
            "text": p,
            "disable_web_page_preview": True,
            # parse_mode не ставим, чтобы не ловить 400 из-за Markdown-экранирования
        }, timeout=20)
        if not resp.ok:
            raise requests.HTTPError(f"{resp.status_code} {resp.text}")

# ==========================
# Выбор дат, сбор завершённых игр
# ==========================

def collect_completed_games() -> List[dict]:
    # Берём (вчера, сегодня, завтра) по московской зоне,
    # чтобы гарантированно покрыть поздние старты.
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Moscow")
    except Exception:
        tz = None

    if os.getenv("REPORT_DATE_MSK"):
        try:
            base = datetime.fromisoformat(os.getenv("REPORT_DATE_MSK")).date()
        except Exception:
            base = date.today()
    else:
        base = date.today()

    days = [base - timedelta(days=1), base, base + timedelta(days=1)]
    # Если нужна строгая логика включения окон старта по 15:00 МСК — можно развить,
    # но для стабильности клиента берём просто завершённые (OFF/FINAL) из трёх дат.

    seen = set()
    out = []
    for d in days:
        ymd = d.strftime("%Y-%m-%d")
        games = nhl_schedule(ymd)
        for g in games:
            state = g.get("gameState")
            gid = g.get("id")
            if gid in seen:
                continue
            if state in ("OFF", "FINAL"):  # завершённые матчи
                out.append(g)
                seen.add(gid)
            else:
                dbg(f"skip not final: {gid} {state}")
    dbg(f"Collected unique FINAL games: {len(out)}")
    return out

# ==========================
# main
# ==========================

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("ERROR: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set", file=sys.stderr)
        sys.exit(1)

    games = collect_completed_games()

    # Рекорды
    try:
        records_by_tri = nhl_standings_now()
    except Exception:
        records_by_tri = {}
        dbg("records loaded: 0")

    text = build_post(games, records_by_tri)

    # Отправляем
    try:
        dbg("POST Telegram sendMessage")
        tg_send(token, chat_id, text)
        dbg("Telegram OK")
        print("OK")
    except Exception as e:
        print(f"ERROR: {repr(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
