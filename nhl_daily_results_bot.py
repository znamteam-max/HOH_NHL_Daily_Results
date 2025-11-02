# nhl_daily_results_bot.py
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional
import requests

# =========================
# Config / Constants
# =========================

MSK = timezone(timedelta(hours=3))
UTC = timezone.utc

TEAM_RU = {
    "ANA": "Анахайм",
    "ARI": "Аризона",
    "BOS": "Бостон",
    "BUF": "Баффало",
    "CAR": "Каролина",
    "CBJ": "Коламбус",
    "CGY": "Калгари",
    "CHI": "Чикаго",
    "COL": "Колорадо",
    "DAL": "Даллас",
    "DET": "Детройт",
    "EDM": "Эдмонтон",
    "FLA": "Флорида",
    "LAK": "Лос-Анджелес",
    "MIN": "Миннесота",
    "MTL": "Монреаль",
    "NJD": "Нью-Джерси",
    "NSH": "Нэшвилл",
    "NYI": "Айлендерс",
    "NYR": "Рейнджерс",
    "OTT": "Оттава",
    "PHI": "Филадельфия",
    "PIT": "Питтсбург",
    "SEA": "Сиэтл",
    "SJS": "Сан-Хосе",
    "STL": "Сент-Луис",
    "TBL": "Тампа-Бэй",
    "TOR": "Торонто",
    "VAN": "Ванкувер",
    "VGK": "Вегас",
    "WPG": "Виннипег",
    "WSH": "Вашингтон",
}

TEAM_EMOJI = {
    "ANA": "🦆", "ARI": "🦂", "BOS": "🐻", "BUF": "🦬", "CAR": "🌪️",
    "CBJ": "💣", "CGY": "🔥", "CHI": "🦅", "COL": "⛰️", "DAL": "⭐️",
    "DET": "🚗", "EDM": "🛢️", "FLA": "🐆", "LAK": "👑", "MIN": "🌲",
    "MTL": "🇨🇦", "NJD": "😈", "NSH": "🐯", "NYI": "🏝️", "NYR": "🗽",
    "OTT": "🛡", "PHI": "🛩", "PIT": "🐧", "SEA": "🦑", "SJS": "🦈",
    "STL": "🎵", "TBL": "⚡", "TOR": "🍁", "VAN": "🐳", "VGK": "🎰",
    "WPG": "✈️", "WSH": "🦅",
}

DIV = "Регулярный чемпионат НХЛ"
SEPARATOR = "——————————————————"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_PARSE_MODE = "Markdown"

DEBUG = os.getenv("DEBUG", "1") != "0"
TIMEOUT = (8, 20)  # (connect, read)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (NHL Results Bot; +https://api-web.nhle.com)",
    "Accept": "application/json, text/plain, */*",
})


# =========================
# Utils / Logging
# =========================

def dbg(*args):
    if DEBUG:
        try:
            print("[DBG]", *args, file=sys.stderr)
        except Exception:
            # fallback safe
            pass


def http_json(url: str) -> Any:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    if "application/json" in r.headers.get("Content-Type", ""):
        return r.json()
    # иногда NHL отдаёт json с text/plain
    try:
        return json.loads(r.text)
    except Exception:
        return r.text


def ru_date(d: datetime) -> str:
    MONTHS = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля",
        5: "мая", 6: "июня", 7: "июля", 8: "августа",
        9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
    }
    return f"{d.day} {MONTHS[d.month]}"


def mmss_to_dot(s: str) -> str:
    # "1:27" -> "01.27"
    try:
        m, sec = s.split(":")
        return f"{int(m):02d}.{int(sec):02d}"
    except Exception:
        return s.replace(":", ".")


def points_from_record(rec: str) -> Optional[int]:
    # "7-4-2" -> 2*7 + 1*2 = 16
    try:
        parts = [int(x) for x in rec.strip().split("-")]
        if len(parts) == 3:
            w, l, otl = parts
            return 2 * w + otl
        if len(parts) == 2:
            w, l = parts
            return 2 * w
    except Exception:
        pass
    return None


# =========================
# Collect Finals (schedule)
# =========================

def _iter_schedule_blocks(data) -> List[Dict]:
    # Унифицируем schedule: бывает gameWeek[], бывает gameDay[]
    if isinstance(data, dict):
        if isinstance(data.get("gameWeek"), list):
            return data["gameWeek"]
        if isinstance(data.get("gameDay"), list):
            return data["gameDay"]
    return []


def collect_final_game_ids(msk_start: datetime, msk_end: datetime) -> List[int]:
    # берём даты окном [-1..+1] для обеих границ
    date_set = set()
    for d in range(-1, 2):
        date_set.add((msk_start + timedelta(days=d)).date().isoformat())
        date_set.add((msk_end + timedelta(days=d)).date().isoformat())
    dates = sorted(date_set)

    final_ids = set()
    for ds in dates:
        url = f"https://api-web.nhle.com/v1/schedule/{ds}"
        data = http_json(url)
        blocks = _iter_schedule_blocks(data)
        day_games = 0
        for blk in blocks:
            games = blk.get("games", [])
            day_games += len(games)
            for g in games:
                gid = g.get("id")
                if not gid:
                    continue
                state = (g.get("gameState")
                         or (g.get("gameStatus") or {}).get("state")
                         or "").upper()
                # считаем завершёнными обоих статусов
                if state in {"OFF", "FINAL"}:
                    final_ids.add(int(gid))
        dbg(f"SCHEDULE {ds}: blocks={len(blocks)} games={day_games}")

    ids_sorted = sorted(final_ids)
    dbg("Collected FINAL games:", len(ids_sorted))
    return ids_sorted


# =========================
# Standings (fallback for points/record)
# =========================

def _walk_standings(obj):
    # рекурсивный обход, ищем узлы с teamAbbrev/Tricode
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_standings(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _walk_standings(it)


def fetch_standings_now_map() -> Dict[str, Dict[str, Any]]:
    """
    Возвращает карту:
      triCode -> { "record": "W-L-OTL" | None, "points": int | None }
    """
    url = "https://api-web.nhle.com/v1/standings/now"
    data = http_json(url)
    out = {}
    count_nodes = 0
    for node in _walk_standings(data):
        if not isinstance(node, dict):
            continue
        tri = node.get("teamAbbrev") or node.get("teamAbbrevTricode") or node.get("teamTriCode")
        if not tri or not isinstance(tri, str):
            continue
        tri = tri.upper()
        count_nodes += 1

        # Пробуем разные поля
        w = node.get("wins") or node.get("otWins") or node.get("w")
        l = node.get("losses") or node.get("l")
        otl = node.get("otLosses") or node.get("otl") or node.get("overtimeLosses")
        pts = node.get("points") or node.get("pts")

        rec = None
        if isinstance(w, int) and isinstance(l, int):
            if isinstance(otl, int):
                rec = f"{w}-{l}-{otl}"
            else:
                rec = f"{w}-{l}"

        if tri not in out:
            out[tri] = {"record": rec, "points": int(pts) if isinstance(pts, int) else None}
        else:
            # не перезаписываем, если уже есть всё
            if out[tri].get("record") is None and rec:
                out[tri]["record"] = rec
            if out[tri].get("points") is None and isinstance(pts, int):
                out[tri]["points"] = int(pts)
    dbg("standings nodes scanned:", count_nodes, "got teams:", len(out))
    return out


# =========================
# Game data (landing + PBP)
# =========================

def fetch_landing(game_id: int) -> Dict[str, Any]:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/landing"
    return http_json(url)


def fetch_pbp(game_id: int) -> Dict[str, Any]:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    return http_json(url)


def extract_teams_from_landing(landing: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Возвращает (home, away) в унифицированном виде."""
    # Разные версии API называли поля по-разному, нормализуем
    home = landing.get("homeTeam") or landing.get("home") or {}
    away = landing.get("awayTeam") or landing.get("away") or {}

    def norm(x: Dict[str, Any]) -> Dict[str, Any]:
        tri = (x.get("abbrev") or x.get("triCode") or x.get("tricode") or "").upper()
        name = x.get("placeNameWithPreposition") or x.get("name") or TEAM_RU.get(tri) or tri
        score = x.get("score")
        rec = x.get("record") or x.get("teamRecord")  # строка "7-7-0" иногда кладут сюда
        return {"tri": tri, "name": name, "score": score, "record": rec}

    return norm(home), norm(away)


def extract_goals_from_pbp(pbp: Dict[str, Any], home_tri: str, away_tri: str) -> List[Dict[str, Any]]:
    """
    Возвращает массив событий-голов:
    {
      "period": 1|2|3|4...,
      "type": "REG"|"OT"|"SO",
      "clock": "MM.SS",      # время в периоде
      "scorer": "Last, First",
      "assists": ["A1", "A2"],  # может быть []
      "home": int, "away": int  # счёт после гола
    }
    """
    out = []
    plays = pbp.get("plays") or pbp.get("playByPlay") or pbp.get("allPlays") or []
    # Возможна другая форма: pbp["gameByPeriod"][i]["events"]
    if not plays and isinstance(pbp.get("gameByPeriod"), list):
        tmp = []
        for per in pbp["gameByPeriod"]:
            tmp.extend(per.get("events") or [])
        plays = tmp

    home_score = 0
    away_score = 0

    for ev in plays:
        etype = (ev.get("typeDescKey") or ev.get("typeDesc") or ev.get("eventTypeId") or "").lower()
        if etype != "goal":
            # обновим счёт, если в ивенте он есть (иногда есть "details": {"homeScore","awayScore"})
            det = ev.get("details") or {}
            if isinstance(det, dict):
                home_score = det.get("homeScore", home_score)
                away_score = det.get("awayScore", away_score)
            continue

        det = ev.get("details") or {}
        per_desc = ev.get("periodDescriptor") or ev.get("about") or {}
        period = per_desc.get("number") or ev.get("period") or 0
        period_type = (per_desc.get("periodType") or "").upper()
        clock = det.get("timeInPeriod") or per_desc.get("periodTimeRemaining")
        if not clock:
            # иногда в ev["time"] или ev["clock"]
            clock = ev.get("time") or ev.get("clock") or "0:00"
        clock = mmss_to_dot(str(clock))

        scorer = det.get("scorer") or {}
        # В разных версиях может быть:
        #   {"firstName": "...", "lastName": "..."} или {"player": {"fullName": "..."}}
        if isinstance(scorer, dict):
            if "fullName" in scorer:
                s_name = scorer["fullName"]
            else:
                s_name = " ".join([scorer.get("firstName", ""), scorer.get("lastName", "")]).strip()
        else:
            s_name = str(scorer)

        assists = []
        for a_key in ("assist1", "assist2", "assist3", "assists"):
            a_val = det.get(a_key)
            if not a_val:
                continue
            if isinstance(a_val, dict):
                if "fullName" in a_val:
                    assists.append(a_val["fullName"])
                else:
                    assists.append(" ".join([a_val.get("firstName", ""), a_val.get("lastName", "")]).strip())
            elif isinstance(a_val, list):
                for item in a_val:
                    if isinstance(item, dict):
                        nm = item.get("fullName") or " ".join([item.get("firstName", ""), item.get("lastName", "")]).strip()
                        if nm:
                            assists.append(nm)
                    else:
                        assists.append(str(item))
            else:
                assists.append(str(a_val))

        # после гола счёт — иногда приходит в details, а иногда нет
        h_after = det.get("homeScore")
        a_after = det.get("awayScore")
        if isinstance(h_after, int) and isinstance(a_after, int):
            home_score = h_after
            away_score = a_after
        else:
            # если не пришло — инкрементируем по команде автора
            team_tri = (det.get("teamAbbrev") or det.get("teamTricode") or ev.get("team", {}).get("triCode") or "").upper()
            if team_tri == home_tri:
                home_score += 1
            elif team_tri == away_tri:
                away_score += 1
            # иначе оставим как есть

        # Тип периода
        typ = "REG"
        if period >= 4:
            typ = "OT"
        if (ev.get("shootout") or period_type == "SHOOTOUT"):
            typ = "SO"

        out.append({
            "period": int(period) if isinstance(period, int) else period,
            "type": typ,
            "clock": clock,
            "scorer": s_name,
            "assists": assists,
            "home": home_score,
            "away": away_score,
        })

    # Отсортируем на всякий случай по периоду и времени (если API пришло перемешанным)
    def time_key(g):
        # "MM.SS" -> (MM, SS)
        t = g["clock"]
        if ":" in t:
            m, s = t.split(":")
        else:
            m, s = t.split(".") if "." in t else ("0", "0")
        try:
            return (int(g["period"]), int(m), int(s))
        except Exception:
            return (int(g.get("period", 0)), 99, 99)

    out.sort(key=time_key)
    return out


# =========================
# Formatting
# =========================

def period_header(period: int, typ: str) -> str:
    if typ == "SO":
        return "Серия буллитов"
    if period <= 3:
        return f"{period}-й период"
    # OT нумеруем
    return f"Овертайм №{period-3}"


def format_goal_line(goal: Dict[str, Any], home_first: bool) -> str:
    # Счёт выводим как "X:Y" с ориентацией относительно home
    if home_first:
        score = f"{goal['home']}:{goal['away']}"
    else:
        score = f"{goal['away']}:{goal['home']}"
    # Автор (Ассистенты)
    if goal["assists"]:
        return f"{score} – {goal['clock']} {goal['scorer']} ({', '.join(goal['assists'])})"
    else:
        return f"{score} – {goal['clock']} {goal['scorer']}"


def format_game_block(landing: Dict[str, Any],
                      goals: List[Dict[str, Any]],
                      standings_map: Dict[str, Dict[str, Any]]) -> str:
    home, away = extract_teams_from_landing(landing)
    # Названия/эмодзи
    h_tri, a_tri = home["tri"], away["tri"]
    h_name = TEAM_RU.get(h_tri, home["name"])
    a_name = TEAM_RU.get(a_tri, away["name"])
    h_emoji = TEAM_EMOJI.get(h_tri, "🏒")
    a_emoji = TEAM_EMOJI.get(a_tri, "🏒")

    # Счёт
    h_score = home.get("score")
    a_score = away.get("score")
    # Бывает, что score отсутствует в landing до полного OFF: попробуем из goals
    if not isinstance(h_score, int) or not isinstance(a_score, int):
        if goals:
            h_score = goals[-1]["home"]
            a_score = goals[-1]["away"]
        else:
            h_score = h_score or 0
            a_score = a_score or 0

    # Рекорды и очки
    def rec_and_pts(team):
        rec = team.get("record")
        pts = None
        if rec:
            pts = points_from_record(rec)
        if (not rec) or (pts is None):
            sm = standings_map.get(team["tri"], {})
            rec = rec or sm.get("record")
            pts = pts if pts is not None else sm.get("points")
        # финал: если очков нет, посчитаем из rec
        if pts is None and rec:
            pts = points_from_record(rec)
        return rec, pts

    h_rec, h_pts = rec_and_pts(home)
    a_rec, a_pts = rec_and_pts(away)

    # Шапка
    head = []
    head.append(f"{h_emoji} «{h_name}»: {h_score} ({h_rec if h_rec else '?'}{', ' if h_pts is not None else ''}{str(h_pts)+' о.' if h_pts is not None else ''})")
    head.append(f"{a_emoji} «{a_name}»: {a_score} ({a_rec if a_rec else '?'}{', ' if a_pts is not None else ''}{str(a_pts)+' о.' if a_pts is not None else ''})")
    out_lines = ["\n".join(head), ""]

    # Разбивка по периодам
    # Собираем по period/type
    by_period = {}
    for g in goals:
        key = (g["period"], g["type"])
        by_period.setdefault(key, []).append(g)

    # Порядок периодов: 1..3, затем OT, затем SO
    keys_sorted = sorted(by_period.keys(), key=lambda x: (x[0], 0 if x[1] == "REG" else (1 if x[1] == "OT" else 2)))

    for per, typ in keys_sorted:
        out_lines.append(period_header(per, typ))
        for g in by_period[(per, typ)]:
            out_lines.append(format_goal_line(g, home_first=True))
        out_lines.append("")  # пустая строка между блоками

    # remove trailing blank line
    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines)


# =========================
# Telegram
# =========================

def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        # локальный вывод
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": TELEGRAM_PARSE_MODE,
    }
    r = requests.post(url, json=payload, timeout=TIMEOUT)
    try:
        jr = r.json()
    except Exception:
        jr = {"text": r.text}
    if r.ok and jr.get("ok"):
        dbg("Telegram OK")
    else:
        dbg("Telegram ERROR", r.status_code, jr)


# =========================
# Window / Main
# =========================

def compute_msk_window() -> Tuple[datetime, datetime]:
    # Окно "вчера 15:00 MSK" → "сегодня 23:59:59 MSK" под nightly-посты
    now = datetime.now(MSK)
    start = (now - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end


def build_header(msk_end: datetime, n_games: int) -> str:
    # «13 матчей» склонение
    def plural(n: int, forms=("матч", "матча", "матчей")):
        n = abs(n) % 100
        n1 = n % 10
        if 10 < n < 20:
            return forms[2]
        if 1 == n1:
            return forms[0]
        if 2 <= n1 <= 4:
            return forms[1]
        return forms[2]

    return f"🗓 {DIV} • {ru_date(msk_end)} • {n_games} {plural(n_games)}\n\nРезультаты надёжно спрятаны 👇"


def main():
    msk_from, msk_to = compute_msk_window()
    dbg("MSK window:", msk_from.isoformat(), "→", msk_to.isoformat())

    game_ids = collect_final_game_ids(msk_from, msk_to)
    if not game_ids:
        send_telegram(f"🗓 {DIV} • {ru_date(msk_to)}\n\nСегодня завершённых матчей не найдено в заданном окне.")
        return

    # standings как подстраховка (очки/рекорд)
    try:
        standings_map = fetch_standings_now_map()
    except Exception as e:
        dbg("standings fail:", repr(e))
        standings_map = {}

    # собираем блоки
    game_blocks = []
    for gid in game_ids:
        try:
            landing = fetch_landing(gid)
            pbp = fetch_pbp(gid)
            home, away = extract_teams_from_landing(landing)
            goals = extract_goals_from_pbp(pbp, home["tri"], away["tri"])
            block = format_game_block(landing, goals, standings_map)
            game_blocks.append(SEPARATOR + "\n" + block)
        except Exception as e:
            dbg("game build fail:", gid, repr(e))

    text = build_header(msk_to, len(game_blocks)) + "\n\n" + "\n\n".join(game_blocks)
    send_telegram(text)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
