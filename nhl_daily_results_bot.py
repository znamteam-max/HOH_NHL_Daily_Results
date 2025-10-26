# -*- coding: utf-8 -*-
"""
NHL Daily Results -> Telegram (RU)
- Берём список игр за день из /v1/score/{date}
- Для каждой завершённой игры пытаемся достать детальную ленту:
    1) /v1/gamecenter/{gameId}/play-by-play     (основной источник)
    2) /v1/wsc/play-by-play/{gameId}            (фолбэк №1)
    3) /v1/gamecenter/{gameId}/landing          (фолбэк №2: summary/scoring)
- Перевод имён на кириллицу через sports.ru/person/<slug> (h1.titleH1)
- Если кириллицу не нашли — ОШИБКА (настраивается STRICT_RU)
- Форматируем по периодам. Для буллитов печатаем только победный буллит.
- Время голов в формате «абсолютные минуты»: MM.SS (например 61.28)
"""

import os, sys, time, json, re, math, random
import datetime as dt
from zoneinfo import ZoneInfo
from html import escape

import requests
from bs4 import BeautifulSoup

# =========================
# Конфигурация
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# Часовой пояс дня матчей (дату вывода заголовка считаем по Хельсинки)
TZ_OUTPUT = "Europe/Helsinki"

# Строго требовать кириллицу (True -> упасть с ошибкой, если не нашли ru-имя)
STRICT_RU = True

# Файлы кэша русских имён и «хвостов» для ручной донастройки
RU_CACHE_FILE   = "ru_names_cache.json"
RU_PENDING_FILE = "ru_pending_sportsru.json"

# Пауза между запросами к NHL API, чтобы не ловить блоки (сек.)
REQUEST_JITTER = (0.4, 0.9)

# =========================
# HTTP сессия
# =========================
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/125.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Origin": "https://www.nhl.com",
        "Referer": "https://www.nhl.com/",
        "Connection": "keep-alive",
    })
    return s

S = make_session()

def get_json(url, *, allow_retry=True):
    """
    Обёртка над GET с антибот-хедерами + таймаут + джиттер + логирование.
    Добавляем пустой параметр _=timestamp для обхода жёсткого кэша.
    """
    params = {"_": str(int(time.time() * 1000))}
    try:
        r = S.get(url, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        # иногда помогает один повтор
        if allow_retry and r.status_code in (403, 429, 503):
            time.sleep(random.uniform(0.8, 1.2))
            r2 = S.get(url, params={"_": str(int(time.time() * 1000))}, timeout=20)
            if r2.status_code == 200:
                return r2.json()
            print(f"[WARN] GET {url} -> {r.status_code} / retry {r2.status_code}", file=sys.stderr)
        else:
            print(f"[WARN] GET {url} -> {r.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"[ERR ] GET {url} -> {repr(e)}", file=sys.stderr)
    return {}

def sleep_jitter():
    time.sleep(random.uniform(*REQUEST_JITTER))

# =========================
# Утилиты времени и форматирования
# =========================
def fmt_abs_minutes(period_number: int, time_in_period: str) -> str:
    """
    period_number: 1..3, OT=4..n, SO не считаем (особая ветка)
    time_in_period: "M:SS" или "MM:SS" — пройденное время ПЕРИОДА.
    Возвращаем строку "MM.SS", где MM — абсолютные минуты с начала матча.
    """
    try:
        mm, ss = time_in_period.split(":")
        m = int(mm); s = int(ss)
    except Exception:
        return time_in_period.replace(":", ".")
    total = (period_number - 1) * 20 + m
    return f"{total}.{s:02d}"

def italic(s: str) -> str:
    return f"<i>{escape(s)}</i>"

# =========================
# Дата и игры дня
# =========================
def dates_for_fetch() -> str:
    # Берём вчера по Хельсинки, когда все игры точно закончены
    now = dt.datetime.now(ZoneInfo(TZ_OUTPUT))
    # Если раннее утро, нас интересует «вчерашняя ночь» по Северной Америке
    target = now - dt.timedelta(days=1)
    return target.strftime("%Y-%m-%d"), target.strftime("%-d %B").replace("January","января").replace("February","февраля")\
        .replace("March","марта").replace("April","апреля").replace("May","мая").replace("June","июня")\
        .replace("July","июля").replace("August","августа").replace("September","сентября")\
        .replace("October","октября").replace("November","ноября").replace("December","декабря")

def get_finished_games(date_str: str):
    """
    /v1/score/{date} -> { games: [...] }
    Берём только завершённые игры.
    """
    j = get_json(f"https://api-web.nhle.com/v1/score/{date_str}")
    games = j.get("games", [])
    finished = []
    for g in games:
        state = g.get("gameState") or g.get("gameStateCode") or ""
        if state in ("FINAL", "OFF", "COMPLETED"):  # OFF бывает после завершения
            finished.append(g)
    return finished

# =========================
# Извлечение авторов голов
# =========================
def fetch_goals_primary(game_id: int):
    """
    Основной источник: /gamecenter/{id}/play-by-play
    Возвращает кортеж (goals, shootout_winner) — где goals = список словарей:
      {period, time, abs_time, scorerId, scorer, assists: [ (id, name), ... ] }
    shootout_winner = {"scorerId","scorer"} или None
    """
    sleep_jitter()
    j = get_json(f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play")
    plays = j.get("plays") or []
    goals = []
    shootout_winner = None

    for p in plays:
        t = (p.get("typeDescKey") or "").lower()
        if t != "goal":
            continue
        pd = p.get("periodDescriptor", {})
        pn = int(pd.get("number") or 0)
        ptype = (pd.get("periodType") or "").upper()
        time_in = p.get("timeInPeriod") or p.get("timeRemaining") or "0:00"
        details = p.get("details", {}) or {}
        scorer_name = details.get("scoringPlayerName") or details.get("name") or ""
        scorer_id = details.get("scoringPlayerId") or details.get("playerId")

        # ассисты могут лежать как готовые поля, либо как список
        assists = []
        for k in ("assist1PlayerId", "assist2PlayerId"):
            pid = details.get(k)
            if pid:
                assists.append( (pid, details.get(k.replace("Id","Name"), "")) )
        # альтернативная структура
        if not assists and isinstance(details.get("assists"), list):
            for a in details["assists"]:
                assists.append( (a.get("playerId"), a.get("playerName")) )

        rec = {
            "period": pn,
            "ptype": ptype,  # REG, OT, SO
            "time": time_in,
            "abs_time": fmt_abs_minutes(pn, time_in) if ptype != "SO" else "65.00",
            "scorerId": scorer_id,
            "scorer": scorer_name,
            "assists": assists,
            "eventId": p.get("eventId")
        }

        if ptype == "SO":
            # для серии буллитов берём только победный — в PBP он помечается isGameWinning или gameWinning
            if details.get("isGameWinning") or details.get("gameWinning"):
                shootout_winner = {"scorerId": scorer_id, "scorer": scorer_name}
        else:
            goals.append(rec)

    # Отсортируем по (period, eventId) на всякий случай
    goals.sort(key=lambda r: (r["period"], r.get("eventId") or 0))
    return goals, shootout_winner

def fetch_goals_fallback_wsc(game_id: int):
    """
    Фолбэк №1: /v1/wsc/play-by-play/{id}
    Структура похожа, но поля могут называться иначе. Берём только «goal».
    """
    sleep_jitter()
    j = get_json(f"https://api-web.nhle.com/v1/wsc/play-by-play/{game_id}")
    items = j.get("plays") or j.get("items") or []
    goals = []
    shootout_winner = None

    for p in items:
        t = (p.get("typeDescKey") or p.get("type") or "").lower()
        if t != "goal":
            continue
        pd = p.get("periodDescriptor", {})
        pn = int(pd.get("number") or 0)
        ptype = (pd.get("periodType") or "").upper()
        time_in = p.get("timeInPeriod") or p.get("time") or "0:00"
        d = p.get("details") or p

        scorer_id = d.get("scoringPlayerId") or d.get("playerId")
        scorer = d.get("scoringPlayerName") or d.get("playerName") or d.get("name") or ""

        assists = []
        if isinstance(d.get("assists"), list):
            for a in d["assists"]:
                assists.append( (a.get("playerId"), a.get("playerName")) )
        else:
            for k in ("assist1PlayerId", "assist2PlayerId"):
                pid = d.get(k)
                if pid:
                    assists.append( (pid, d.get(k.replace("Id","Name"), "")) )

        rec = {
            "period": pn,
            "ptype": ptype,
            "time": time_in,
            "abs_time": fmt_abs_minutes(pn, time_in) if ptype != "SO" else "65.00",
            "scorerId": scorer_id,
            "scorer": scorer,
            "assists": assists,
            "eventId": p.get("eventId") or p.get("eventNumber") or 0
        }
        if ptype == "SO":
            if d.get("isGameWinning") or d.get("gameWinning"):
                shootout_winner = {"scorerId": scorer_id, "scorer": scorer}
        else:
            goals.append(rec)

    goals.sort(key=lambda r: (r["period"], r.get("eventId") or 0))
    return goals, shootout_winner

def fetch_goals_fallback_landing(game_id: int):
    """
    Фолбэк №2: /v1/gamecenter/{id}/landing
    Внутри есть разделы summary/scoring/byPeriod. Там имена как «Инициалы Фамилия», без ID,
    поэтому позже постараемся соотнести через boxscore (roster).
    """
    sleep_jitter()
    j = get_json(f"https://api-web.nhle.com/v1/gamecenter/{game_id}/landing")
    scoring = (j.get("summary") or {}).get("scoring")
    if not scoring:
        return [], None

    goals = []
    shootout_winner = None
    for byp in scoring.get("byPeriod", []):
        pn = int(byp.get("periodDescriptor", {}).get("number") or 0)
        ptype = (byp.get("periodDescriptor", {}).get("periodType") or "").upper()
        for ev in byp.get("goals", []):
            # имена без ID, сохраним как есть — потом попробуем по boxscore сопоставить
            time_in = ev.get("timeInPeriod") or "0:00"
            scorer = ev.get("scorer") or ""
            assists_names = ev.get("assists") or []
            assists = [ (None, name) for name in assists_names ]
            rec = {
                "period": pn,
                "ptype": ptype,
                "time": time_in,
                "abs_time": fmt_abs_minutes(pn, time_in) if ptype != "SO" else "65.00",
                "scorerId": None,
                "scorer": scorer,
                "assists": assists,
                "eventId": 0
            }
            if ptype == "SO":
                if ev.get("gameWinning"):
                    shootout_winner = {"scorerId": None, "scorer": scorer}
            else:
                goals.append(rec)
    goals.sort(key=lambda r: (r["period"], r.get("eventId") or 0))
    return goals, shootout_winner

def fetch_box_roster_names(game_id: int):
    """
    /v1/gamecenter/{id}/boxscore -> карта {playerId: "First Last"}
    """
    sleep_jitter()
    j = get_json(f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore")
    mp = {}
    for side in ("homeTeam", "awayTeam"):
        team = (j.get("playerByGameStats") or {}).get(side) or {}
        for group in ("forwards", "defense", "goalies"):
            for p in team.get(group, []):
                pid = p.get("playerId")
                fn  = ((p.get("firstName") or {}).get("default") or "").strip()
                ln  = ((p.get("lastName") or {}).get("default") or "").strip()
                full = f"{fn} {ln}".strip()
                if pid and full:
                    mp[pid] = full
    return mp

# =========================
# Кириллица с sports.ru
# =========================
def load_json_file(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

RU_MAP = load_json_file(RU_CACHE_FILE)  # { "playerId": "И. Фамилия" }
RU_PENDING = load_json_file(RU_PENDING_FILE)  # { playerId: ["slug1","slug2", ...] }

def save_json(path: str, obj):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] save {path}: {e}", file=sys.stderr)

def slugify_name_en(full_en: str):
    # "Leon Draisaitl" -> "leon-draisaitl"
    name = re.sub(r"[^A-Za-z \-']", " ", full_en).strip()
    name = re.sub(r"\s+", " ", name)
    parts = name.lower().split()
    if not parts:
        return ""
    return "-".join(parts)

def sportsru_fetch_ru(full_en: str):
    """
    Пытаемся вытащить h1.titleH1 со страницы sports.ru/person/<slug>/.
    Возвращаем строку в виде "И. Фамилия" по требованию (инициал + фамилия).
    """
    base = "https://www.sports.ru/hockey/person/"
    candidates = []
    slug = slugify_name_en(full_en)
    if slug:
        candidates.append(slug)
    # Если вдруг двойная фамилия, попробуем последний токен отдельно
    tokens = slug.split("-") if slug else []
    if len(tokens) >= 2:
        candidates.append(tokens[-1])

    for c in candidates:
        url = base + c + "/"
        try:
            r = S.get(url, timeout=20)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            h1 = soup.select_one("h1.titleH1")
            if not h1:
                continue
            ru_full = h1.get_text(strip=True)
            # Преобразуем "Имя Фамилия" -> "И. Фамилия"
            parts = ru_full.split()
            if len(parts) >= 2:
                fam = parts[-1]
                initial = parts[0][0] + "."
                return f"{initial} {fam}"
            # если страница есть, но формат необычный — лучше вернуть полное
            return ru_full
        except Exception as e:
            print(f"[WARN] sports.ru {url}: {e}", file=sys.stderr)
    return None

def to_ru_initial(player_id, full_en: str) -> str:
    if not full_en:
        return None
    # Кэш
    pid = str(player_id) if player_id is not None else None
    if pid and (val := RU_MAP.get(pid)):
        return val

    ru = sportsru_fetch_ru(full_en)
    if ru:
        if pid:
            RU_MAP[pid] = ru
        return ru

    # Запишем кандидатов для ручной правки
    if pid:
        slug = slugify_name_en(full_en)
        RU_PENDING.setdefault(pid, [])
        for cand in filter(None, [slug, slug.split("-")[-1] if slug else None]):
            if cand not in RU_PENDING[pid]:
                RU_PENDING[pid].append(cand)
    return None

# =========================
# Сборка блока матча
# =========================
def build_game_block(g, date_str: str) -> str:
    # Названия команд
    home_name = g.get("homeTeam", {}).get("name", "")
    away_name = g.get("awayTeam", {}).get("name", "")
    home_tr   = g.get("homeTeam", {}).get("placeName", {}).get("default", home_name)
    away_tr   = g.get("awayTeam", {}).get("placeName", {}).get("default", away_name)

    home_score = g.get("homeTeam", {}).get("score", 0)
    away_score = g.get("awayTeam", {}).get("score", 0)
    winner_bold_home = home_score > away_score

    game_id = g.get("id") or g.get("gameId")

    # Пытаемся получить цели через цепочку источников
    goals, shootout = fetch_goals_primary(game_id)
    src_used = "PBP"
    if not goals and not shootout:
        print(f"[INFO] game {game_id}: primary PBP empty -> try WSC", file=sys.stderr)
        goals, shootout = fetch_goals_fallback_wsc(game_id)
        src_used = "WSC"
    if not goals and not shootout:
        print(f"[INFO] game {game_id}: WSC empty -> try landing", file=sys.stderr)
        goals, shootout = fetch_goals_fallback_landing(game_id)
        src_used = "LANDING"

    # Для сопоставления англ. ФИО -> ID достанем ростер
    roster = fetch_box_roster_names(game_id)

    # Переведём лидеров в кириллицу
    ru_miss = []
    lines = []

    # Заголовок (жирным у победителя счёт)
    home_line = f"{escape(home_tr)}: {home_score}"
    away_line = f"{escape(away_tr)}: {away_score}"
    if winner_bold_home:
        home_line = f"<b>{home_line}</b>"
    else:
        away_line = f"<b>{away_line}</b>"

    lines.append(f"{home_line}\n{away_line}\n")

    if not goals and not shootout:
        lines.append("— события матча недоступны")
        return "\n".join(lines)

    # Группируем по периоду
    goals_by_period = {}
    for r in goals:
        goals_by_period.setdefault(r["period"], []).append(r)

    # Печатаем периоды по порядку
    period_names = {}
    for p in sorted(goals_by_period.keys()):
        if p <= 3:
            period_names[p] = f"{p}-й период"
        else:
            period_names[p] = f"Овертайм №{p-3}"

    # Построим функцию перевода «EN -> RU инициалы»
    def ru_name(player_id, en_full, fallback_en=False):
        ru = to_ru_initial(player_id, en_full)
        if ru:
            return ru
        # соберём пропуск для отчёта
        ru_miss.append((player_id, en_full))
        return en_full if fallback_en else None

    # У нас в PBP могут лежать «M. Smith». Попробуем развернуть по roster с ID.
    def normalize_full_en(pid, raw_name):
        if pid in roster:
            return roster[pid]
        # если pid None (landing), попытаемся по последнему слову подобрать из ростера
        last = (raw_name or "").split()[-1].lower()
        if last:
            for rid, full in roster.items():
                if full.lower().split()[-1] == last:
                    return full
        return raw_name or ""

    for p in sorted(goals_by_period.keys()):
        lines.append(italic(period_names[p]))
        for r in goals_by_period[p]:
            # нормализуем ФИО
            en_full = normalize_full_en(r.get("scorerId"), r.get("scorer"))
            ru_scorer = ru_name(r.get("scorerId"), en_full, fallback_en=False)

            as_ru = []
            for aid, aname in (r.get("assists") or []):
                full_en = normalize_full_en(aid, aname)
                ru_ass = ru_name(aid, full_en, fallback_en=False)
                if ru_ass:
                    as_ru.append(ru_ass)

            # Если кого-то не нашли — позже упадём (STRICT_RU)
            # Формат: X:Y – MM.SS Ф. Фамилия (И. Фамилия, И. Фамилия)
            score_before = ""  # Опционально можно посчитать текущий счёт, но это дорого без live логики
            time_abs = r.get("abs_time") or r.get("time")
            assists_txt = ""
            if as_ru:
                assists_txt = " (" + ", ".join(as_ru) + ")"

            if not ru_scorer:
                # мы добавили в ru_miss, просто вставим заглушку — всё равно упадём позже
                ru_scorer = normalize_full_en(r.get("scorerId"), r.get("scorer"))

            lines.append(f"{score_before}{r.get('time') and ''}{escape(r.get('time') and '')}"
                         )  # placeholder чтобы не путать старый вид; основное — abs_time ниже
            # заменим пред. строку корректной, без score_before:
            lines[-1] = f"{escape(r.get('abs_time'))} {escape(ru_scorer)}{escape(assists_txt)}"

    # Победный буллит, если был
    if shootout:
        en_full = normalize_full_en(shootout.get("scorerId"), shootout.get("scorer"))
        ru_scorer = ru_name(shootout.get("scorerId"), en_full, fallback_en=False) or en_full
        lines.append(italic("Победный буллит"))
        lines.append(f"65.00 {escape(ru_scorer)}")

    # Если строго требуем кириллицу — проверим нехватки
    if STRICT_RU and ru_miss:
        # Сохраним pending для доработки
        save_json(RU_PENDING_FILE, RU_PENDING)
        save_json(RU_CACHE_FILE, RU_MAP)
        preview = "\n".join([f"- id={pid} | {name}" for pid, name in ru_miss[:15]])
        raise RuntimeError(
            "Не удалось получить имена на кириллице для некоторых игроков sports.ru.\n"
            "Примеры:\n" + preview + "\n…см. ru_pending_sportsru.json для всех."
        )

    return "\n".join(lines)

# =========================
# Сборка сообщения дня
# =========================
def build_message():
    date_api, date_human = dates_for_fetch()
    games = get_finished_games(date_api)

    title = f"🗓 Регулярный чемпионат НХЛ • {date_human} • {len(games)} матчей\n\n" \
            f"Результаты надёжно спрятаны 👇\n\n" \
            f"——————————————————\n"

    blocks = []
    for g in games:
        try:
            blocks.append(build_game_block(g, date_api))
        except Exception as e:
            # Не роняем весь пост — покажем матч и причину
            print(f"[ERR ] game {g.get('id')}: {e}", file=sys.stderr)
            home = g.get("homeTeam", {}).get("name","")
            away = g.get("awayTeam", {}).get("name","")
            hs = g.get("homeTeam", {}).get("score",0)
            as_ = g.get("awayTeam", {}).get("score",0)
            winner_home = hs > as_
            home_line = f"{escape(home)}: {hs}"
            away_line = f"{escape(away)}: {as_}"
            if winner_home: home_line = f"<b>{home_line}</b>"
            else: away_line = f"<b>{away_line}</b>"
            blocks.append(f"{home_line}\n{away_line}\n\n— события матча недоступны")

    body = "\n\n".join(blocks)
    return title + body

# =========================
# Telegram
# =========================
def send_telegram(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        print("No TELEGRAM_BOT_TOKEN/CHAT_ID provided", file=sys.stderr)
        return
    # Чтобы телега не ломала курсывы/жирный — HTML режим
    r = S.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
               json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
                     "disable_web_page_preview": True},
               timeout=20)
    if r.status_code != 200:
        print(f"[ERR ] telegram send: {r.status_code} {r.text[:200]}", file=sys.stderr)

# =========================
# main
# =========================
if __name__ == "__main__":
    try:
        msg = build_message()
        # Сохраним кэш имён/пенд. на диск
        save_json(RU_CACHE_FILE, RU_MAP)
        save_json(RU_PENDING_FILE, RU_PENDING)
        send_telegram(msg)
        print("OK")
    except Exception as e:
        # Сохраним то, что уже наковыряли, чтобы не потерять прогресс
        save_json(RU_CACHE_FILE, RU_MAP)
        save_json(RU_PENDING_FILE, RU_PENDING)
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
