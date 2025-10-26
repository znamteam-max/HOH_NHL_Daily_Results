#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results -> Telegram (RU)
- Игры: /v1/score/{date}
- Детализация:
    1) /v1/gamecenter/{gameId}/play-by-play
    2) /v1/wsc/play-by-play/{gameId}
    3) /v1/gamecenter/{gameId}/landing  (summary/scoring/byPeriod)
- Имёны игроков: строго со sports.ru/hockey/person/<slug>/ из <h1 class="titleH1">.
  Нет кириллицы -> считаем НЕ НАЙДЕНО и падаем.
- Печать по периодам, время голов — абсолютное (MM.SS от старта матча).
- Серия буллитов: печатаем только «Победный буллит».

requirements.txt:
    requests==2.32.3
    beautifulsoup4==4.12.3
"""

import os, sys, re, json, time, random, unicodedata
import datetime as dt
from zoneinfo import ZoneInfo
from html import escape

import requests
from bs4 import BeautifulSoup

# ───────── Конфиг
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TZ_OUTPUT = "Europe/Helsinki"

STRICT_RU = True  # если не нашли кириллицу на sports.ru — падать
RU_CACHE_FILE   = "ru_names_sportsru.json"
RU_PENDING_FILE = "ru_pending_sportsru.json"

REQUEST_JITTER = (0.4, 0.9)
API = "https://api-web.nhle.com/v1"
SPORTS_ROOT = "https://www.sports.ru/hockey/person/"

# ───────── HTTP
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/125.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Origin": "https://www.nhl.com",
        "Referer": "https://www.nhl.com/",
        "Connection": "keep-alive",
    })
    return s

S = make_session()

def get_json(url, *, allow_retry=True):
    params = {"_": str(int(time.time()*1000))}
    try:
        r = S.get(url, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        if allow_retry and r.status_code in (403, 429, 503):
            time.sleep(random.uniform(0.8, 1.2))
            r2 = S.get(url, params={"_": str(int(time.time()*1000))}, timeout=20)
            if r2.status_code == 200:
                return r2.json()
            print(f"[WARN] GET {url} -> {r.status_code} / retry {r2.status_code}", file=sys.stderr)
        else:
            print(f"[WARN] GET {url} -> {r.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"[ERR ] GET {url} -> {repr(e)}", file=sys.stderr)
    return {}

def jitter():
    time.sleep(random.uniform(*REQUEST_JITTER))

# ───────── Утилиты строк
def to_text(v) -> str:
    """Любое поле ⇒ нормализованная строка. Поддерживает dict с 'default', 'name' и т.п."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        for key in ("default", "name", "fullName", "scoringPlayerName", "playerName"):
            val = v.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
            if isinstance(val, dict):
                inner = val.get("default")
                if isinstance(inner, str) and inner.strip():
                    return inner.strip()
        # склеим все строковые значения
        parts = [x for x in v.values() if isinstance(x, str)]
        if parts:
            return " ".join(parts).strip()
        return ""
    # список имён → склеить
    if isinstance(v, list):
        return " ".join(to_text(x) for x in v if x).strip()
    return str(v)

# ───────── Дата
RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}
def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def pick_report_date() -> dt.date:
    now = dt.datetime.now(ZoneInfo(TZ_OUTPUT))
    # Нам нужна дата по Хельсинки за «вчера», чтобы все матчи уже точно завершились
    return (now - dt.timedelta(days=1)).date()

# ───────── Игры дня
def fetch_games_for_date(day: dt.date) -> list[dict]:
    j = get_json(f"{API}/score/{day.isoformat()}")
    games = j.get("games") or []
    finished = []
    for g in games:
        state = (g.get("gameState") or g.get("gameStateCode") or "").upper()
        if state in {"FINAL", "OFF", "COMPLETED"}:
            finished.append(g)
    return finished

# ───────── Время
def abs_time_mmss(period_num: int, time_in_period: str, ptype: str) -> str:
    """Вернёт абсолютное время MM.SS. Для SO — 65.00, для OT — 60+."""
    t = to_text(time_in_period)
    try:
        m, s = t.split(":")
        mm = int(m); ss = int(s)
    except Exception:
        return t.replace(":", ".") if isinstance(t, str) else "0.00"
    if ptype == "SO":
        return "65.00"
    if period_num >= 4:
        return f"{60 + mm}.{ss:02d}"
    return f"{(period_num-1)*20 + mm}.{ss:02d}"

# ───────── Boxscore: roster (id -> "First Last")
def fetch_box_roster_names(game_id: int) -> dict[int, str]:
    jitter()
    j = get_json(f"{API}/gamecenter/{game_id}/boxscore")
    roster = {}
    pbgs = j.get("playerByGameStats") or {}
    for side in ("homeTeam", "awayTeam"):
        team = pbgs.get(side) or {}
        for group in ("forwards", "defense", "goalies"):
            for p in team.get(group, []) or []:
                pid = p.get("playerId")
                fn  = to_text((p.get("firstName") or {}))
                ln  = to_text((p.get("lastName")  or {}))
                full = f"{fn} {ln}".strip()
                if pid and full:
                    roster[int(pid)] = full
    return roster

# ───────── PBP основа
def fetch_goals_primary(game_id: int):
    jitter()
    j = get_json(f"{API}/gamecenter/{game_id}/play-by-play")
    plays = j.get("plays") or []
    goals, shootout = [], None

    for p in plays:
        t = to_text(p.get("typeDescKey")).lower()
        if t != "goal":
            continue
        pd = p.get("periodDescriptor") or {}
        pn = int(pd.get("number") or 0)
        ptype = to_text(pd.get("periodType")).upper() or "REG"
        time_in = to_text(p.get("timeInPeriod") or p.get("timeRemaining") or "0:00")
        d = p.get("details") or {}

        scorer_id = d.get("scoringPlayerId") or d.get("playerId")
        scorer = to_text(d.get("scoringPlayerName") or d.get("playerName") or d.get("name"))

        assists = []
        if isinstance(d.get("assists"), list):
            for a in d["assists"]:
                assists.append((a.get("playerId"), to_text(a.get("playerName"))))
        else:
            for k in ("assist1PlayerId","assist2PlayerId"):
                pid = d.get(k)
                if pid:
                    assists.append((pid, to_text(d.get(k.replace("Id","Name")))))

        rec = {
            "period": pn,
            "ptype": ptype,
            "time": time_in,
            "abs_time": abs_time_mmss(pn, time_in, ptype),
            "scorerId": scorer_id,
            "scorer": scorer,
            "assists": assists,
            "eventId": p.get("eventId") or 0,
            "teamId": d.get("eventOwnerTeamId") or p.get("teamId") or d.get("teamId"),
        }

        if ptype == "SO":
            if (d.get("isGameWinning") or d.get("gameWinning")):
                shootout = {"scorerId": scorer_id, "scorer": scorer}
        else:
            goals.append(rec)

    goals.sort(key=lambda r: (r["period"], r["eventId"]))
    return goals, shootout

# ───────── WSC фолбэк
def fetch_goals_fallback_wsc(game_id: int):
    jitter()
    j = get_json(f"{API}/wsc/play-by-play/{game_id}")
    items = j.get("plays") or j.get("items") or []
    goals, shootout = [], None

    for p in items:
        t = to_text(p.get("typeDescKey") or p.get("type")).lower()
        if t != "goal":
            continue
        pd = p.get("periodDescriptor") or {}
        pn = int(pd.get("number") or 0)
        ptype = to_text(pd.get("periodType")).upper() or "REG"
        time_in = to_text(p.get("timeInPeriod") or p.get("time") or "0:00")
        d = p.get("details") or p

        scorer_id = d.get("scoringPlayerId") or d.get("playerId")
        scorer = to_text(d.get("scoringPlayerName") or d.get("playerName") or d.get("name"))

        assists = []
        if isinstance(d.get("assists"), list):
            for a in d["assists"]:
                assists.append((a.get("playerId"), to_text(a.get("playerName") or a.get("name"))))
        else:
            for k in ("assist1PlayerId","assist2PlayerId"):
                pid = d.get(k)
                if pid:
                    assists.append((pid, to_text(d.get(k.replace("Id","Name")))))

        rec = {
            "period": pn, "ptype": ptype, "time": time_in,
            "abs_time": abs_time_mmss(pn, time_in, ptype),
            "scorerId": scorer_id, "scorer": scorer,
            "assists": assists, "eventId": p.get("eventId") or p.get("eventNumber") or 0,
            "teamId": d.get("eventOwnerTeamId") or p.get("teamId") or d.get("teamId"),
        }
        if ptype == "SO":
            if (d.get("isGameWinning") or d.get("gameWinning")):
                shootout = {"scorerId": scorer_id, "scorer": scorer}
        else:
            goals.append(rec)

    goals.sort(key=lambda r: (r["period"], r["eventId"]))
    return goals, shootout

# ───────── landing фолбэк
def fetch_goals_fallback_landing(game_id: int):
    jitter()
    j = get_json(f"{API}/gamecenter/{game_id}/landing")
    scoring = (j.get("summary") or {}).get("scoring") or {}
    goals, shootout = [], None
    for byp in scoring.get("byPeriod", []) or []:
        pd = byp.get("periodDescriptor") or {}
        pn = int(pd.get("number") or 0)
        ptype = to_text(pd.get("periodType")).upper() or "REG"
        for ev in byp.get("goals", []) or []:
            time_in = to_text(ev.get("timeInPeriod") or "0:00")
            scorer = to_text(ev.get("scorer"))
            assists_names = ev.get("assists") or []
            # assists может быть списком строк/словари
            assists = []
            for a in assists_names:
                assists.append((None, to_text(a)))
            rec = {
                "period": pn, "ptype": ptype, "time": time_in,
                "abs_time": abs_time_mmss(pn, time_in, ptype),
                "scorerId": None, "scorer": scorer,
                "assists": assists, "eventId": 0,
                "teamId": None,
            }
            if ptype == "SO":
                if ev.get("gameWinning"):
                    shootout = {"scorerId": None, "scorer": scorer}
            else:
                goals.append(rec)
    goals.sort(key=lambda r: (r["period"], r["eventId"]))
    return goals, shootout

# ───────── Кириллица со sports.ru
def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

RU_MAP     = load_json(RU_CACHE_FILE, {})
RU_PENDING = load_json(RU_PENDING_FILE, {})

def _norm_ascii(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = s.replace("'", "").replace("’","").replace("."," ").replace("/"," ")
    s = re.sub(r"[^a-z0-9\\- ]+", " ", s)
    s = re.sub(r"\\s+", "-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
    return s

def slugify_en(full_en: str) -> list[str]:
    """Возвращает список кандидатов slug: ['first-last', 'last']"""
    name = re.sub(r"[^A-Za-z\\-\\s']", " ", full_en).strip()
    name = re.sub(r"\\s+", " ", name)
    parts = name.lower().split()
    if not parts:
        return []
    slug = "-".join(parts)
    cands = [slug]
    if len(parts) >= 2:
        cands.append(parts[-1])  # только фамилия
    # уникализируем
    out, seen = [], set()
    for c in cands:
        c = _norm_ascii(c)
        if c and c not in seen:
            out.append(c); seen.add(c)
    return out

def sportsru_fetch_ru_initial(full_en: str):
    """Ищем страницу игрока на sports.ru и берём <h1 class="titleH1"> «Имя Фамилия» -> «И. Фамилия»."""
    candidates = slugify_en(full_en)
    for slug in candidates:
        url = f"{SPORTS_ROOT}{slug}/"
        try:
            r = S.get(url, timeout=20)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            h1 = soup.select_one("h1.titleH1") or soup.find("h1")
            if not h1:
                continue
            ru_full = to_text(h1.get_text(strip=True))
            # проверим наличие кириллицы
            if not re.search(r"[А-Яа-яЁё]", ru_full):
                continue
            parts = ru_full.split()
            if len(parts) >= 2:
                fam = parts[-1]
                ini = parts[0][0] + "."
                return f"{ini} {fam}"
            return ru_full
        except Exception as e:
            print(f"[WARN] sports.ru {url}: {e}", file=sys.stderr)
    return None, candidates

def to_ru_initial(player_id: int | None, full_en: str) -> str | None:
    if not full_en:
        return None
    pid = str(player_id) if player_id is not None else None
    if pid and (v := RU_MAP.get(pid)):
        return v
    ru = sportsru_fetch_ru_initial(full_en)
    if isinstance(ru, tuple):
        # не нашли
        _, cands = ru
        if pid:
            RU_PENDING.setdefault(pid, [])
            for sl in cands:
                if sl not in RU_PENDING[pid]:
                    RU_PENDING[pid].append(sl)
        return None
    if ru:
        if pid:
            RU_MAP[pid] = ru
        return ru
    return None

# ───────── Построение блока матча
def build_game_block(g: dict) -> str:
    game_id = g.get("id") or g.get("gameId")
    home_name = to_text((g.get("homeTeam") or {}).get("name"))
    away_name = to_text((g.get("awayTeam") or {}).get("name"))
    home_score = int((g.get("homeTeam") or {}).get("score") or 0)
    away_score = int((g.get("awayTeam") or {}).get("score") or 0)
    winner_home = home_score > away_score

    # цели голов
    goals, shootout = fetch_goals_primary(game_id)
    if not goals and not shootout:
        print(f"[INFO] {game_id}: PBP empty -> WSC", file=sys.stderr)
        goals, shootout = fetch_goals_fallback_wsc(game_id)
    if not goals and not shootout:
        print(f"[INFO] {game_id}: WSC empty -> LANDING", file=sys.stderr)
        goals, shootout = fetch_goals_fallback_landing(game_id)

    roster = fetch_box_roster_names(game_id)

    # Заголовок
    home_line = f"{escape(home_name)}: {home_score}"
    away_line = f"{escape(away_name)}: {away_score}"
    if winner_home:
        home_line = f"<b>{home_line}</b>"
    else:
        away_line = f"<b>{away_line}</b>"

    parts = [f"{home_line}\n{away_line}\n"]

    if not goals and not shootout:
        raise RuntimeError("Нет событий матча (play-by-play пуст)")

    # билд имя EN → EN full (по roster), затем → RU (sports.ru)
    def normalize_full_en(pid, raw):
        raw = to_text(raw)
        if pid and pid in roster:
            return roster[pid]
        if raw:
            # сопоставим по фамилии, если pid None
            last = raw.split()[-1].lower()
            for rid, full in roster.items():
                if full.lower().split()[-1] == last:
                    return full
        return raw

    # собираем по периодам
    goals_by_p = {}
    for r in goals:
        goals_by_p.setdefault(r["period"], []).append(r)

    ru_missing = []  # [(pid, "First Last"), ...]

    def ru_name(pid, en_full) -> str | None:
        ru = to_ru_initial(pid, en_full)
        if ru:
            return ru
        ru_missing.append((pid, en_full))
        return None

    for p in sorted(goals_by_p.keys()):
        if p <= 3:
            parts.append(f"<i>{p}-й период</i>")
        else:
            parts.append(f"<i>Овертайм №{p-3}</i>")
        for r in goals_by_p[p]:
            en_scorer = normalize_full_en(r.get("scorerId"), r.get("scorer"))
            ru_scorer = ru_name(r.get("scorerId"), en_scorer) or en_scorer  # временно подставим EN — потом упадём

            ass_ru = []
            for aid, aname in (r.get("assists") or []):
                en_a = normalize_full_en(aid, aname)
                rux = ru_name(aid, en_a) or en_a
                ass_ru.append(rux)

            t_abs = to_text(r.get("abs_time")) or to_text(r.get("time")) or ""
            ass_txt = f" ({', '.join(escape(x) for x in ass_ru if x)})" if ass_ru else ""
            parts.append(f"{escape(t_abs)} {escape(ru_scorer)}{ass_txt}")

    # Победный буллит
    if shootout:
        parts.append("<i>Победный буллит</i>")
        en_w = normalize_full_en(shootout.get("scorerId"), shootout.get("scorer"))
        ru_w = ru_name(shootout.get("scorerId"), en_w) or en_w
        parts.append(f"65.00 {escape(ru_w)}")

    if STRICT_RU and ru_missing:
        # сохраняем кэши/пенд и падаем
        save_json(RU_CACHE_FILE, RU_MAP)
        save_json(RU_PENDING_FILE, RU_PENDING)
        preview = "\n".join(
            f"- id={pid} | {name}" for pid, name in ru_missing[:20]
        )
        raise RuntimeError(
            "Не удалось получить имена на кириллице для некоторых игроков sports.ru.\n"
            "Примеры:\n" + preview + ("\n…см. ru_pending_sportsru.json" if len(ru_missing) > 20 else "")
        )

    return "\n".join(parts)

# ───────── Пост
def build_post(day: dt.date) -> str:
    games = fetch_games_for_date(day)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(day)} • {len(games)} " + \
            ("матч" if len(games)==1 else "матча" if len(games)%10 in (2,3,4) and not 12<=len(games)%100<=14 else "матчей")
    head = f"{title}\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    if not games:
        return head.strip()

    blocks = []
    for i, g in enumerate(games, 1):
        blocks.append(build_game_block(g))
        if i < len(games):
            blocks.append("")

    return head + "\n".join(blocks).strip()

# ───────── Telegram
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    maxlen = 3500
    rest = text
    while rest:
        if len(rest) <= maxlen:
            chunk, rest = rest, ""
        else:
            cut = rest.rfind("\n\n", 0, maxlen)
            if cut == -1: cut = maxlen
            chunk, rest = rest[:cut], rest[cut:].lstrip()
        r = S.post(url, json={
            "chat_id": CHAT_ID, "text": chunk, "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:200]}")
        time.sleep(0.25)

# ───────── main
if __name__ == "__main__":
    try:
        day = pick_report_date()
        text = build_post(day)
        # сохраняем кэш имён
        save_json(RU_CACHE_FILE, RU_MAP)
        save_json(RU_PENDING_FILE, RU_PENDING)
        tg_send(text)
        print("OK")
    except Exception as e:
        save_json(RU_CACHE_FILE, RU_MAP)
        save_json(RU_PENDING_FILE, RU_PENDING)
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
