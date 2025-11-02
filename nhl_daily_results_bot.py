# -*- coding: utf-8 -*-
"""
NHL Daily Results → Telegram (RU)
- Берём завершённые матчи NHL за MSK-день: [D-1 15:00, D 23:59:59] по Москве
- Голы и счёт — из NHL PBP
- Имена (кириллица) — из sports.ru (строго события «Гол» или паттерн «Фамилия (ассистенты)»), сопоставление по (период, mm:ss±15с)
- Если на sports.ru нет части голов, оставляем "—", счёт корректный
- Рекорды и очки — из /v1/standings/now (с поддержкой dict в teamAbbrev)

Env:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID — для отправки
  TEAM_EMOJI_JSON — опционально (кастомные эмодзи)
  REPORT_DATE_MSK — YYYY-MM-DD (опционально)
"""

import os
import sys
import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple, Any, Optional

import requests
from bs4 import BeautifulSoup

# ----------------------- ЛОГ -----------------------

def dbg(*a):
    print("[DBG]", *a)

def err(*a):
    print("ERROR:", *a, file=sys.stderr)

# ----------------------- КОНСТАНТЫ -----------------------

MSK = ZoneInfo("Europe/Moscow")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()

COMPLETE_STATES = {"OFF", "FINAL", "COMPLETED", "END"}

HEADERS_WEB = {"User-Agent": "Mozilla/5.0 (compatible; HOH_NHL_Bot/1.1)"}

RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

RU_TEAMS = {
    "ANA": "Анахайм", "ARI": "Аризона", "UTA": "Юта", "BOS": "Бостон",
    "BUF": "Баффало", "CGY": "Калгари", "CAR": "Каролина", "CHI": "Чикаго",
    "COL": "Колорадо", "CBJ": "Коламбус", "DAL": "Даллас", "DET": "Детройт",
    "EDM": "Эдмонтон", "FLA": "Флорида", "LAK": "Лос-Анджелес", "MIN": "Миннесота",
    "MTL": "Монреаль", "NJD": "Нью-Джерси", "NSH": "Нэшвилл", "NYI": "Айлендерс",
    "NYR": "Рейнджерс", "OTT": "Оттава", "PHI": "Филадельфия", "PIT": "Питтсбург",
    "SJS": "Сан-Хосе", "SEA": "Сиэтл", "STL": "Сент-Луис", "TBL": "Тампа-Бэй",
    "TOR": "Торонто", "VAN": "Ванкувер", "VGK": "Вегас", "WSH": "Вашингтон", "WPG": "Виннипег",
}

DEFAULT_EMOJI = {
    "ANA": "🦆", "UTA": "🦣", "ARI": "🦂", "BOS": "🐻", "BUF": "🦬", "CGY": "🔥", "CAR": "🌪️",
    "CHI": "🦅", "COL": "⛰️", "CBJ": "💣", "DAL": "⭐️", "DET": "🔴", "EDM": "🛢️", "FLA": "🐆",
    "LAK": "👑", "MIN": "🌲", "MTL": "🇨🇦", "NJD": "😈", "NSH": "🐯", "NYI": "🟠", "NYR": "🗽",
    "OTT": "🛡", "PHI": "🛩", "PIT": "🐧", "SJS": "🦈", "SEA": "🦑", "STL": "🎵", "TBL": "⚡",
    "TOR": "🍁", "VAN": "🐳", "VGK": "🎰", "WSH": "🦅", "WPG": "✈️",
}

# Жёсткая мапа слагов sports.ru по аббревиатурам NHL
SPORTSRU_SLUG_BY_ABBR = {
    "ANA": "anaheim-ducks", "ARI": "arizona-coyotes", "UTA": "utah-hc",
    "BOS": "boston-bruins", "BUF": "buffalo-sabres", "CGY": "calgary-flames",
    "CAR": "carolina-hurricanes", "CHI": "chicago-blackhawks", "COL": "colorado-avalanche",
    "CBJ": "columbus-blue-jackets", "DAL": "dallas-stars", "DET": "detroit-red-wings",
    "EDM": "edmonton-oilers", "FLA": "florida-panthers", "LAK": "los-angeles-kings",
    "MIN": "minnesota-wild", "MTL": "montreal-canadiens", "NJD": "new-jersey-devils",
    "NSH": "nashville-predators", "NYI": "new-york-islanders", "NYR": "new-york-rangers",
    "OTT": "ottawa-senators", "PHI": "philadelphia-flyers", "PIT": "pittsburgh-penguins",
    "SJS": "san-jose-sharks", "SEA": "seattle-kraken", "STL": "st-louis-blues",
    "TBL": "tampa-bay-lightning", "TOR": "toronto-maple-leafs", "VAN": "vancouver-canucks",
    "VGK": "vegas-golden-knights", "WSH": "washington-capitals", "WPG": "winnipeg-jets",
}

# Кастомные эмодзи (если заданы)
try:
    if os.getenv("TEAM_EMOJI_JSON"):
        DEFAULT_EMOJI.update(json.loads(os.getenv("TEAM_EMOJI_JSON")))
except Exception as e:
    err("TEAM_EMOJI_JSON parse error:", repr(e))

# ----------------------- УТИЛИТЫ -----------------------

def ru_date(d: datetime) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def http_get(url: str, timeout: int = 20) -> requests.Response:
    dbg("GET", url)
    r = requests.get(url, headers=HEADERS_WEB, timeout=timeout)
    r.raise_for_status()
    return r

def http_json(url: str) -> Dict[str, Any]:
    return http_get(url).json()

def _val(x):
    if isinstance(x, dict):
        return x.get("default") or next((v for v in x.values() if isinstance(v, str) and v), "")
    return x or ""

def msk_window_for_date(base_date: datetime.date) -> Tuple[datetime, datetime]:
    wnd_start = datetime(base_date.year, base_date.month, base_date.day, 15, 0, 0, tzinfo=MSK) - timedelta(days=1)
    wnd_end   = datetime(base_date.year, base_date.month, base_date.day, 23, 59, 59, tzinfo=MSK)
    return wnd_start, wnd_end

# ----------------------- SPORTS.RU ПАРСЕР -----------------------

# время «mm.ss» (в ленте sports.ru часто абсолютное или периодное — нам без разницы, мы матчим только по mm:ss)
_TIME_RE = re.compile(r'\b([0-9]{1,3})[.:]([0-5]\d)\b')
_GOAL_WORD = re.compile(r'\bГол\b', re.I)
_ASSISTS_RE = re.compile(r'Ассистент(?:ы)?\s*:?\s*([А-ЯA-ZЁ][^()\n\r]+)', re.I)
_NAME_PAREN_RE = re.compile(r'([А-ЯЁ][А-Яа-яЁё \-’\'`]+?)\s*\(([^)]+)\)')

# не выбрасываем строки с ассистентами/скобками
_TRASH_WORDS = re.compile(
    r'\b(Видео|в сезоне|сэйв|штраф|травм|новост|БОЛЬШЕ НОВОСТЕЙ)\b',
    re.I
)

_CYR_TOKEN = re.compile(r"[А-ЯЁ][а-яё]+(?:[-ʼ’'][А-ЯЁ][а-яё]+)*")

def _ru_lastname_only(s: str) -> str:
    toks = _CYR_TOKEN.findall(s or "")
    return toks[-1] if toks else ""

def _ru_names_list(s: str) -> List[str]:
    raw = (s or "").replace("Ассистент:", "").replace("Ассистенты:", "")
    parts = [p.strip() for p in re.split(r"[;,]", raw) if p.strip()]
    out: List[str] = []
    for p in parts:
        fam = _ru_lastname_only(p)
        if fam:
            out.append(fam)
    seen = set(); uniq: List[str] = []
    for x in out:
        if x not in seen:
            uniq.append(x); seen.add(x)
    return uniq

def parse_sportsru_goals(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    live_candidates = []
    for sel in [".live-block", ".match__live", ".transText", ".live", ".live-feed",
                ".material-body", ".article__content", ".match__text", ".match__events"]:
        live_candidates += soup.select(sel)
    text = "\n".join(el.get_text("\n", strip=True) for el in live_candidates) or soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    events: List[Dict[str, Any]] = []
    for i, line in enumerate(lines):
        tm = _TIME_RE.search(line)
        if not tm:
            continue

        # окно контекста вокруг времени (назад/вперёд)
        ctx = " ".join(lines[max(0, i-2): i+5])

        # защитимся от совсем нерелевантного мусора
        if _TRASH_WORDS.search(ctx) and not (_GOAL_WORD.search(ctx) or _ASSISTS_RE.search(ctx) or _NAME_PAREN_RE.search(ctx)):
            continue

        # фамилия + ассистенты в скобках (самый частый формат)
        m_np = _NAME_PAREN_RE.search(ctx)
        author_last, assists = "", []
        if m_np:
            author_last = _ru_lastname_only(m_np.group(1))
            assists = _ru_names_list(m_np.group(2))
        else:
            # «Ассистенты: …» без слова «Гол»
            m_ast = _ASSISTS_RE.search(ctx)
            if m_ast:
                assists = _ru_names_list(m_ast.group(1))
            # фамилия сразу после времени или на следующей строке
            if not author_last:
                nxt = (lines[i+1] if i+1 < len(lines) else "")
                author_last = _ru_lastname_only(nxt) or _ru_lastname_only(line)

        if not (author_last or assists or _GOAL_WORD.search(ctx)):
            continue

        # время в периоде — оставляем как "mm:ss"
        mm, ss = int(tm.group(1)), int(tm.group(2))
        mmss = f"{mm:02d}:{ss:02d}"

        # период определить из абсолютных минут невозможно достоверно → доверяем матчеру по дельте времени
        events.append({
            "period": 0,           # будет определён на стадии сопоставления
            "mmss": mmss,          # время для матчинга
            "scorer": author_last or "",
            "assists": assists,
        })

    # удалим дубль-таймкоды с преимуществом записей с автором
    by_time: Dict[str, Dict[str, Any]] = {}
    for ev in events:
        key = ev["mmss"]
        cur = by_time.get(key)
        if (not cur) or (ev.get("scorer") and not cur.get("scorer")):
            by_time[key] = ev
    return list(by_time.values())

def attach_ru_names_to_nhl_goals(nhl_goals: List[Dict[str, Any]], sr_goals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Сопоставляем по (period, mm:ss) с допуском 4/7/10/15 секунд. Если period у sports.ru неизвестен, матчим только по времени."""
    def to_sec(mmss: str) -> int:
        mm, ss = mmss.split(":")
        return int(mm)*60 + int(ss)

    # индекс sports.ru по времени (без учёта периода)
    sr_by_sec = {}
    for ev in sr_goals:
        sr_by_sec.setdefault(to_sec(ev["mmss"]), []).append(ev)

    tolerances = (4, 7, 10, 15)

    out = []
    used = set()  # (sec, idx)
    for g in nhl_goals:
        tgt = to_sec(g["mmss"])
        pick = None

        # точный по времени
        for tol in tolerances:
            for sec in range(tgt - tol, tgt + tol + 1):
                cand_list = sr_by_sec.get(sec) or []
                for idx, ev in enumerate(cand_list):
                    key = (sec, idx)
                    if key in used:
                        continue
                    # если у нас по NHL известен период, а в ev период неизвестен — позволяем матч
                    pick = ev
                    used.add(key)
                    break
                if pick:
                    break
            if pick:
                break

        g2 = dict(g)
        if pick:
            if pick.get("scorer"):
                g2["ru_scorer"] = pick["scorer"]
            else:
                g2["ru_scorer"] = "—"
            g2["ru_assists"] = pick.get("assists") or []
        else:
            g2.setdefault("ru_scorer", "—")
            g2.setdefault("ru_assists", [])
        out.append(g2)

    return out

# ----------------------- NHL API -----------------------

def fetch_schedule(day: datetime.date) -> List[Dict[str, Any]]:
    url = f"https://api-web.nhle.com/v1/schedule/{day.isoformat()}"
    j = http_json(url)
    out = []
    for gd in j.get("games", []) or []:
        out.append(gd)
    for wk in j.get("gameWeek", []) or []:
        for gd in wk.get("games", []) or []:
            out.append(gd)
    by_id = {}
    for gd in out:
        gid = gd.get("id") or gd.get("gamePk") or gd.get("gameNumber") or gd.get("gameId")
        if gid is not None:
            by_id[gid] = gd
    return list(by_id.values())

def game_start_msk(g: Dict[str, Any]) -> Optional[datetime]:
    ts = g.get("startTimeUTC") or g.get("startTimeUTCDate") or g.get("startTimeUTCFormatted")
    if not ts:
        return None
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts).astimezone(MSK)
    except Exception:
        return None

def game_state(g: Dict[str, Any]) -> str:
    return str(g.get("gameState") or g.get("gameStatus", {}).get("state") or "").upper()

def team_info(g: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return (g.get("homeTeam") or {}), (g.get("awayTeam") or {})

def filter_completed_in_window(games: List[Dict[str, Any]], start_msk: datetime, end_msk: datetime) -> List[Dict[str, Any]]:
    picked = []
    for g in games:
        st = game_state(g)
        if st not in COMPLETE_STATES:
            continue
        dt = game_start_msk(g)
        if not dt:
            continue
        if start_msk <= dt <= end_msk:
            picked.append(g)
    return picked

def fetch_pbp(game_id: int) -> Dict[str, Any]:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    return http_json(url)

def extract_goals_from_pbp(pbp: Dict[str, Any], home_id: int, away_id: int) -> Tuple[List[Dict[str, Any]], bool]:
    plays = pbp.get("plays") or []
    goals: List[Dict[str, Any]] = []
    home = 0; away = 0
    shootout = False

    for p in plays:
        ty = (p.get("typeDescKey") or p.get("typeCode") or "").lower()
        period = int((p.get("periodDescriptor") or {}).get("number") or 0)
        ptype  = ((p.get("periodDescriptor") or {}).get("periodType") or "").upper()
        time_str = p.get("timeInPeriod") or p.get("timeRemaining") or "00:00"
        team_id = (p.get("details") or {}).get("eventOwnerTeamId") or p.get("teamId") or (p.get("details") or {}).get("teamId")

        if ptype == "SHOOTOUT":
            shootout = True
            continue

        if ty == "goal" or ty == "goalie_goal":
            if team_id == home_id:
                home += 1; side = "HOME"
            elif team_id == away_id:
                away += 1; side = "AWAY"
            else:
                continue

            if ":" not in time_str and "." in time_str:
                time_str = time_str.replace(".", ":")
            mm, ss = time_str.split(":")
            time_str = f"{int(mm):02d}:{int(ss):02d}"

            goals.append({
                "period": period or (4 if ptype.startswith("OT") else 3),
                "periodType": ptype or ("REG" if period <= 3 else "OT"),
                "mmss": time_str,
                "side": side,
                "home": home,
                "away": away,
            })

    return goals, shootout

# ----------------------- СТАТЫ -----------------------

def fetch_records(date_hint: Optional[datetime.date] = None) -> Dict[str, Dict[str, int]]:
    urls = ["https://api-web.nhle.com/v1/standings/now"]
    if date_hint:
        urls.append(f"https://api-web.nhle.com/v1/standings/{date_hint.isoformat()}")

    for url in urls:
        try:
            j = http_json(url)
            tbl = {}
            arr = j.get("standings") or j.get("records") or []
            for row in arr:
                abbr_raw = row.get("teamAbbrev") or row.get("teamAbbrevDefault") or (row.get("team") or {}).get("abbrev")
                abbr = (_val(abbr_raw) or "").upper()
                if not abbr:
                    continue
                wins = int(row.get("wins", 0))
                loss = int(row.get("losses", 0))
                otl  = int(row.get("otLosses", 0))
                pts  = int(row.get("points", row.get("pts", 0)))
                tbl[abbr] = {"w": wins, "l": loss, "ot": otl, "pts": pts}
            if tbl:
                dbg("records loaded:", len(tbl))
                return tbl
            dbg("records empty from", url)
        except Exception as e:
            dbg("records error:", repr(e))
    return {}

# ----------------------- SPORTS.RU МАТЧ -----------------------

def sportsru_try_fetch(slug: str) -> List[Dict[str, Any]]:
    for suffix in ("/lineups/", "/"):
        url = f"https://www.sports.ru/hockey/match/{slug}{suffix}"
        try:
            html = http_get(url).text
            goals = parse_sportsru_goals(html)
            if goals:
                dbg(f"sports.ru goals for {slug}: {len(goals)}")
                return goals
        except Exception:
            pass
    return []

def _slugify_city(team: Dict[str, Any]) -> str:
    city = _val(team.get("placeName")) or _val(team.get("city")) or _val(team.get("name")) or ""
    s = city.lower()
    s = s.replace("st. ", "st-").replace("st ", "st-")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s

def sportsru_match_goals(home_team: Dict[str, Any], away_team: Dict[str, Any]) -> List[Dict[str, Any]]:
    home_abbr = (_val(home_team.get("abbrev")) or "").upper()
    away_abbr = (_val(away_team.get("abbrev")) or "").upper()

    tried: List[str] = []
    def add_pair(h: str, a: str):
        tried.append(f"{h}-vs-{a}")
        tried.append(f"{a}-vs-{h}")

    h1 = SPORTSRU_SLUG_BY_ABBR.get(home_abbr)
    a1 = SPORTSRU_SLUG_BY_ABBR.get(away_abbr)
    if h1 and a1:
        add_pair(h1, a1)

    h2 = _slugify_city(home_team)
    a2 = _slugify_city(away_team)
    if h2 and a2:
        add_pair(h2, a2)

    dbg("sports.ru slugs tried:", tried)

    for slug in tried:
        goals = sportsru_try_fetch(slug)
        if goals:
            return goals
    return []

# ----------------------- ФОРМАТ -----------------------

def team_emoji(abbr: str) -> str:
    return DEFAULT_EMOJI.get(abbr, "🏒")

def team_ru(abbr: str) -> str:
    return RU_TEAMS.get(abbr, abbr)

def format_record(rec: Optional[Dict[str, int]]) -> str:
    return "" if not rec else f" ({rec['w']}-{rec['l']}-{rec['ot']}, {rec['pts']} о.)"

def period_header(period: int, ptype: str) -> str:
    if ptype == "SHOOTOUT":
        return "Буллиты"
    if period <= 3:
        return f"{period}-й период"
    return f"Овертайм №{period-3}"

def _fmt_mmdotss(mmss: str) -> str:
    mm, ss = mmss.split(":")
    return f"{int(mm):02d}.{int(ss):02d}"

def format_goal_line(g: Dict[str, Any]) -> str:
    assists = g.get("ru_assists") or []
    ast_txt = f" ({', '.join(assists)})" if assists else ""
    scorer = g.get("ru_scorer") or "—"
    return f"{g['home']}:{g['away']} – {_fmt_mmdotss(g['mmss'])} {scorer}{ast_txt}"

def build_message(base_date: datetime.date,
                  games: List[Dict[str, Any]],
                  records: Dict[str, Dict[str, int]]) -> str:
    lines: List[str] = []
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(datetime(base_date.year, base_date.month, base_date.day))} • {len(games)} матчей"
    lines += [title, "", "Результаты надёжно спрятаны 👇", "", "——————————————————", ""]

    for g in games:
        gid = g.get("id") or g.get("gamePk") or g.get("gameNumber")
        home, away = team_info(g)
        home_abbr = (_val(home.get("abbrev")) or "").upper()
        away_abbr = (_val(away.get("abbrev")) or "").upper()
        home_id = int(home.get("id") or 0)
        away_id = int(away.get("id") or 0)

        final_home = int(home.get("score") or 0)
        final_away = int(away.get("score") or 0)

        lines.append(f"{team_emoji(home_abbr)} «{team_ru(home_abbr)}»: {final_home}{format_record(records.get(home_abbr))}")
        lines.append(f"{team_emoji(away_abbr)} «{team_ru(away_abbr)}»: {final_away}{format_record(records.get(away_abbr))}")
        lines.append("")

        try:
            pbp = fetch_pbp(int(gid))
        except Exception as e:
            err("pbp fetch fail", gid, repr(e))
            lines.append("— события матча недоступны\n")
            continue

        goals, was_shootout = extract_goals_from_pbp(pbp, home_id, away_id)

        sr_goals = []
        try:
            sr_goals = sportsru_match_goals(home, away)
        except Exception as e:
            dbg("sports.ru parse fail:", repr(e))

        if sr_goals:
            goals = attach_ru_names_to_nhl_goals(goals, sr_goals)
        else:
            for gg in goals:
                gg.setdefault("ru_scorer", "—")
                gg.setdefault("ru_assists", [])

        if not goals and not was_shootout:
            lines.append("— события матча недоступны\n")
            continue

        grouped: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
        for gg in goals:
            key = (gg["period"], gg["periodType"])
            grouped.setdefault(key, []).append(gg)

        for key in sorted(grouped.keys()):
            pnum, ptype = key
            lines.append(f"{period_header(pnum, ptype)}")
            for gg in grouped[key]:
                lines.append(format_goal_line(gg))
            lines.append("")

    return "\n".join(lines).strip() + "\n"

# ----------------------- TELEGRAM -----------------------

def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT:
        dbg("Telegram env not set; output follows:\n" + text)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    dbg("POST Telegram sendMessage")
    r = requests.post(url, json=payload, timeout=20)
    if r.status_code != 200:
        raise requests.HTTPError(r.text)

# ----------------------- MAIN -----------------------

def main():
    force = os.getenv("REPORT_DATE_MSK", "").strip()
    if force:
        try:
            base_date = datetime.fromisoformat(force).date()
        except Exception:
            err("REPORT_DATE_MSK must be YYYY-MM-DD"); sys.exit(1)
    else:
        base_date = datetime.now(MSK).date()

    wnd_start, wnd_end = msk_window_for_date(base_date)
    dbg("MSK window:", wnd_start.isoformat(), "→", wnd_end.isoformat())

    schedules: List[Dict[str, Any]] = []
    for d in [base_date - timedelta(days=1), base_date, base_date + timedelta(days=1)]:
        try:
            schedules += fetch_schedule(d)
        except Exception as e:
            err("schedule fetch fail", d, repr(e))

    completed = filter_completed_in_window(schedules, wnd_start, wnd_end)
    dbg("Collected unique FINAL games:", len(completed))
    completed.sort(key=lambda g: game_start_msk(g) or wnd_start)

    records = fetch_records(base_date if completed else None)

    msg = build_message(base_date, completed, records)

    try:
        tg_send(msg)
        dbg("Telegram OK")
    except Exception as e:
        err(repr(e))
        print(msg)
        sys.exit(1 if TG_TOKEN and TG_CHAT else 0)

if __name__ == "__main__":
    main()
