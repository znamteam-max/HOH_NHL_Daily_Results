# -*- coding: utf-8 -*-
"""
NHL Daily Results → Telegram (RU)
- Берём завершённые матчи NHL за MSK-день: от D-1 15:00 до D 23:59 (MSK)
- Голы и счёт — из NHL PBP (надёжно)
- Имена (кириллица) — из sports.ru "Трансляции" (только строки с "Гол")
- Если на sports.ru нет трансляции, оставляем прочерк вместо фамилий, счёт остаётся верный
- Рекорды и очки — из NHL /v1/standings/now (с фолбэком по дате)
- Сообщение — HTML (без Markdown ловушек)
Env:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  — обязательно
  TEAM_EMOJI_JSON (опц.) — JSON-словарь { "VGK":"🎰", ... }
  REPORT_DATE_MSK (опц.) — YYYY-MM-DD, иначе берём "сегодня" по MSK
"""

import os
import sys
import json
import time
import re
from datetime import datetime, timedelta, timezone
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

COMPLETE_STATES = {"OFF", "FINAL", "COMPLETED", "END"}
LIVE_OR_FUTURE = {"FUT", "LIVE", "PRE", "CRIT"}

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not TG_TOKEN or not TG_CHAT:
    err("TELEGRAM_* env vars are missing")
    # не выходим: позволим локальный прогон без отправки

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (compatible; NHLBot/1.0; +https://example.local)"
}

RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

RU_TEAMS = {
    "ANA": "Анахайм",
    "ARI": "Аризона",   # исторически
    "UTA": "Юта",
    "BOS": "Бостон",
    "BUF": "Баффало",
    "CGY": "Калгари",
    "CAR": "Каролина",
    "CHI": "Чикаго",
    "COL": "Колорадо",
    "CBJ": "Коламбус",
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
    "SJS": "Сан-Хосе",
    "SEA": "Сиэтл",
    "STL": "Сент-Луис",
    "TBL": "Тампа-Бэй",
    "TOR": "Торонто",
    "VAN": "Ванкувер",
    "VGK": "Вегас",
    "WSH": "Вашингтон",
    "WPG": "Виннипег",
}

DEFAULT_EMOJI = {
    "ANA": "🦆", "UTA": "🦣", "ARI": "🦂",
    "BOS": "🐻", "BUF": "🦬", "CGY": "🔥", "CAR": "🌪️",
    "CHI": "🦅", "COL": "⛰️", "CBJ": "💣", "DAL": "⭐",
    "DET": "🔴", "EDM": "🛢️", "FLA": "🐆", "LAK": "👑",
    "MIN": "🌲", "MTL": "🇨🇦", "NJD": "😈", "NSH": "🐯",
    "NYI": "🟠", "NYR": "🗽", "OTT": "🛡", "PHI": "🛩",
    "PIT": "🐧", "SJS": "🦈", "SEA": "🦑", "STL": "🎵",
    "TBL": "⚡", "TOR": "🍁", "VAN": "🐳", "VGK": "🎰",
    "WSH": "🦅", "WPG": "✈️",
}

# Подгрузить кастомные эмодзи (если заданы)
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

def slugify_team_name(name: str) -> str:
    # нормализуем под sports.ru slug
    s = name.lower()
    repl = {
        "st. ": "st-",
        "st ": "st-",
        "é": "e",
        "è": "e",
        "á": "a",
        "à": "a",
        "ó": "o",
        "ö": "o",
        "ü": "u",
        "í": "i",
        "î": "i",
        "â": "a",
        "ç": "c",
        "ñ": "n",
        "š": "s",
        "ž": "z",
        "’": "-", "‘": "-", "“": "-", "”": "-",
        "'": "-", "&": " and ",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s

def msk_window_for_date(base_date: datetime.date) -> Tuple[datetime, datetime]:
    """
    Окно для поста D:
      [D-1 15:00 MSK, D 23:59:59 MSK]
    """
    start = datetime(base_date.year, base_date.month, base_date.day, 23, 59, 59, tzinfo=MSK)
    start = start.replace(day=base_date.day)  # просто для читаемости
    wnd_start = datetime(base_date.year, base_date.month, base_date.day, 23, 59, 59, tzinfo=MSK)
    wnd_start = wnd_start.replace(hour=15, minute=0, second=0) - timedelta(days=1)
    wnd_end = datetime(base_date.year, base_date.month, base_date.day, 23, 59, 59, tzinfo=MSK)
    return wnd_start, wnd_end

def parse_iso_utc(s: str) -> datetime:
    # NHL API: "2025-11-01T02:00:00Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

# ----------------------- SPORTS.RU ПАРСЕР -----------------------

_TIME_RE = re.compile(r'\b([0-5]?\d)[\.:]([0-5]\d)\b')
_GOAL_FLAG_RE = re.compile(r'\bГол!?', re.I)
_ASSISTS_RE = re.compile(r'Ассистент(?:ы)?\s*:?\s*([А-ЯA-ZЁ][^()\n\r]+)', re.I)
_TRASH_WORDS = re.compile(
    r'\b(Удалени[ея]|Перерыв|Видео|в сезоне|сэйв|штраф|травм|новост|БОЛЬШЕ НОВОСТЕЙ|Овертайм|Овер|фол|блокир)\b',
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
    # порядок, без дублей
    seen = set(); uniq: List[str] = []
    for x in out:
        if x not in seen:
            uniq.append(x); seen.add(x)
    return uniq

def parse_sportsru_goals(html: str) -> List[Tuple[str, str, List[str]]]:
    """
    -> [(mm.ss, 'Фамилия', ['Фамилия', ...])]
    Берём только окна, где рядом с временем есть 'Гол', и выбрасываем мусор.
    """
    soup = BeautifulSoup(html, "html.parser")
    live_candidates = []
    for sel in [".live-block", ".match__live", ".transText", ".live", ".live-feed", ".material-body", ".article__content"]:
        live_candidates += soup.select(sel)
    text = "\n".join(el.get_text("\n", strip=True) for el in live_candidates) or soup.get_text("\n", strip=True)

    # иногда в тексте дублируются блоки — норм
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    events: List[Tuple[str, str, List[str]]] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        tm = _TIME_RE.search(line)
        if not tm:
            i += 1
            continue

        mm, ss = tm.group(1).zfill(2), tm.group(2).zfill(2)
        mmss = f"{mm}.{ss}"
        window = " ".join(lines[i:i+5])

        if _TRASH_WORDS.search(window):
            i += 1
            continue

        if not _GOAL_FLAG_RE.search(window):
            i += 1
            continue

        author_last = ""
        m_goal_after = re.search(r"Гол!?[^А-ЯЁ]*([А-ЯЁ][^,\(\)\n\r]+)", window, re.I)
        if m_goal_after:
            author_last = _ru_lastname_only(m_goal_after.group(1))
        if not author_last and i+1 < len(lines):
            author_last = _ru_lastname_only(lines[i+1])

        assists: List[str] = []
        m_ast = _ASSISTS_RE.search(window)
        if m_ast:
            assists = _ru_names_list(m_ast.group(1))

        if not author_last:
            i += 1
            continue

        events.append((mmss, author_last, assists))
        i += 1

    # уникальность по времени
    seen = set()
    uniq: List[Tuple[str, str, List[str]]] = []
    for mmss, a, ast in events:
        if mmss in seen:
            continue
        seen.add(mmss)
        uniq.append((mmss, a, ast))
    return uniq

def attach_ru_names_to_nhl_goals(nhl_goals: List[Dict[str, Any]], sr_goals: List[Tuple[str, str, List[str]]]) -> List[Dict[str, Any]]:
    """
    nhl_goals: [{ 'period':1.., 'mmss':'12:34', 'side':'HOME'/'AWAY', 'home':int, 'away':int }]
    sr_goals:  [( '12.34', 'Фамилия', ['Фамилия', ...])]

    Сопоставляем по времени (точно или +-4 сек).
    """
    def to_sec_dot(mmss_dot: str) -> int:
        mm, ss = mmss_dot.split(".")
        return int(mm)*60 + int(ss)

    sr = [(to_sec_dot(t), fam, ast) for (t, fam, ast) in sr_goals]
    used = set()

    out: List[Dict[str, Any]] = []
    for g in nhl_goals:
        mm, ss = g["mmss"].split(":")
        t = int(mm)*60 + int(ss)

        pick = None
        # точное
        for idx, (s, fam, ast) in enumerate(sr):
            if idx in used:
                continue
            if s == t:
                pick = (idx, fam, ast)
                break
        # ближайшее
        if not pick:
            best = None; best_d = 999
            for idx, (s, fam, ast) in enumerate(sr):
                if idx in used:
                    continue
                d = abs(s - t)
                if d < best_d and d <= 4:
                    best_d = d; best = (idx, fam, ast)
            if best:
                pick = best

        g2 = dict(g)
        if pick:
            idx, fam, ast = pick
            used.add(idx)
            g2["ru_scorer"]  = fam
            g2["ru_assists"] = ast
        else:
            g2["ru_scorer"]  = "—"
            g2["ru_assists"] = []
        out.append(g2)
    return out

# ----------------------- NHL API -----------------------

def fetch_schedule(days: List[datetime.date]) -> List[Dict[str, Any]]:
    out = []
    for d in days:
        url = f"https://api-web.nhle.com/v1/schedule/{d.isoformat()}"
        r = http_get(url)
        j = r.json()
        for g in j.get("gameWeek", []):
            for day in g.get("games", []):
                # В альтернативной структуре
                pass
        # новый формат (stable на 2024+/2025+):
        for game in j.get("gameWeek", []) or []:
            # иногда вложено, поэтому продублируем обход
            for gd in game.get("games", []) or []:
                out.append(gd)
        # fallback — прямой массив "games"
        for gd in j.get("games", []) or []:
            out.append(gd)
    # удалить дубли по gamePk / id
    by_id = {}
    for gd in out:
        gid = gd.get("id") or gd.get("gamePk") or gd.get("gameId") or gd.get("gameNumber")
        if gid is not None:
            by_id[gid] = gd
    return list(by_id.values())

def game_start_msk(g: Dict[str, Any]) -> Optional[datetime]:
    ts = g.get("startTimeUTC") or g.get("startTimeUTCFormatted") or g.get("startTimeUTCDate")
    if not ts:
        return None
    try:
        dt = parse_iso_utc(ts).astimezone(MSK)
        return dt
    except Exception:
        return None

def game_state(g: Dict[str, Any]) -> str:
    st = g.get("gameState") or g.get("gameStatus", {}).get("state")
    return str(st or "").upper()

def team_info(g: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    home = g.get("homeTeam") or {}
    away = g.get("awayTeam") or {}
    return home, away

def filter_completed_in_window(games: List[Dict[str, Any]], start_msk: datetime, end_msk: datetime) -> List[Dict[str, Any]]:
    picked = []
    for g in games:
        st = game_state(g)
        if st not in COMPLETE_STATES and st != "OFF":
            # пропускаем FUT/LIVE/PRE
            continue
        dt = game_start_msk(g)
        if not dt:
            continue
        if start_msk <= dt <= end_msk:
            picked.append(g)
    return picked

def fetch_pbp(game_id: int) -> Dict[str, Any]:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    r = http_get(url)
    return r.json()

def extract_goals_from_pbp(pbp: Dict[str, Any], home_id: int, away_id: int) -> Tuple[List[Dict[str, Any]], bool, Optional[int]]:
    """
    Возвращает:
      goals = [ {period:int, periodType:str, mmss:'MM:SS', side:'HOME'|'AWAY', home:int, away:int} ... ]
      shootout: bool
      decisive_shooter_playerId (если удалось определить победный буллит)
    """
    plays = pbp.get("plays") or []
    goals: List[Dict[str, Any]] = []
    home = 0; away = 0
    shootout = False
    shootout_entries = []

    for p in plays:
        ty = (p.get("typeDescKey") or p.get("typeCode") or "").lower()
        period = (p.get("periodDescriptor") or {}).get("number") or 0
        ptype  = ((p.get("periodDescriptor") or {}).get("periodType") or "").upper()
        time_str = p.get("timeInPeriod") or p.get("timeRemaining") or "00:00"
        team_id = (p.get("details") or {}).get("eventOwnerTeamId") or p.get("teamId") or (p.get("details") or {}).get("teamId")

        if ptype == "SHOOTOUT":
            shootout = True
            # в деталях бывают поля shooterId / shotResult / gameWinningGoal
            if (p.get("details") or {}).get("shotResult") == "GOAL":
                shootout_entries.append(p)
            continue

        if ty == "goal" or ty == "goalie_goal":
            if team_id == home_id:
                home += 1
                side = "HOME"
            elif team_id == away_id:
                away += 1
                side = "AWAY"
            else:
                # если не совпал team_id — аккуратно пропустим
                continue

            goals.append({
                "period": int(period),
                "periodType": ptype or ("REG" if int(period) <= 3 else "OT"),
                "mmss": time_str if ":" in time_str else time_str.replace(".", ":"),
                "side": side,
                "home": home,
                "away": away,
            })

    decisive = None
    # Попробуем найти победный буллит — последний GOAL в серии за победившую команду
    if shootout and shootout_entries:
        last = shootout_entries[-1]
        decisive = (last.get("details") or {}).get("shooterId") or (last.get("details") or {}).get("scoringPlayerId")

    return goals, shootout, decisive

# ----------------------- СТАТИСТИКА/ТАБЛИЦЫ -----------------------

def fetch_records(date_hint: Optional[datetime.date] = None) -> Dict[str, Dict[str, int]]:
    """
    Возвращает { 'VGK': {'w':6,'l':2,'ot':3,'pts':15}, ... }
    """
    urls = ["https://api-web.nhle.com/v1/standings/now"]
    if date_hint:
        urls.append(f"https://api-web.nhle.com/v1/standings/{date_hint.isoformat()}")

    for url in urls:
        try:
            r = http_get(url)
            j = r.json()
            tbl = {}
            # формат: j['standings'][...] или иной
            arr = j.get("standings") or j.get("records") or []
            for row in arr:
                abbr = (row.get("teamAbbrev") or row.get("teamAbbrevDefault") or row.get("team", {}).get("abbrev") or "").upper()
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
            else:
                dbg("records empty from", url)
        except Exception as e:
            dbg("records error:", repr(e))
    return {}

# ----------------------- СПОРТС.РУ МАТЧ-СТРАНИЦА -----------------------

def sportsru_match_goals(home_name_en: str, away_name_en: str) -> List[Tuple[str, str, List[str]]]:
    """
    Пытаемся найти страницу матча на sports.ru по slug'ам команд.
    Возвращаем список голов [(mm.ss, 'Фамилия', ['Фамилия', ...]), ...]
    """
    hs = slugify_team_name(home_name_en)
    as_ = slugify_team_name(away_name_en)
    tried = [
        f"{home_name_en}".lower(),  # заглушка (не используется)
    ]
    slugs = [
        f"{away_name_en}",
        f"{home_name_en}",
    ]
    # правильные варианты:
    candidates = [
        f"{as_}-vs-{hs}",
        f"{hs}-vs-{as_}",
    ]
    dbg("sports.ru slugs tried:", candidates)
    for slug in candidates:
        for suffix in ("/lineups/", "/"):
            url = f"https://www.sports.ru/hockey/match/{slug}{suffix}"
            try:
                r = http_get(url)
                html = r.text
                goals = parse_sportsru_goals(html)
                if goals:
                    dbg(f"sports.ru goals for {slug}: {len(goals)}")
                    return goals
            except Exception as e:
                # просто продолжаем
                pass
    return []

# ----------------------- ФОРМАТИРОВАНИЕ -----------------------

def team_emoji(abbr: str) -> str:
    return DEFAULT_EMOJI.get(abbr, "🏒")

def team_ru(abbr: str) -> str:
    return RU_TEAMS.get(abbr, abbr)

def format_record(rec: Optional[Dict[str, int]]) -> str:
    if not rec:
        return ""
    return f" ({rec['w']}-{rec['l']}-{rec['ot']}, {rec['pts']} о.)"

def format_goal_line(g: Dict[str, Any]) -> str:
    # "H:A – mm.ss Фамилия (Ассистент1, Ассистент2)"
    mm, ss = g["mmss"].split(":")
    mmss_dot = f"{mm}.{ss}"
    assists = g.get("ru_assists") or []
    if assists:
        ast_txt = " (" + ", ".join(assists) + ")"
    else:
        ast_txt = ""
    scorer = g.get("ru_scorer") or "—"
    return f"{g['home']}:{g['away']} – {mmss_dot} {scorer}{ast_txt}"

def period_header(period: int, ptype: str) -> str:
    if ptype == "SHOOTOUT":
        return "Буллиты"
    if period <= 3:
        return f"{period}-й период"
    else:
        # OT №(period-3)
        return f"Овертайм №{period-3}"

def build_message(base_date: datetime.date,
                  games: List[Dict[str, Any]],
                  records: Dict[str, Dict[str, int]]) -> str:
    lines: List[str] = []
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(datetime(base_date.year, base_date.month, base_date.day))} • {len(games)} матчей"
    lines.append(title)
    lines.append("")
    lines.append("Результаты надёжно спрятаны 👇")
    lines.append("")
    lines.append("——————————————————")
    lines.append("")

    for g in games:
        gid = g.get("id") or g.get("gamePk") or g.get("gameNumber")
        home, away = team_info(g)
        home_abbr = (home.get("abbrev") or home.get("teamAbbrev") or "").upper()
        away_abbr = (away.get("abbrev") or away.get("teamAbbrev") or "").upper()
        home_id = int(home.get("id") or 0)
        away_id = int(away.get("id") or 0)
        home_name_en = home.get("name") or home.get("teamName") or home_abbr
        away_name_en = away.get("name") or away.get("teamName") or away_abbr

        # финальный счёт:
        final_home = int(home.get("score") or 0)
        final_away = int(away.get("score") or 0)

        # строка заголовка матча
        hline = f"{team_emoji(home_abbr)} «{team_ru(home_abbr)}»: {final_home}{format_record(records.get(home_abbr))}"
        aline = f"{team_emoji(away_abbr)} «{team_ru(away_abbr)}»: {final_away}{format_record(records.get(away_abbr))}"
        lines.append(hline)
        lines.append(aline)
        lines.append("")

        # PBP → цели
        pbp = {}
        try:
            pbp = fetch_pbp(int(gid))
        except Exception as e:
            err("pbp fetch fail", gid, repr(e))
            lines.append("— события матча недоступны")
            lines.append("")
            continue

        goals, was_shootout, decisive_player = extract_goals_from_pbp(pbp, home_id, away_id)

        # sports.ru имена
        sr_goals: List[Tuple[str, str, List[str]]] = []
        try:
            sr_goals = sportsru_match_goals(home_name_en, away_name_en)
        except Exception as e:
            dbg("sports.ru parse fail:", repr(e))

        # сопоставим
        if sr_goals:
            goals = attach_ru_names_to_nhl_goals(goals, sr_goals)
        else:
            # нет трансляции — имен не будет, но счёт останется верный
            for gg in goals:
                gg.setdefault("ru_scorer", "—")
                gg.setdefault("ru_assists", [])

        if not goals and not was_shootout:
            lines.append("— события матча недоступны")
            lines.append("")
            continue

        # группировка по периодам
        grouped: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
        for gg in goals:
            key = (gg["period"], gg["periodType"])
            grouped.setdefault(key, []).append(gg)

        for (pnum, ptype) in sorted(grouped.keys()):
            hdr = period_header(pnum, ptype)
            lines.append(f"<i>{hdr}</i>")
            for gg in grouped[(pnum, ptype)]:
                lines.append(format_goal_line(gg))
            lines.append("")

        if was_shootout:
            lines.append(f"<i>Буллиты</i>")
            # Если удалось вытащить победный буллит (по shooterId) — попробуем найти русскую фамилию
            win_txt = "победный буллит — "
            ru_name = "—"
            if decisive_player:
                # поиск в sr_goals по последнему совпадению времени уже не подойдёт
                # поэтому просто оставим прочерк; при желании можно дёрнуть championat.com для «автора»
                pass
            lines.append(win_txt + ru_name)
            lines.append("")

    # Склеиваем
    text = "\n".join(lines).strip() + "\n"
    return text

# ----------------------- TELEGRAM -----------------------

def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT:
        dbg("Telegram env not set, printing message:\n", text)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    dbg("POST Telegram sendMessage")
    r = requests.post(url, json=payload, timeout=20)
    if r.status_code != 200:
        raise requests.HTTPError(r.text)

# ----------------------- MAIN -----------------------

def main():
    # Базовая дата поста (MSK)
    force = os.getenv("REPORT_DATE_MSK", "").strip()
    if force:
        try:
            base_date = datetime.fromisoformat(force).date()
        except Exception:
            err("REPORT_DATE_MSK must be YYYY-MM-DD")
            sys.exit(1)
    else:
        base_date = datetime.now(MSK).date()

    wnd_start, wnd_end = msk_window_for_date(base_date)
    dbg("MSK window:", wnd_start.isoformat(), "→", wnd_end.isoformat())

    # Чтобы покрыть окно, качаем календарь за три дня: D-1, D, D+1 (UTC-разброс)
    days = [base_date - timedelta(days=1), base_date, base_date + timedelta(days=1)]
    schedules = []
    for d in days:
        try:
            schedules += fetch_schedule([d])
        except Exception as e:
            err("schedule fetch fail", d, repr(e))

    # фильтр завершённых в окне
    completed = filter_completed_in_window(schedules, wnd_start, wnd_end)
    dbg("Collected unique FINAL games:", len(completed))
    # сортируем по старту
    completed.sort(key=lambda g: game_start_msk(g) or datetime.min.replace(tzinfo=MSK))

    # records (фолбэк — по базовой дате)
    records = fetch_records(base_date if completed else None)

    # Сообщение
    msg = build_message(base_date, completed, records)

    # Отправка
    try:
        tg_send(msg)
        dbg("Telegram OK")
    except Exception as e:
        err(repr(e))
        # Падать не будем — полезно видеть текст в логах
        print(msg)
        sys.exit(1 if TG_TOKEN and TG_CHAT else 0)

if __name__ == "__main__":
    main()
