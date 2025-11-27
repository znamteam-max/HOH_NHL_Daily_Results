#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram (spoiler friendly, RU names if possible)

Основные фиксы:
- DRY_RUN безопасно инициализирован из ENV
- Эмодзи и названия команд в одном блоке, без обрыва строки
- Восстановлен показ имён авторов голов/ассистов
- Расширен генератор URL для sports.ru: Utah → `utah-mammoth`, Vegas → `vegas` (+fallback’и)
- Дом/гости на sports.ru могут меняться — пробуем обе ориентации и /stat

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID – куда слать
  REPORT_DATE_LOCAL – YYYY-MM-DD (опционально)
  REPORT_TZ – IANA TZ, по умолчанию Europe/Amsterdam
  DRY_RUN – "1" печатаем в логи, не отправляем
  DEBUG_VERBOSE – "1" расширенные логи
"""

from __future__ import annotations
import os, sys, time, textwrap, json, math, re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

API = "https://api-web.nhle.com"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NHLDailyBot/1.1; +github)",
    "Accept": "application/json, text/plain, */*",
}

SCHEDULE_FMT = API + "/v1/schedule/{ymd}"
STANDINGS_NOW = API + "/v1/standings/now"
GAME_PBP_FMT = API + "/v1/gamecenter/{gamePk}/play-by-play"

# ----------- ENV / flags -----------
REPORT_DATE_LOCAL = (os.getenv("REPORT_DATE_LOCAL") or "").strip()
REPORT_TZ = (os.getenv("REPORT_TZ") or os.getenv("REPORT_DATE_TZ") or "Europe/Amsterdam").strip()
DRY_RUN = (os.getenv("DRY_RUN") or "0").strip() == "1"
DEBUG_VERBOSE = (os.getenv("DEBUG_VERBOSE") or "1").strip() == "1"

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

def dbg(msg: str):
    if DEBUG_VERBOSE:
        print(f"[DBG] {msg}", flush=True)

def _get_with_retries(url: str, *, timeout: int = 30, as_text: bool = False) -> Any:
    last = None
    for i in range(3):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text if as_text else r.json()
        except Exception as e:
            last = e
            dbg(f"retry {i+1}/3 for {url}: {repr(e)}")
            time.sleep(0.75 * (i + 1))
    if last:
        raise last

def http_get_json(url: str, timeout: int = 30) -> Any:
    return _get_with_retries(url, timeout=timeout, as_text=False)

def http_get_text(url: str, timeout: int = 30) -> str:
    return _get_with_retries(url, timeout=timeout, as_text=True)

# ----------- RU команды и эмодзи (сжато) -----------
TEAM_RU = {
    "EDM": "Эдмонтон", "DAL": "Даллас", "DET": "Детройт", "NSH": "Нэшвилл",
    "TBL": "Тампа-Бэй", "CGY": "Калгари", "FLA": "Флорида", "PHI": "Филадельфия",
    "NJD": "Нью-Джерси", "STL": "Сент-Луис", "NYI": "Айлендерс", "BOS": "Бостон",
    "PIT": "Питтсбург", "BUF": "Баффало", "WSH": "Вашингтон", "WPG": "Виннипег",
    "CAR": "Каролина", "NYR": "Рейнджерс", "CBJ": "Коламбус", "TOR": "Торонто",
    "CHI": "Чикаго", "MIN": "Миннесота", "COL": "Колорадо", "SJS": "Сан-Хосе",
    "UTA": "Юта", "MTL": "Монреаль", "VGK": "Вегас", "OTT": "Оттава",
    "ANA": "Анахайм", "VAN": "Ванкувер", "SEA": "Сиэтл", "LAK": "Лос-Анджелес",
    "NHL": "НХЛ",  # запасной
}
TEAM_EMOJI = {
    "EDM": "🛢️", "DAL": "⭐️", "DET": "🛡️", "NSH": "🐯",
    "TBL": "⚡",  "CGY": "🔥",  "FLA": "🐆", "PHI": "🛩",
    "NJD": "😈", "STL": "🎵",  "NYI": "🏝️","BOS": "🐻",
    "PIT": "🐧", "BUF": "🦬",  "WSH": "🦅", "WPG": "✈️",
    "CAR": "🌪️", "NYR": "🗽",  "CBJ": "💣", "TOR": "🍁",
    "CHI": "🦅", "MIN": "🌲",  "COL": "⛰️", "SJS": "🦈",
    "UTA": "🧊", "MTL": "🇨🇦", "VGK": "🎰", "OTT": "🛡",
    "ANA": "🦆", "VAN": "🐳",  "SEA": "🦑", "LAK": "👑",
}

# ----------- Sports.ru URL generator (Utah/Vegas fix) -----------
def _slugify_en(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s

SPORTSRU_TEAM_SLUGS = {
    "VGK": ["vegas", "vegas-golden-knights"],
    "UTA": ["utah-mammoth", "utah", "utah-hc", "utah-hockey-club", "utah-hc-nhl"],
    "UTH": ["utah-mammoth", "utah", "utah-hc", "utah-hockey-club", "utah-hc-nhl"],
    "UTAH": ["utah-mammoth", "utah", "utah-hc", "utah-hockey-club", "utah-hc-nhl"],
}

def _team_slug_variants_for_sportsru(team: Dict[str, Any]) -> List[str]:
    v: List[str] = []
    abbr = (team.get("abbrev") or team.get("triCode") or "").upper()
    if abbr in SPORTSRU_TEAM_SLUGS:
        v.extend(SPORTSRU_TEAM_SLUGS[abbr])

    place = _slugify_en(team.get("placeName") or team.get("city") or "")
    nick  = _slugify_en(team.get("teamName") or team.get("name") or "")

    if place and nick:
        v.append(f"{place}-{nick}")
    if nick:
        v.append(nick)
    if place and place not in v:
        v.append(place)

    seen = set()
    out = []
    for x in v:
        if x and x not in seen:
            out.append(x); seen.add(x)
    return out

def gen_sportsru_match_urls(home_team: Dict[str, Any], away_team: Dict[str, Any]) -> List[str]:
    base = "https://www.sports.ru/hockey/match"
    hs = _team_slug_variants_for_sportsru(home_team)
    as_ = _team_slug_variants_for_sportsru(away_team)

    urls: List[str] = []
    for h in hs:
        for a in as_:
            urls.append(f"{base}/{h}-vs-{a}/")
            urls.append(f"{base}/{a}-vs-{h}/")
            urls.append(f"{base}/{h}-vs-{a}/stat/")
            urls.append(f"{base}/{a}-vs-{h}/stat/")

    seen = set(); out = []
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

# ----------- Telegram -----------
def send_telegram_text(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if DRY_RUN:
        print("[DRY RUN] " + textwrap.shorten(text.replace("\n", " "), 200, placeholder="…"))
        return
    r = requests.post(url, json=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram error: {js}")

# ----------- Helpers -----------
def parse_ymd_in_tz(ymd: str, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    d = date.fromisoformat(ymd)
    start = datetime.combine(d, dtime(0,0), tzinfo=tz)
    end   = datetime.combine(d, dtime(23,59,59), tzinfo=tz)
    return start, end

def fetch_schedule_day(ymd: date) -> List[Dict[str, Any]]:
    js = http_get_json(SCHEDULE_FMT.format(ymd=ymd.isoformat()))
    games = js.get("games")
    if games is None:
        games = []
        for w in (js.get("gameWeek") or []):
            games.extend(w.get("games") or [])
    return games or []

def start_dt_in_tz(g: Dict[str, Any], tz: ZoneInfo) -> Optional[datetime]:
    utc = g.get("startTimeUTC") or g.get("startTime") or g.get("gameDate")
    if not utc:
        return None
    try:
        return datetime.fromisoformat(utc.replace("Z","+00:00")).astimezone(tz)
    except Exception:
        return None

def is_final(g: Dict[str, Any]) -> bool:
    st = (g.get("gameState") or g.get("gameStatus") or "").upper()
    return st in ("FINAL", "OFF")

def team_block(g: Dict[str, Any], side: str) -> Dict[str, Any]:
    # NHL v1 schedule typical keys
    t = (g.get(f"{side}Team") or {})
    # normalize abbrev keys
    ab = t.get("abbrev") or t.get("triCode")
    if not ab and t.get("id"):
        # try "teamAbbrev"
        ab = t.get("teamAbbrev")
    t["abbrev"] = (ab or "").upper()
    return t

def fetch_standings_map() -> Dict[str, Tuple[int,int,int]]:
    try:
        js = http_get_json(STANDINGS_NOW)
    except Exception as e:
        dbg(f"standings fetch failed: {e!r}")
        return {}
    out = {}
    for conf in js.get("standings", []):
        for div in conf.get("divisions", []):
            for team in div.get("teams", []):
                ab = (team.get("teamAbbrev") or team.get("abbrev") or "").upper()
                rec = team.get("record") or {}
                out[ab] = (rec.get("wins",0), rec.get("losses",0), rec.get("ot",0))
    dbg(f"standings map built: {len(out)}")
    return out

def fmt_record(rec: Tuple[int,int,int]) -> str:
    return f"{rec[0]}-{rec[1]}-{rec[2]}"

def mmss_to_ru(mmss: str) -> str:
    # "03:48" → "03.48"
    return mmss.replace(":", ".")

def load_pbp_goals(game_pk: int) -> List[Dict[str, Any]]:
    js = http_get_json(GAME_PBP_FMT.format(gamePk=game_pk))
    goals: List[Dict[str, Any]] = []
    for ev in js.get("plays", {}).get("scoringPlays", []):
        try:
            per = ev["periodDescriptor"]["number"]
            tm = ev["timeInPeriod"]
            # scorer + assists (may be empty)
            desc = []
            if ev.get("scorer"):
                desc.append(ev["scorer"].get("lastName", "").strip())
            assists = []
            for a in ev.get("assists") or []:
                nm = a.get("lastName", "").strip()
                if nm:
                    assists.append(nm)
            goals.append({
                "period": per,
                "time": tm,
                "scorer": desc[0] if desc else "",
                "assists": assists,
                "teamAbbrev": (ev.get("details") or {}).get("eventOwnerTeamAbbrev") or ev.get("teamAbbrev") or "",
            })
        except Exception:
            continue
    return goals

# ---- Sports.ru parsing (акцент на имена; поддержка Utah/Vegas slug’ов) ----
def try_parse_sportsru_names(url: str) -> Dict[str, str]:
    """
    Возвращает карту EN->RU для фамилий, если удалось достать из матча.
    Парсим блоки 'Голы' или табличные карточки. Это best-effort (HTML может меняться).
    """
    try:
        html = http_get_text(url, timeout=25)
    except Exception as e:
        dbg(f"sports.ru fetch fail {url}: {e!r}")
        return {}
    soup = BeautifulSoup(html, "html.parser")

    ru_names: Dict[str,str] = {}

    # 1) В таблицах состава/протокола часто присутствуют 'data-player-name'/'title'/'href'
    for a in soup.select("a[href*='/hockey/players/'], a[href*='/hockey/player/']"):
        txt = (a.get_text(strip=True) or "")
        if not txt:
            continue
        # Если встречается 'Иван Иванов', сохраним обе части + короткую фамилию
        parts = txt.split()
        if len(parts) >= 1:
            ru_last = parts[-1]
            # иногда в title латиница:
            en = (a.get("title") or "").strip()
            # если латиницы нет — попробуем data-атрибуты:
            if not en:
                en = (a.get("data-name") or a.get("data-player-name") or "").strip()
            # как fallback — href (часто '/players/ivan-ivanov/')
            if not en:
                href = a.get("href") or ""
                m = re.search(r"/players/([\w-]+)/", href)
                if m:
                    en = m.group(1).replace("-", " ")
            en = en.strip()
            if en:
                en_last = en.split()[-1].title()
                if en_last and ru_last:
                    ru_names[en_last] = ru_last

    # 2) В ленте событий (голы) текст уже на русском — дополним карты по структуре "Фамилия (Фамилия, Фамилия)"
    for li in soup.select("li, div"):
        t = li.get_text(" ", strip=True)
        if not t:
            continue
        # вычленим "Иванов (" и имена в скобках
        if " – " in t or " - " in t:
            # грубый Heuristic: после "– ММ.СС " идёт Фамилия (Ассист, Ассист)
            # но если время перед фамилией — просто собираем все кириллические «слова с заглавной»
            names = re.findall(r"([А-ЯЁ][а-яё\-']{2,})", t)
            for nm in names:
                ru_names.setdefault(nm, nm)  # не знаем латиницу — хотя бы RU->RU

    if ru_names:
        dbg(f"sports.ru names extracted from {url}: {len(ru_names)}")
    return ru_names

def fetch_ru_name_map_for_match(home_team: Dict[str,Any], away_team: Dict[str,Any]) -> Dict[str,str]:
    tried = []
    for url in gen_sportsru_match_urls(home_team, away_team):
        tried.append(url)
        mp = try_parse_sportsru_names(url)
        if mp:
            dbg(f"sports.ru goals ok for {url}")
            return mp
    dbg("sports.ru tried URLs (no data): " + " | ".join(tried[:8]))
    return {}

def ru_last_or_keep(en_last: str, ru_map: Dict[str,str]) -> str:
    if not en_last:
        return ""
    return ru_map.get(en_last, en_last)

# ----------- Rendering -----------
def render_game_block(g: Dict[str, Any], standings: Dict[str,Tuple[int,int,int]]) -> str:
    home = team_block(g, "home")
    away = team_block(g, "away")
    h_ab, a_ab = home["abbrev"], away["abbrev"]
    h_emoji = TEAM_EMOJI.get(h_ab, "")
    a_emoji = TEAM_EMOJI.get(a_ab, "")

    h_name = TEAM_RU.get(h_ab, home.get("name") or h_ab)
    a_name = TEAM_RU.get(a_ab, away.get("name") or a_ab)

    h_score = (g.get("homeTeam") or {}).get("score", 0)
    a_score = (g.get("awayTeam") or {}).get("score", 0)

    h_rec = fmt_record(standings.get(h_ab, (0,0,0)))
    a_rec = fmt_record(standings.get(a_ab, (0,0,0)))

    # Заголовок пары
    head = []
    head.append(f"{h_emoji} «{h_name}»")
    head.append(f"{a_emoji} «{a_name}»")
    head_txt = "\n".join(head)

    # Счёт с рекордами
    lines = [
        head_txt,
        "",
        f"«{h_name}»: {h_score} ({h_rec})",
        f"«{a_name}»: {a_score} ({a_rec})",
        "",
    ]

    # Голы по периодам
    goals = load_pbp_goals(g["id"])
    ru_map = fetch_ru_name_map_for_match(home, away)  # best-effort карта EN->RU

    per_goals: Dict[int, List[str]] = {1:[],2:[],3:[]}
    ot_goals: List[str] = []
    so_goals: List[str] = []

    # Попробуем восстановить динамику счёта (как "1:0 – 03.48 ...")
    h_c, a_c = 0, 0
    for ev in goals:
        per = int(ev.get("period", 0) or 0)
        tm = mmss_to_ru(ev.get("time", "00:00"))
        owner = (ev.get("teamAbbrev") or "").upper()
        if owner == h_ab:
            h_c += 1
        elif owner == a_ab:
            a_c += 1
        score_str = f"{h_c}:{a_c}"

        scorer = ru_last_or_keep(ev.get("scorer","").title(), ru_map)
        assists = [ru_last_or_keep(x.title(), ru_map) for x in (ev.get("assists") or [])]
        if assists:
            who = f"{scorer} ({', '.join(assists)})"
        else:
            who = scorer or "—"

        line = f"{score_str} – {tm} {who}"

        if per in (1,2,3):
            per_goals[per].append(line)
        elif per == 4:
            ot_goals.append(line)
        elif per >= 5:
            so_goals.append(line)

    def add_period(title: str, arr: List[str]):
        lines.append(title)
        if arr:
            for s in arr:
                lines.append(s)
        else:
            lines.append("Голов не было")
        lines.append("")

    # Оборачиваем всё, кроме шапки, в <tg-spoiler>
    body_parts: List[str] = []
    add_period_func = lambda title, arr: (body_parts.append(title), body_parts.extend(arr or ["Голов не было"]), body_parts.append(""))
    # Переформатируем, чтобы собрать потом в спойлер:
    body_parts = []
    add_period_func("<i>1-й период</i>", per_goals[1])
    add_period_func("<i>2-й период</i>", per_goals[2])
    add_period_func("<i>3-й период</i>", per_goals[3])
    if ot_goals:
        add_period_func("<i>Овертайм</i>", ot_goals)
    if so_goals:
        add_period_func("<i>Буллиты</i>", so_goals)

    # Итоговый блок игры
    full = []
    full.append("\n".join(head))
    full.append("")
    full.append(f"<tg-spoiler><b>«{h_name}»: {h_score}</b> ({h_rec})")
    full.append(f"<b>«{a_name}»: {a_score}</b> ({a_rec})")
    full.append("")
    full.append("\n".join(body_parts).strip())
    full.append("</tg-spoiler>")
    return "\n".join(full).replace("\n\n\n", "\n\n").strip()

def build_day_text(ymd: str, tz: str) -> List[str]:
    tzinfo = ZoneInfo(tz)
    if not ymd:
        base_local = datetime.now(tzinfo).date()
        ymd = base_local.isoformat()
    else:
        base_local = date.fromisoformat(ymd)

    dbg(f"Daily summary for {ymd} in {tz}")
    start, end = parse_ymd_in_tz(ymd, tzinfo)

    # собираем игры, попавшие в локальный день
    raw = fetch_schedule_day(base_local - timedelta(days=1)) \
        + fetch_schedule_day(base_local) \
        + fetch_schedule_day(base_local + timedelta(days=1))

    games = []
    for g in raw:
        dt = start_dt_in_tz(g, tzinfo)
        if not dt:
            continue
        if start <= dt <= end and is_final(g):
            games.append(g)

    dbg(f"Collected FINAL games: {len(games)}")

    standings = fetch_standings_map()

    if not games:
        return [f"🗓 Регулярный чемпионат НХЛ • {base_local.day} {month_ru(base_local.month)} • матчей нет"]

    head = f"🗓 Регулярный чемпионат НХЛ • {base_local.day} {month_ru(base_local.month)} • {len(games)} матчей\n\nРезультаты надёжно спрятаны 👇"
    sep = "—" * 66

    # Рендерим игры
    blocks = []
    for g in games:
        blocks.append(sep)
        blocks.append(render_game_block(g, standings))

    # Сплит по размерам Telegram ( ~4096, возьмём запас)
    txt = head + "\n" + "\n".join(blocks)
    parts: List[str] = []
    cur = []
    cur_len = 0
    for line in txt.splitlines():
        n = len(line) + 1
        if cur_len + n > 3500:
            parts.append("\n".join(cur))
            cur = [line]; cur_len = n
        else:
            cur.append(line); cur_len += n
    if cur:
        parts.append("\n".join(cur))
    return parts

RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}
def month_ru(m: int) -> str:
    return RU_MONTHS.get(m, "")

def main():
    parts = build_day_text(REPORT_DATE_LOCAL, REPORT_TZ)
    dbg(f"Telegram parts: {len(parts)}")
    for i, part in enumerate(parts, 1):
        if i == 1:
            send_telegram_text(part)
        else:
            send_telegram_text(f"…продолжение (часть {i}/{len(parts)})\n\n{part}")

if __name__ == "__main__":
    main()
