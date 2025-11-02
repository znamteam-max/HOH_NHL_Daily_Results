#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results Bot — DETAILED (per-period)
Постит/печатает сводку по матчам за MSK-окно с разбивкой по периодам,
с авторами голов и ассистами (из PBP; фолбэк: scoring-summary).

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID — чтобы отправлять в Telegram
  DEBUG=1 — подробные [DBG] логи
"""

from __future__ import annotations
import os, sys, json, time, math
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional
import requests

DEBUG = os.getenv("DEBUG", "0") == "1"

def dbg(*a):
    if DEBUG:
        print("[DBG]", *a)

# --------------------- HTTP ---------------------

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "NHL-Results-Bot/1.0 (+https://api-web.nhle.com)"
})

def http_json(url: str, timeout: float = 15.0) -> Dict[str, Any]:
    dbg("GET", url)
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def telegram_send(text: str) -> bool:
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        print(text)
        dbg("Telegram skipped (no env)")
        return False
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    payload = {
        "chat_id": chat,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    dbg("POST Telegram sendMessage")
    resp = SESSION.post(url, json=payload, timeout=20)
    ok = resp.ok and resp.json().get("ok")
    dbg("Telegram OK" if ok else f"Telegram FAIL {resp.status_code} {resp.text[:200]}")
    return bool(ok)

# --------------------- DATES (MSK window) ---------------------

MSK = dt.timezone(dt.timedelta(hours=3))

def msk_now() -> dt.datetime:
    return dt.datetime.now(tz=MSK)

def msk_date_str(d: dt.date) -> str:
    return d.isoformat()

def build_schedule_dates_window() -> List[str]:
    """Собираем три даты по MSK: вчера, сегодня, завтра — как в логах."""
    now = msk_now().date()
    dates = [now - dt.timedelta(days=1), now, now + dt.timedelta(days=1)]
    dbg("MSK window:", f"{dates[0]} → {dates[-1]}")
    return [msk_date_str(d) for d in dates]

# --------------------- TEAMS & RUSSIAN LABELS ---------------------

TEAM_RU = {
    "BOS":"Бостон", "CAR":"Каролина",
    "WPG":"Виннипег", "PIT":"Питсбург",
    "NSH":"Нэшвилл", "CGY":"Калгари",
    "SJS":"Сан-Хосе", "COL":"Колорадо",
    "FLA":"Флорида", "DAL":"Даллас",
    "BUF":"Баффало", "WSH":"Вашингтон",
    "MTL":"Монреаль", "OTT":"Оттава",
    "PHI":"Филадельфия", "TOR":"Торонто",
    "CBJ":"Коламбус", "STL":"Сент-Луис",
    "MIN":"Миннесота", "VAN":"Ванкувер",
    "LAK":"Лос-Анджелес", "NJD":"Нью-Джерси",
    "EDM":"Эдмонтон", "CHI":"Чикаго",
    "SEA":"Сиэтл", "NYR":"Рейнджерс",
    "ANA":"Анахайм", "ARI":"Аризона",
    "VGK":"Вегас", "DET":"Детройт",
    "NYI":"Айлендерс", "TBL":"Тампа-Бэй",
}

TEAM_EMOJI = {
    "BOS":"🐻","CAR":"🌪️",
    "WPG":"✈️","PIT":"🐧",
    "NSH":"🐯","CGY":"🔥",
    "SJS":"🦈","COL":"⛰️",
    "FLA":"🐆","DAL":"⭐️",
    "BUF":"🦬","WSH":"🦅",
    "MTL":"🇨🇦","OTT":"🛡",
    "PHI":"🛩","TOR":"🍁",
    "CBJ":"💣","STL":"🎵",
    "MIN":"🌲","VAN":"🐳",
    "LAK":"👑","NJD":"😈",
    "EDM":"🛢️","CHI":"🦅",
    "SEA":"🦑","NYR":"🗽",
    "ANA":"🦆","ARI":"🐺",
    "VGK":"🗡️","DET":"🛠️",
    "NYI":"🏝️","TBL":"⚡",
}

# --------------------- EN→RU transliteration (tolerant) ---------------------

EX_NAMERU = {
    "Charlie McAvoy":"Чарли Макэвой",
    "Pavel Zacha":"Павел Заха",
    "Mark Jankowski":"Марк Янковски",
    "Josh Morrissey":"Джош Моррисси",
    "Gabriel Vilardi":"Гэбриел Виларди",
    "Kyle Connor":"Кайл Коннор",
    "Sidney Crosby":"Сидни Кросби",
    "Evgeni Malkin":"Евгений Малкин",
    "Roman Josi":"Роман Йоси",
    "Artturi Lehkonen":"Артури Лехконен",
    "Mikko Rantanen":"Микко Рантанен",
    "Aaron Ekblad":"Аарон Экблад",
    "Matthew Tkachuk":"Мэттью Ткачак",
    "Brandon Montour":"Брэндон Монтур",
    "John Tavares":"Джон Таварес",
    "Auston Matthews":"Остин Мэттьюс",
    "Morgan Rielly":"Морган Райли",
    "Cole Caufield":"Коул Кофилд",
    "Nick Suzuki":"Ник Сузуки",
    "Tim Stützle":"Тим Штюцле",
    "Shane Pinto":"Шейн Пинто",
    "Damon Severson":"Дэймон Сиверсон",
    "Nico Hischier":"Нико Хишир",
    "Luke Hughes":"Люк Хьюз",
    "Connor Bedard":"Коннор Бедард",
}

def translit_en_to_ru(name: str) -> str:
    if not name:
        return ""
    name = name.strip()
    if name in EX_NAMERU:
        return EX_NAMERU[name]
    # очень простая транслитерация; лучше заменить вашим слоем словаря
    # поддержка сложных сочетаний
    s = name
    pairs = [
        ("Sch","Щ"), ("Sh","Ш"), ("Ch","Ч"), ("Th","Т"), ("Ph","Ф"),
        ("Qu","Кв"), ("Qu","Кв"), ("Yu","Ю"), ("Ya","Я"), ("Yo","Ё"), ("Zh","Ж"),
        ("sch","щ"), ("sh","ш"), ("ch","ч"), ("th","т"), ("ph","ф"),
        ("qu","кв"), ("yu","ю"), ("ya","я"), ("yo","ё"), ("zh","ж"),
        ("ck","к"), ("Mc","Мак"), ("mac","мак"),
    ]
    for a,b in pairs:
        s = s.replace(a,b)
    table = {
        "A":"А","B":"Б","C":"К","D":"Д","E":"Е","F":"Ф","G":"Г","H":"Х","I":"И","J":"Дж","K":"К","L":"Л","M":"М","N":"Н","O":"О","P":"П","Q":"К","R":"Р","S":"С","T":"Т","U":"У","V":"В","W":"В","X":"Кс","Y":"И","Z":"З",
        "a":"а","b":"б","c":"к","d":"д","e":"е","f":"ф","g":"г","h":"х","i":"и","j":"дж","k":"к","l":"л","m":"м","n":"н","o":"о","p":"п","q":"к","r":"р","s":"с","t":"т","u":"у","v":"в","w":"в","x":"кс","y":"и","z":"з",
        "-":"-","’":"’","'":"’",".":" ","́":"", "̈":""
    }
    out = []
    for ch in s:
        out.append(table.get(ch, ch))
    return "".join(out).replace("  ", " ").strip()

def ru_player(name_en: str) -> str:
    """Позволяет легко заменить на ваш словарь RU-имён."""
    return translit_en_to_ru(name_en)

# --------------------- NHL helpers ---------------------

def _pick_str(x):
    """Берёт строку из значения или словаря локализаций (default/en/любой первый str)."""
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        for k in ("default", "en", "en_US", "eng", "us", "USA"):
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in x.values():
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""

def _to_int(x, default=0):
    """Надёжно приводит к int, даже если пришёл dict/str."""
    try:
        if isinstance(x, dict):
            # попробуем вытащить любое числовое значение
            for v in x.values():
                try:
                    return int(v)
                except Exception:
                    continue
            s = _pick_str(x)
            return int(s) if s else int(default)
        return int(x)
    except Exception:
        return int(default)

def _get_abbrev(rec):
    """Достаёт трёхбуквенную аббревиатуру из разных вариантов полей/вложенностей."""
    for key in ("teamAbbrev", "teamAbbrevTricode", "teamTricode", "tricode"):
        s = _pick_str(rec.get(key))
        if s:
            return s.upper()
    team = rec.get("team") or {}
    for key in ("abbrev", "abbrevTricode", "tricode"):
        s = _pick_str(team.get(key))
        if s:
            return s.upper()
    return ""

def fetch_standings_now() -> dict:
    """abbr -> {w,l,otl,pts}, устойчиво к dict-полям в JSON."""
    url = "https://api-web.nhle.com/v1/standings/now"
    data = http_json(url)
    rows = data if isinstance(data, list) else data.get("standings", [])
    out = {}

    dbg("records loaded:", len(rows) if isinstance(rows, list) else 0)

    for r in (rows or []):
        abbr = _get_abbrev(r)
        if not abbr:
            continue
        w   = _to_int(r.get("wins"), 0)
        l   = _to_int(r.get("losses"), 0)
        otl = _to_int(r.get("otLosses") or r.get("overtimeLosses"), 0)
        pts = _to_int(r.get("points"), 0)
        out[abbr] = {"w": w, "l": l, "otl": otl, "pts": pts}

    # Фолбэк на датированный эндпоинт, если что-то пошло не так
    if not out:
        try:
            today = msk_now().date().isoformat()
            dbg("standings empty; fallback to", today)
            data = http_json(f"https://api-web.nhle.com/v1/standings/{today}")
            rows = data if isinstance(data, list) else data.get("standings", [])
            for r in (rows or []):
                abbr = _get_abbrev(r)
                if not abbr:
                    continue
                out[abbr] = {
                    "w": _to_int(r.get("wins"), 0),
                    "l": _to_int(r.get("losses"), 0),
                    "otl": _to_int(r.get("otLosses") or r.get("overtimeLosses"), 0),
                    "pts": _to_int(r.get("points"), 0),
                }
        except Exception as e:
            dbg("standings fallback failed:", repr(e))

    return out

def fetch_schedule_for_dates(dates: List[str]) -> List[Dict[str,Any]]:
    games = []
    for d in dates:
        data = http_json(f"https://api-web.nhle.com/v1/schedule/{d}")
        for g in data.get("gameWeek", []):
            for day in g.get("games", []):
                games.append(day)
    return games

def is_final(game: Dict[str,Any]) -> bool:
    s = (game.get("gameState") or "").upper()
    return s in ("FINAL","OFF")

def unique_final_games(glist: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    seen = set()
    out = []
    for g in glist:
        gid = g.get("id")
        if not gid: continue
        if gid in seen: continue
        if is_final(g):
            seen.add(gid); out.append(g)
        else:
            dbg("skip not final:", gid, g.get("gameState"))
    return out

def fetch_pbp(gid: int) -> Dict[str,Any]:
    return http_json(f"https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play")

def fetch_scoring_summary(gid: int) -> Dict[str,Any]:
    try:
        return http_json(f"https://api-web.nhle.com/v1/gamecenter/{gid}/scoring-summary")
    except Exception:
        return {}

# --------------------- Extract names from PBP ---------------------

def _name_from_any(v: Any) -> str:
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        for k in ("fullName","name","playerName","scorerName","scorerFullName"):
            s = v.get(k)
            if isinstance(s, str) and s.strip():
                return s.strip()
            if isinstance(s, dict):
                d = s.get("default")
                if isinstance(d, str) and d.strip():
                    return d.strip()
        fn = v.get("firstName"); ln = v.get("lastName")
        if isinstance(fn, dict) or isinstance(ln, dict):
            f = (fn.get("default") if isinstance(fn, dict) else "") or ""
            l = (ln.get("default") if isinstance(ln, dict) else "") or ""
            return (f + " " + l).strip()
        f = v.get("firstName") or ""
        l = v.get("lastName") or ""
        if f or l:
            return (str(f).strip() + " " + str(l).strip()).strip()
    return ""

def extract_names_from_play(p: Dict[str, Any]) -> Tuple[str, List[str]]:
    d = p.get("details") or {}
    # direct fields
    scorer = ""
    for key in ("scorerFullName","scorerName","scorer"):
        nm = _name_from_any(d.get(key))
        if nm:
            scorer = nm; break
    assists: List[str] = []
    if isinstance(d.get("assists"), list):
        for a in d["assists"]:
            nm = _name_from_any(a)
            if nm: assists.append(nm)

    # players[]
    if not scorer or not assists:
        for pl in (p.get("players") or []):
            pt = (str(pl.get("playerType") or pl.get("type") or "")).lower().replace(" ", "")
            ptc = (str(pl.get("playerTypeCode") or "")).upper()
            nm = ""
            if "player" in pl:
                nm = _name_from_any(pl.get("player"))
            if not nm:
                nm = _name_from_any(pl)
            is_scorer = (pt == "scorer") or (ptc == "G")
            is_ast1 = (pt in ("assist","assist1","assist_1")) or (ptc == "A")
            is_ast2 = (pt in ("assist2","assist_2")) or (ptc == "A2")
            if is_scorer and nm:
                scorer = scorer or nm
            elif (is_ast1 or is_ast2) and nm:
                if nm not in assists:
                    assists.append(nm)

    if len(assists) > 2:
        assists = assists[:2]
    return scorer, assists

def extract_goals_from_pbp(pbp: Dict[str, Any], home_id: int, away_id: int) -> Tuple[List[Dict[str, Any]], bool]:
    plays = pbp.get("plays") or []
    goals, home, away, shootout = [], 0, 0, False
    for p in plays:
        ty = (p.get("typeDescKey") or p.get("typeCode") or "").lower()
        period = int((p.get("periodDescriptor") or {}).get("number") or 0)
        ptype  = ((p.get("periodDescriptor") or {}).get("periodType") or "").upper()
        t = p.get("timeInPeriod") or p.get("timeRemaining") or "00:00"
        team_id = (p.get("details") or {}).get("eventOwnerTeamId") or p.get("teamId") or (p.get("details") or {}).get("teamId")

        if ptype == "SHOOTOUT":
            shootout = True
        if ty not in ("goal","goalie_goal"):
            continue

        if ":" not in t and "." in t:
            t = t.replace(".", ":")
        try:
            mm, ss = t.split(":")
            t = f"{int(mm):02d}:{int(ss):02d}"
        except Exception:
            t = "00:00"

        if team_id == home_id:
            home += 1; side = "HOME"
        elif team_id == away_id:
            away += 1; side = "AWAY"
        else:
            continue

        scorer_en, assists_en = extract_names_from_play(p)

        goals.append({
            "period": period or (4 if ptype.startswith("OT") else 3),
            "periodType": ptype or ("REG" if period <= 3 else "OT"),
            "mmss": t,
            "side": side,
            "home": home,
            "away": away,
            "en_scorer": scorer_en,
            "en_assists": assists_en,
        })
    return goals, shootout

def extract_goals_from_summary(summary: Dict[str, Any], home_abbr: str, away_abbr: str) -> List[Dict[str, Any]]:
    if not summary:
        return []
    goals: List[Dict[str, Any]] = []
    home = away = 0
    by_period = summary.get("byPeriod") or summary.get("goalsByPeriod") or []
    for pd in by_period:
        pnum = int((pd.get("periodDescriptor") or {}).get("number") or pd.get("period") or 0)
        ptype = ((pd.get("periodDescriptor") or {}).get("periodType") or ("REG" if pnum <= 3 else "OT")).upper()
        for ev in (pd.get("goals") or []):
            team = (ev.get("teamAbbrev") or ev.get("team") or "").upper()
            mmss = ev.get("timeInPeriod") or ev.get("time") or "00:00"
            try:
                mm, ss = mmss.split(":"); mmss = f"{int(mm):02d}:{int(ss):02d}"
            except Exception:
                mmss = "00:00"

            if team == home_abbr:
                home += 1; side = "HOME"
            elif team == away_abbr:
                away += 1; side = "AWAY"
            else:
                continue

            scorer = _name_from_any(ev.get("scorer") or ev.get("scorerName") or ev.get("scorerFullName"))
            assists = []
            for a in (ev.get("assists") or []):
                if isinstance(a, dict):
                    nm = _name_from_any(a) or _name_from_any(a.get("player"))
                else:
                    nm = _name_from_any(a)
                if nm:
                    assists.append(nm)
            if len(assists) > 2:
                assists = assists[:2]

            goals.append({
                "period": pnum or (4 if ptype.startswith("OT") else 3),
                "periodType": ptype,
                "mmss": mmss,
                "side": side,
                "home": home,
                "away": away,
                "en_scorer": scorer,
                "en_assists": assists,
            })
    return goals

# --------------------- Formatting ---------------------

def mmss_dot(s: str) -> str:
    return s.replace(":", ".")

def period_header(pnum: int, ptype: str, idx_ot: int) -> str:
    if ptype == "REG":
        return f"{pnum}-й период"
    if ptype == "OT":
        n = idx_ot if idx_ot > 0 else 1
        return f"Овертайм №{n}"
    if ptype == "SHOOTOUT":
        return "Буллиты"
    # fallback
    return f"{pnum}-й период"

def format_game_detailed(game: Dict[str,Any], rec: Dict[str,Dict[str,int]]) -> str:
    gid = int(game["id"])
    h = game["homeTeam"]; a = game["awayTeam"]
    h_abbr, a_abbr = h["abbrev"].upper(), a["abbrev"].upper()
    h_id, a_id = int(h["id"]), int(a["id"])
    h_score, a_score = int(game["homeTeam"]["score"]), int(game["awayTeam"]["score"])

    header = []
    for abbr, score in ((h_abbr,h_score),(a_abbr,a_score)):
        emoji = TEAM_EMOJI.get(abbr, "")
        ru = TEAM_RU.get(abbr, abbr)
        r = rec.get(abbr, {"w":0,"l":0,"otl":0,"pts":0})
        header.append(f"{emoji} «{ru}»: {score} ({r['w']}-{r['l']}-{r['otl']}, {r['pts']} о.)")
    top = "\n".join(header)

    pbp = fetch_pbp(gid)
    goals, shootout = extract_goals_from_pbp(pbp, h_id, a_id)

    missing = sum(1 for g in goals if not g.get("en_scorer"))
    if goals and (missing == len(goals) or missing >= max(2, len(goals)-1)):
        ss = fetch_scoring_summary(gid)
        ss_goals = extract_goals_from_summary(ss, h_abbr, a_abbr)
        if ss_goals:
            goals = ss_goals

    # группировка по периодам
    byp: Dict[Tuple[int,str], List[Dict[str,Any]]] = {}
    for g in goals:
        key = (int(g["period"]), g.get("periodType") or ("REG" if int(g["period"])<=3 else "OT"))
        byp.setdefault(key, []).append(g)

    # порядок периодов 1,2,3,OT1,OT2...
    keys_sorted = sorted(byp.keys(), key=lambda t: (100 if t[1]!="REG" else 0) + t[0])
    out_lines = [top, ""]
    ot_count = 0
    for (pnum, ptype) in keys_sorted:
        if ptype == "OT":
            ot_count += 1
        out_lines.append(period_header(pnum, ptype if ptype!="PERIOD" else ("REG" if pnum<=3 else "OT"), ot_count))
        for ev in byp[(pnum,ptype)]:
            side = ev["side"]
            # счёт после гола
            sc_home, sc_away = ev["home"], ev["away"]
            # авторы
            scorer_en = ev.get("en_scorer","") or ""
            assists_en = ev.get("en_assists") or []
            scorer_ru = ru_player(scorer_en) if scorer_en else "—"
            assists_ru = [ru_player(x) for x in assists_en if x]
            ass_txt = f" ({', '.join(assists_ru)})" if assists_ru else ""
            # как в примере: "2:1 – 17.11 Имя (ассисты)"
            score_pair = f"{sc_home}:{sc_away}" if side=="HOME" else f"{sc_home}:{sc_away}"
            out_lines.append(f"{score_pair} – {mmss_dot(ev['mmss'])} {scorer_ru}{ass_txt}")
        out_lines.append("")  # пустая строка после периода

    return "\n".join(out_lines).strip()

# --------------------- Main message ---------------------

def build_message_detailed(final_games: List[Dict[str,Any]], rec: Dict[str,Dict[str,int]]) -> str:
    # дата для заголовка — берём «сегодня по MSK»
    today = msk_now().date()
    date_ru = today.strftime("%-d %B").replace("January","января").replace("February","февраля")\
        .replace("March","марта").replace("April","апреля").replace("May","мая").replace("June","июня")\
        .replace("July","июля").replace("August","августа").replace("September","сентября")\
        .replace("October","октября").replace("November","ноября").replace("December","декабря")

    head = f"🗓 Регулярный чемпионат НХЛ • {date_ru} • {len(final_games)} матчей\n\nРезультаты надёжно спрятаны 👇\n"
    sep = "\n" + "—"*40 + "\n"

    parts = [head]
    for g in final_games:
        parts.append(sep)
        parts.append(format_game_detailed(g, rec))

    return "\n".join(parts).strip()

# --------------------- Entry ---------------------

def main():
    dates = build_schedule_dates_window()
    sched = fetch_schedule_for_dates(dates)
    finals = unique_final_games(sched)
    dbg("Collected unique FINAL games:", len(finals))
    rec = fetch_standings_now()
    text = build_message_detailed(finals, rec)
    telegram_send(text)

if __name__ == "__main__":
    main()
