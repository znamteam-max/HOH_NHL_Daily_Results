# -*- coding: utf-8 -*-
"""
NHL Daily Results → Telegram (RU)
Источник времени/счёта — NHL PBP.
Имена:
  • сначала пытаемся подтянуть русские с sports.ru (если матч есть и парсится),
  • иначе берём имена из NHL (EN) и переводим в кириллицу (в т.ч. список исключений для корректных фамилий).
"""

import os
import sys
import re
import json
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ----------------------- ЛОГ -----------------------
def dbg(*a): print("[DBG]", *a)
def err(*a): print("ERROR:", *a, file=sys.stderr)

# ----------------------- КОНСТАНТЫ -----------------------
MSK = ZoneInfo("Europe/Moscow")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()

COMPLETE_STATES = {"OFF", "FINAL", "COMPLETED", "END"}
HEADERS_WEB = {"User-Agent": "Mozilla/5.0 (compatible; HOH_NHL_Bot/1.3)"}

RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

RU_TEAMS = {
    "ANA":"Анахайм","ARI":"Аризона","UTA":"Юта","BOS":"Бостон","BUF":"Баффало","CGY":"Калгари","CAR":"Каролина",
    "CHI":"Чикаго","COL":"Колорадо","CBJ":"Коламбус","DAL":"Даллас","DET":"Детройт","EDM":"Эдмонтон",
    "FLA":"Флорида","LAK":"Лос-Анджелес","MIN":"Миннесота","MTL":"Монреаль","NJD":"Нью-Джерси","NSH":"Нэшвилл",
    "NYI":"Айлендерс","NYR":"Рейнджерс","OTT":"Оттава","PHI":"Филадельфия","PIT":"Питтсбург","SJS":"Сан-Хосе",
    "SEA":"Сиэтл","STL":"Сент-Луис","TBL":"Тампа-Бэй","TOR":"Торонто","VAN":"Ванкувер","VGK":"Вегас",
    "WSH":"Вашингтон","WPG":"Виннипег"
}

DEFAULT_EMOJI = {
    "ANA":"🦆","UTA":"🦣","ARI":"🦂","BOS":"🐻","BUF":"🦬","CGY":"🔥","CAR":"🌪️","CHI":"🦅","COL":"⛰️",
    "CBJ":"💣","DAL":"⭐️","DET":"🔴","EDM":"🛢️","FLA":"🐆","LAK":"👑","MIN":"🌲","MTL":"🇨🇦","NJD":"😈",
    "NSH":"🐯","NYI":"🟠","NYR":"🗽","OTT":"🛡","PHI":"🛩","PIT":"🐧","SJS":"🦈","SEA":"🦑","STL":"🎵",
    "TBL":"⚡","TOR":"🍁","VAN":"🐳","VGK":"🎰","WSH":"🦅","WPG":"✈️"
}
try:
    if os.getenv("TEAM_EMOJI_JSON"):
        DEFAULT_EMOJI.update(json.loads(os.getenv("TEAM_EMOJI_JSON")))
except Exception as e:
    err("TEAM_EMOJI_JSON parse error:", repr(e))

SPORTSRU_SLUG_BY_ABBR = {
    "ANA":"anaheim-ducks","ARI":"arizona-coyotes","UTA":"utah-hc","BOS":"boston-bruins","BUF":"buffalo-sabres",
    "CGY":"calgary-flames","CAR":"carolina-hurricanes","CHI":"chicago-blackhawks","COL":"colorado-avalanche",
    "CBJ":"columbus-blue-jackets","DAL":"dallas-stars","DET":"detroit-red-wings","EDM":"edmonton-oilers",
    "FLA":"florida-panthers","LAK":"los-angeles-kings","MIN":"minnesota-wild","MTL":"montreal-canadiens",
    "NJD":"new-jersey-devils","NSH":"nashville-predators","NYI":"new-york-islanders","NYR":"new-york-rangers",
    "OTT":"ottawa-senators","PHI":"philadelphia-flyers","PIT":"pittsburgh-penguins","SJS":"san-jose-sharks",
    "SEA":"seattle-kraken","STL":"st-louis-blues","TBL":"tampa-bay-lightning","TOR":"toronto-maple-leafs",
    "VAN":"vancouver-canucks","VGK":"vegas-golden-knights","WSH":"washington-capitals","WPG":"winnipeg-jets"
}

BAD_TOKENS = {
    "Завершен","Завершён","Пока","Перерыв","Буллиты","Овер","Овертайм","Удаление","В большинстве","В меньшинстве",
    "Бостон","Каролина","Виннипег","Питтсбург","Нэшвилл","Калгари","Сан-Хосе","Колорадо","Флорида","Даллас",
    "Баффало","Вашингтон","Монреаль","Оттава","Филадельфия","Торонто","Коламбус","Сент-Луис","Миннесота","Ванкувер",
    "Лос-Анджелес","Нью-Джерси","Эдмонтон","Чикаго","Сиэтл","Рейнджерс","Айлендерс","Тампа-Бэй","Вегас","Юта",
}
CYR = re.compile(r"^[А-ЯЁ][а-яё\-’'`]{2,}$")

# ----------------------- ТРАНСЛИТ -----------------------
EXCEPT_LAST = {
    # частые
    "CROSBY":"Кросби","MALKIN":"Малкин","OVECHKIN":"Овечкин","MCDAVID":"Макдэвид","DRAISAITL":"Драйзайтль",
    "TAVARES":"Таварес","MATTHEWS":"Мэттьюс","NYLANDER":"Нюландер","MARNER":"Марнер","REILLY":"Райлли","RIELLY":"Райлли",
    "ROBERTSON":"Робертсон","BENN":"Бенн","FOX":"Фокс","MILLER":"Миллер","HUGHES":"Хьюз","LAFRENIERE":"Лафреньер",
    "PANARIN":"Панарин","KAPRIZOV":"Капризов","FORSBERG":"Форсберг","PRICE":"Прайс","KANE":"Кейн",
    "SCHEIFELE":"Шайфли","MORRISSEY":"Моррисси","NICHUSHKIN":"Ничушкин","LEHKONEN":"Лехконен","MAKAR":"Макар",
    "LIZOTTE":"Лизотт","ACCIARI":"Аччари","BURAKOVSKY":"Бураковски","BEDARD":"Бедард","KYROU":"Кайру",
    "STUTZLE":"Штюцле","STUETZLE":"Штюцле","SLAFKOVSKY":"Слафковски","BATHERSON":"Батерсон","PINTO":"Пинто",
    "ZUB":"Зуб","PROTAS":"Протас","FEHERVARY":"Фегервары","LEONARD":"Леонард","POWER":"Пауэр","DOWD":"Доун",
    "HARLEY":"Харли","LOUSTARINEN":"Луостаринен","LUOSTARINEN":"Луостаринен","LUNDELL":"Лунделль","STEEL":"Стил",
    "RANTANEN":"Рантанен","MARCHESSAULT":"Маршессо","BEAN":"Бин","KADRI":"Кадри","BUNTING":"Бантинг","WOOD":"Вуд",
    "KARLSSON":"Карлссон","MCAVOY":"Макэвой","JANKOWSKI":"Янковски","NIKISHIN":"Никишин","CONNOR":"Коннор",
    "VILARDI":"Виларди","LAMBERT":"Ламберт","CARTLSON":"Карлссон","KUZMENKO":"Кузьменко","MERCER":"Мерсер",
    "GLENDENING":"Гленденинг","CHOLOWSKI":"Чоловски","HISHIER":"Хишир","BEDNAR":"Беднар","BRODZINSKI":"Бродзински",
    "KYLE":"Кайлле","KAILEY":"Кайлле","MILLER-KANDRE":"Миллер","TANEV":"Танев","KNIES":"Найз","JARNKROK":"Йернкрук",
    "COE":"Коуэн","COWEN":"Коуэн","ROBINSON":"Робинсон","ROSEN":"Росен","TAUCH":"Так","TUCH":"Так","THOMPSON":"Томпсон",
}

def translit_last(last_en: str) -> str:
    key = re.sub(r"[^A-Za-z\-]", "", last_en).upper()
    if key in EXCEPT_LAST:
        return EXCEPT_LAST[key]
    s = last_en.lower()
    # диграфы
    rep2 = [
        ("shch","щ"), ("sch","ш"), ("ch","ч"), ("sh","ш"), ("zh","ж"), ("ph","ф"),
        ("kh","х"), ("ts","ц"), ("ya","я"), ("yu","ю"), ("yo","йо"), ("ye","е"),
        ("ee","и"), ("oo","у"), ("ou","оу"), ("ai","ай"), ("ei","ей"), ("ie","и"),
        ("ck","к"), ("qu","кв")
    ]
    for a,b in rep2: s = s.replace(a,b)
    # одиночные
    table = {
        "a":"а","b":"б","c":"к","d":"д","e":"е","f":"ф","g":"г","h":"х","i":"и","j":"дж","k":"к",
        "l":"л","m":"м","n":"н","o":"о","p":"п","q":"к","r":"р","s":"с","t":"т","u":"у","v":"в",
        "w":"в","x":"кс","y":"и","z":"з","’":"", "'":"", "`":""
    }
    out = "".join(table.get(ch, ch) for ch in s)
    # концовки
    out = re.sub(r"ii\b","и", out)
    out = re.sub(r"th\b","т", out)
    out = re.sub(r"w\b","у", out)
    # заглавная
    return out[:1].upper() + out[1:]

def en_full_to_ru_last(en_name: str) -> str:
    # берём последнюю «фамилию» из полного имени
    parts = [p for p in re.split(r"\s+", en_name.strip()) if p]
    last = parts[-1] if parts else en_name.strip()
    # некоторые фамилии состоят из двух частей (DeBrusk, Van Riemsdyk) — склеим
    last = last.replace("-", "").replace("’","").replace("'","").replace("`","")
    return translit_last(last)

# ----------------------- УТИЛИТЫ -----------------------
def ru_date(d: datetime) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def http_get(url: str, timeout: int = 20) -> requests.Response:
    dbg("GET", url); r = requests.get(url, headers=HEADERS_WEB, timeout=timeout); r.raise_for_status(); return r
def http_json(url: str) -> Dict[str, Any]: return http_get(url).json()

def _val(x):
    if isinstance(x, dict):
        return x.get("default") or next((v for v in x.values() if isinstance(v, str) and v), "")
    return x or ""

def msk_window_for_date(base_date: date) -> Tuple[datetime, datetime]:
    start = datetime(base_date.year, base_date.month, base_date.day, 15, 0, 0, tzinfo=MSK) - timedelta(days=1)
    end   = datetime(base_date.year, base_date.month, base_date.day, 23, 59, 59, tzinfo=MSK)
    return start, end

# ----------------------- NHL CORE -----------------------
def fetch_schedule(day: date) -> List[Dict[str, Any]]:
    j = http_json(f"https://api-web.nhle.com/v1/schedule/{day.isoformat()}")
    out = []
    for gd in (j.get("games") or []): out.append(gd)
    for wk in (j.get("gameWeek") or []):
        for gd in (wk.get("games") or []): out.append(gd)
    by_id = {}
    for gd in out:
        gid = gd.get("id") or gd.get("gamePk") or gd.get("gameNumber") or gd.get("gameId")
        if gid is not None: by_id[gid] = gd
    return list(by_id.values())

def game_state(g) -> str:
    return str(g.get("gameState") or g.get("gameStatus", {}).get("state") or "").upper()

def game_start_msk(g) -> Optional[datetime]:
    ts = g.get("startTimeUTC") or g.get("startTimeUTCDate") or g.get("startTimeUTCFormatted")
    if not ts: return None
    if ts.endswith("Z"): ts = ts[:-1] + "+00:00"
    try: return datetime.fromisoformat(ts).astimezone(MSK)
    except Exception: return None

def team_info(g) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return (g.get("homeTeam") or {}), (g.get("awayTeam") or {})

def filter_completed_in_window(games, start_msk, end_msk):
    res = []
    for g in games:
        if game_state(g) not in COMPLETE_STATES: continue
        dt = game_start_msk(g)
        if not dt: continue
        if start_msk <= dt <= end_msk: res.append(g)
    return res

def fetch_pbp(game_id: int) -> Dict[str, Any]:
    return http_json(f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play")

def _str(x) -> str:
    if x is None: return ""
    if isinstance(x, dict): return _val(x)
    return str(x)

# === NEW: более цепкий разбор имён из одного события PBP ===
NAME_RX = re.compile(
    r"""(?ix)
    ^\s*
    (?P<scorer>[A-Z][A-Za-z.'` -]+?)
    (?:\s*\(\d+\))?
    (?:\s*assists?\s*:\s*
        (?P<a1>[A-Z][A-Za-z.'` -]+?)
        (?:\s*\(\d+\))?
        (?:\s*,\s*
            (?P<a2>[A-Z][A-Za-z.'` -]+?)
            (?:\s*\(\d+\))?
        )?
    )?
    \s*$
    """
)

def extract_names_from_play(p: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Возвращает ('Full Name EN', ['Assist1 EN','Assist2 EN']) или ('', []).
       Перебираем ВСЕ известные схемы NHL + парсим описательный текст, если нужно.
    """
    d = p.get("details") or {}
    scorer_en = ""
    assists_en: List[str] = []

    # 1) Наиболее частые поля в details
    prim = [
        "scorerFullName","scorerName","scoringPlayerName","goalScorerName",
        "playerName","player","scorer"
    ]
    for k in prim:
        v = d.get(k)
        if isinstance(v, dict):
            nm = v.get("fullName") or v.get("default") or (v.get("firstName","")+" "+v.get("lastName","")).strip()
            if nm: scorer_en = nm; break
        elif isinstance(v, str) and v.strip():
            scorer_en = v.strip(); break

    # 2) Игроки внутри события
    if not scorer_en or not assists_en:
        for pl in (p.get("players") or []):
            pt = _str(pl.get("playerType")).lower()
            pv = pl.get("player")
            nm = ""
            if isinstance(pv, dict):
                nm = pv.get("fullName") or pv.get("default") or pv.get("lastName") or ""
            else:
                nm = _str(pv)
            if not nm:
                # отдельные поля
                nm = _str(pl.get("fullName") or pl.get("name") or pl.get("playerName"))
            if not nm:
                continue
            if pt in ("scorer","goal-scorer","goalie_goal","goalscorer"):
                scorer_en = scorer_en or nm
            elif pt.startswith("assist"):
                assists_en.append(nm)

    # 3) Ассистенты как отдельные ключи
    for k in ("assist1PlayerName","assist2PlayerName","assist1Name","assist2Name"):
        v = d.get(k)
        if isinstance(v, dict):
            nm = v.get("fullName") or v.get("default") or ""
        else:
            nm = _str(v)
        if nm:
            assists_en.append(nm)

    # 4) Иногда NHL кладёт весь текст в строку описания
    #    Ищем в details.eventDescription / details.desc.* / p.eventDescription.*
    texts = []
    for key in ("eventDescription","desc","description","eventDetails","resultDescription"):
        v = d.get(key) or p.get(key)
        if isinstance(v, dict):
            texts.append(v.get("default") or v.get("en") or "")
        elif isinstance(v, str):
            texts.append(v)
    texts = [t for t in texts if t]

    if (not scorer_en) and texts:
        # Часто формат: "Sam Reinhart (10) Assists: Verhaeghe (12), Rodrigues (9)"
        for t in texts:
            m = NAME_RX.search(t)
            if m:
                scorer_en = m.group("scorer").strip()
                if m.group("a1"): assists_en.append(m.group("a1").strip())
                if m.group("a2"): assists_en.append(m.group("a2").strip())
                break

    # Нормализуем
    assists_en = [a for a in assists_en if a]
    # max два ассиста
    assists_en = assists_en[:2]
    return scorer_en.strip(), assists_en

# === NEW: запасной источник имён — game-summary ===
def fetch_summary(game_id: int) -> Dict[str, Any]:
    return http_json(f"https://api-web.nhle.com/v1/gamecenter/{game_id}/game-summary")

def extract_goals_from_summary(summ: Dict[str, Any], home_id: int, away_id: int) -> List[Dict[str, Any]]:
    """Достаём список голов с именами из game-summary, если PBP не дал имён."""
    out: List[Dict[str, Any]] = []
    # В разных сезонах структура чуть меняется, пробуем популярные варианты
    # 1) summ["scoring"] = [{"periodNumber":1,"goals":[{timeInPeriod, teamId, scorerName, assists:[...]}]}]
    scoring = summ.get("scoring") or summ.get("goalSummary") or []
    def add_goal(period, ptype, gobj, home_cnt, away_cnt):
        t = gobj.get("timeInPeriod") or gobj.get("timeRemaining") or gobj.get("time") or "00:00"
        if ":" not in t and "." in t: t = t.replace(".", ":")
        try:
            mm, ss = t.split(":"); t = f"{int(mm):02d}:{int(ss):02d}"
        except Exception:
            t = "00:00"
        team_id = gobj.get("teamId") or (gobj.get("team") or {}).get("id")
        side = "HOME" if team_id == home_id else ("AWAY" if team_id == away_id else "")
        if side == "HOME": home_cnt += 1
        elif side == "AWAY": away_cnt += 1
        sc = gobj.get("scorerFullName") or gobj.get("scorerName") or _val(gobj.get("scorer")) or ""
        asts = []
        alist = gobj.get("assists") or []
        for a in alist:
            if isinstance(a, dict):
                nm = a.get("fullName") or a.get("name") or a.get("default") or ""
            else:
                nm = _str(a)
            if nm: asts.append(nm)
        out.append({
            "period": int(period),
            "periodType": ptype,
            "mmss": t,
            "side": side or "",
            "home": home_cnt,
            "away": away_cnt,
            "en_scorer": sc,
            "en_assists": asts[:2],
        })
        return home_cnt, away_cnt

    if isinstance(scoring, list) and scoring:
        home, away = 0, 0
        for per in scoring:
            pnum = per.get("periodNumber") or per.get("number") or per.get("period") or 0
            ptype = (per.get("periodType") or "REG").upper()
            for g in (per.get("goals") or per.get("events") or []):
                home, away = add_goal(pnum, ptype, g, home, away)
        return out

    # 2) запасной бэкап — если есть плоский список summ["goals"]
    goals = summ.get("goals") or []
    if isinstance(goals, list) and goals:
        home, away = 0, 0
        for g in goals:
            pnum = g.get("periodNumber") or g.get("period") or 0
            ptype = (g.get("periodType") or "REG").upper()
            home, away = add_goal(pnum, ptype, g, home, away)
        return out

    return out

# === REPLACE: извлечение голов из PBP + бэкап к summary, если имён нет ===
def extract_goals_from_pbp(pbp: Dict[str, Any], home_id: int, away_id: int) -> Tuple[List[Dict[str, Any]], bool]:
    plays = pbp.get("plays") or []
    goals, home, away, shootout = [], 0, 0, False
    for p in plays:
        ty = (p.get("typeDescKey") or p.get("typeCode") or "").lower()
        period = int((p.get("periodDescriptor") or {}).get("number") or p.get("period") or 0)
        ptype  = ((p.get("periodDescriptor") or {}).get("periodType") or p.get("periodType") or "").upper()
        t = p.get("timeInPeriod") or p.get("timeRemaining") or "00:00"
        team_id = (p.get("details") or {}).get("eventOwnerTeamId") or p.get("teamId") or (p.get("details") or {}).get("teamId")

        if ptype == "SHOOTOUT": shootout = True; continue
        if ty not in ("goal","goalie_goal"): continue

        if team_id == home_id: home += 1; side = "HOME"
        elif team_id == away_id: away += 1; side = "AWAY"
        else: continue

        if ":" not in t and "." in t: t = t.replace(".", ":")
        try:
            mm, ss = t.split(":"); t = f"{int(mm):02d}:{int(ss):02d}"
        except Exception:
            t = "00:00"

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

    # Если НИ у одного гола нет имён — подтянем из game-summary
    if goals and not any(g.get("en_scorer") or g.get("en_assists") for g in goals):
        try:
            summ = fetch_summary(int((pbp.get("gameId") or 0)))
        except Exception:
            summ = {}
        if not summ:
            # иногда gameId нет в pbp — достанем из пути, если доступен
            pass
        if summ:
            # берём имена по времени/порядку
            by_time = {}
            for g in extract_goals_from_summary(summ, home_id, away_id):
                by_time.setdefault((g["period"], g["mmss"]), []).append(g)
            new_goals = []
            for g in goals:
                key = (g["period"], g["mmss"])
                cand = (by_time.get(key) or [None])[0]
                if cand:
                    g["en_scorer"]  = cand.get("en_scorer") or g["en_scorer"]
                    g["en_assists"] = cand.get("en_assists") or g["en_assists"]
                new_goals.append(g)
            goals = new_goals

    return goals, shootout

# ----------------------- SPORTS.RU (опционально) -----------------------
EV_STRICT = re.compile(
    r"^\s*\d+\s*:\s*\d+\s*[–-]\s*(?P<mm>\d{1,2})[.:](?P<ss>[0-5]\d)\s+(?P<sc>[А-ЯЁ][А-Яа-яЁё \-’'`]+?)(?:\s*\((?P<ast>[^)]+)\))?\s*$"
)
EV_BACKUP = re.compile(
    r"(?P<mm>\d{1,2})[.:](?P<ss>[0-5]\d)\s+(?P<sc>[А-ЯЁ][А-Яа-яЁё \-’'`]+?)(?:\s*\((?P<ast>[^)]+)\))"
)
CYR_NAME = re.compile(r"[А-ЯЁ][а-яё]+(?:[-’'`][А-ЯЁ]?[а-яё]+)*")

def sanitize_name(s: str) -> str:
    s = s.strip()
    m = CYR_NAME.findall(s)
    if not m: return ""
    cand = m[-1]
    if cand in BAD_TOKENS: return ""
    if not CYR.match(cand): return ""
    return cand

def parse_assists(s: str) -> List[str]:
    if not s: return []
    parts = re.split(r"[;,]", s)
    out = []
    for p in parts:
        fam = sanitize_name(p)
        if fam and fam not in out:
            out.append(fam)
    return out

def strict_event_lines(soup: BeautifulSoup) -> List[str]:
    blocks = []
    selectors = [
        ".match__live",".match__events",".live-block",".live",".transText",
        "ul li",".article__content",".material-body",".match__text"
    ]
    for sel in selectors:
        blocks += soup.select(sel)
    text = "\n".join(b.get_text("\n", strip=True) for b in blocks) or soup.get_text("\n", strip=True)
    lines = []
    for ln in (ln.strip() for ln in text.splitlines()):
        if not ln: continue
        if (" – " in ln or " - " in ln) and EV_STRICT.match(ln):
            lines.append(ln)
    return lines

def backup_event_chunks(soup: BeautifulSoup) -> List[str]:
    text = soup.get_text("\n", strip=True)
    return [t.strip() for t in re.split(r"[\n]+", text) if t.strip()]

def sportsru_fetch_goals(slug: str) -> List[Dict[str, Any]]:
    goals: List[Dict[str, Any]] = []
    tried_urls = []
    for suffix in ("/lineups/", "/"):
        url = f"https://www.sports.ru/hockey/match/{slug}{suffix}"
        tried_urls.append(url)
        try:
            html = http_get(url).text
            soup = BeautifulSoup(html, "html.parser")
            lines = strict_event_lines(soup)
            for ln in lines:
                m = EV_STRICT.match(ln)
                if not m: continue
                mmss = f"{int(m.group('mm')):02d}:{int(m.group('ss')):02d}"
                sc = sanitize_name(m.group("sc"))
                ast = parse_assists(m.group("ast") or "")
                goals.append({"mmss": mmss, "scorer": sc or "", "assists": ast})

            if not goals:
                # бэкап — вытащить из общего текста
                chunks = backup_event_chunks(soup)
                for ch in chunks:
                    for m in EV_BACKUP.finditer(ch):
                        mmss = f"{int(m.group('mm')):02d}:{int(m.group('ss')):02d}"
                        sc = sanitize_name(m.group("sc"))
                        ast = parse_assists(m.group("ast") or "")
                        if sc or ast:
                            goals.append({"mmss": mmss, "scorer": sc or "", "assists": ast})

            if goals:
                # убрать дубли по времени, оставить запись с автором
                best: Dict[str, Dict[str, Any]] = {}
                for ev in goals:
                    key = ev["mmss"]
                    old = best.get(key)
                    if (not old) or (ev.get("scorer") and not old.get("scorer")):
                        best[key] = ev
                clean = list(best.values())
                dbg(f"sports.ru goals for {slug}: {len(clean)}")
                return clean
        except Exception as e:
            dbg("sports.ru fetch fail", url, repr(e))
    return []

def _slugify_city(team: Dict[str, Any]) -> str:
    city = _val(team.get("placeName")) or _val(team.get("city")) or _val(team.get("name")) or ""
    s = city.lower()
    s = s.replace("st. ", "st-").replace("st ", "st-")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s

def sportsru_match_goals(home_team: Dict[str, Any], away_team: Dict[str, Any]) -> List[Dict[str, Any]]:
    h, a = (_val(home_team.get("abbrev")) or "").upper(), (_val(away_team.get("abbrev")) or "").upper()
    tried: List[str] = []
    def add_pair(x, y):
        tried.extend([f"{x}-vs-{y}", f"{y}-vs-{x}"])

    if h in SPORTSRU_SLUG_BY_ABBR and a in SPORTSRU_SLUG_BY_ABBR:
        add_pair(SPORTSRU_SLUG_BY_ABBR[h], SPORTSRU_SLUG_BY_ABBR[a])

    ch, ca = _slugify_city(home_team), _slugify_city(away_team)
    if ch and ca: add_pair(ch, ca)

    dbg("sports.ru slugs tried:", tried)
    for slug in tried:
        evs = sportsru_fetch_goals(slug)
        if evs: return evs
    return []

# ----------------------- СОВМЕЩЕНИЕ ИМЁН -----------------------
def attach_names(nhl_goals: List[Dict[str, Any]], sr_goals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Приоритет sports.ru; иначе транслит из EN."""
    def to_sec(mmss: str) -> int:
        mm, ss = mmss.split(":"); return int(mm)*60 + int(ss)

    # 1) индексируем sports.ru по секундах
    bysec: Dict[int, List[Dict[str, Any]]] = {}
    for ev in sr_goals:
        bysec.setdefault(to_sec(ev["mmss"]), []).append(ev)

    tolerances = (2, 4, 7, 10, 15)
    used = set()
    out = []
    for g in nhl_goals:
        tgt = to_sec(g["mmss"])
        pick = None
        for tol in tolerances:
            for s in range(tgt - tol, tgt + tol + 1):
                for i, ev in enumerate(bysec.get(s) or []):
                    key = (s, i)
                    if key in used: continue
                    pick = ev; used.add(key); break
                if pick: break
            if pick: break

        g2 = dict(g)
        if pick:
            sc_ru = pick.get("scorer") or ""
            ast_ru = pick.get("assists") or []
            g2["ru_scorer"] = sc_ru if sc_ru and sc_ru not in BAD_TOKENS else "—"
            g2["ru_assists"] = [a for a in ast_ru if a and a not in BAD_TOKENS]
        else:
            # транслит из EN
            sc_en = g.get("en_scorer") or ""
            ast_en = g.get("en_assists") or []
            g2["ru_scorer"] = en_full_to_ru_last(sc_en) if sc_en else "—"
            g2["ru_assists"] = [en_full_to_ru_last(a) for a in ast_en if a]
        out.append(g2)
    return out

# ----------------------- СТАНДИНГИ -----------------------
def fetch_records(date_hint: Optional[date] = None) -> Dict[str, Dict[str, int]]:
    urls = ["https://api-web.nhle.com/v1/standings/now"]
    if date_hint: urls.append(f"https://api-web.nhle.com/v1/standings/{date_hint.isoformat()}")
    for url in urls:
        try:
            j = http_json(url)
            table = {}
            rows = j.get("standings") or j.get("records") or []
            for r in rows:
                abbr = (_val(r.get("teamAbbrev")) or _val((r.get("team") or {}).get("abbrev")) or "").upper()
                if not abbr: continue
                table[abbr] = {
                    "w": int(r.get("wins", 0)),
                    "l": int(r.get("losses", 0)),
                    "ot": int(r.get("otLosses", 0)),
                    "pts": int(r.get("points", r.get("pts", 0)))
                }
            if table:
                dbg("records loaded:", len(table))
                return table
            dbg("records empty from", url)
        except Exception as e:
            dbg("records error:", repr(e))
    return {}

# ----------------------- ФОРМАТИРОВАНИЕ -----------------------
def team_emoji(abbr: str) -> str: return DEFAULT_EMOJI.get(abbr, "🏒")
def team_ru(abbr: str) -> str: return RU_TEAMS.get(abbr, abbr)
def format_record(rec: Optional[Dict[str, int]]) -> str:
    return "" if not rec else f" ({rec['w']}-{rec['l']}-{rec['ot']}, {rec['pts']} о.)"
def period_header(p: int, pt: str) -> str:
    if pt == "SHOOTOUT": return "Буллиты"
    if p <= 3: return f"{p}-й период"
    return f"Овертайм №{p-3}"
def _fmt_mmdotss(mmss: str) -> str:
    mm, ss = mmss.split(":"); return f"{int(mm):02d}.{int(ss):02d}"
def format_goal_line(g: Dict[str, Any]) -> str:
    assists = g.get("ru_assists") or []
    ast = f" ({', '.join(assists)})" if assists else ""
    scorer = g.get("ru_scorer") or "—"
    return f"{g['home']}:{g['away']} – {_fmt_mmdotss(g['mmss'])} {scorer}{ast}"

# ----------------------- ВЫВОД -----------------------
def build_message(base_date: date, games: List[Dict[str, Any]], records: Dict[str, Dict[str, int]]) -> str:
    lines: List[str] = []
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(datetime(base_date.year, base_date.month, base_date.day))} • {len(games)} матчей"
    lines += [title, "", "Результаты надёжно спрятаны 👇", "", "——————————————————", ""]

    for g in games:
        gid = g.get("id") or g.get("gamePk") or g.get("gameNumber")
        home, away = team_info(g)
        h_abbr = (_val(home.get("abbrev")) or "").upper()
        a_abbr = (_val(away.get("abbrev")) or "").upper()
        h_id, a_id = int(home.get("id") or 0), int(away.get("id") or 0)

        fh, fa = int(home.get("score") or 0), int(away.get("score") or 0)

        lines.append(f"{team_emoji(h_abbr)} «{team_ru(h_abbr)}»: {fh}{format_record(records.get(h_abbr))}")
        lines.append(f"{team_emoji(a_abbr)} «{team_ru(a_abbr)}»: {fa}{format_record(records.get(a_abbr))}")
        lines.append("")

        try:
            pbp = fetch_pbp(int(gid))
        except Exception as e:
            err("pbp fetch fail", gid, repr(e))
            lines.append("— события матча недоступны\n")
            continue

        goals, shootout = extract_goals_from_pbp(pbp, h_id, a_id)

        sr_goals = []
        try:
            sr_goals = sportsru_match_goals(home, away)
        except Exception as e:
            dbg("sports.ru parse fail:", repr(e))

        goals = attach_names(goals, sr_goals)

        if not goals and not shootout:
            lines.append("— события матча недоступны\n")
            continue

        grouped: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
        for gg in goals:
            grouped.setdefault((gg["period"], gg["periodType"]), []).append(gg)

        for (pnum, ptype) in sorted(grouped.keys()):
            lines.append(period_header(pnum, ptype))
            for gg in grouped[(pnum, ptype)]:
                lines.append(format_goal_line(gg))
            lines.append("")

    return "\n".join(lines).strip() + "\n"

# ----------------------- TELEGRAM -----------------------
def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT:
        dbg("Telegram env not set; output follows:\n" + text); return
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
        try: base_date = datetime.fromisoformat(force).date()
        except Exception: err("REPORT_DATE_MSK must be YYYY-MM-DD"); sys.exit(1)
    else:
        base_date = datetime.now(MSK).date()

    wnd_start, wnd_end = msk_window_for_date(base_date)
    dbg("MSK window:", wnd_start.isoformat(), "→", wnd_end.isoformat())

    sched = []
    for d in [base_date - timedelta(days=1), base_date, base_date + timedelta(days=1)]:
        try: sched += fetch_schedule(d)
        except Exception as e: err("schedule fetch fail", d, repr(e))

    completed = filter_completed_in_window(sched, wnd_start, wnd_end)
    completed.sort(key=lambda g: game_start_msk(g) or wnd_start)
    dbg("Collected unique FINAL games:", len(completed))

    records = fetch_records(base_date if completed else None)
    msg = build_message(base_date, completed, records)

    try:
        tg_send(msg); dbg("Telegram OK")
    except Exception as e:
        err(repr(e)); print(msg); sys.exit(1 if TG_TOKEN and TG_CHAT else 0)

if __name__ == "__main__":
    main()
