#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results → Telegram (RU) — names only from sports.ru/hockey/person/{first-last}

• NHL JSON:
  - Schedule/Score:   https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
                      (fallback: /v1/score, /v1/scoreboard)
  - Play-by-Play:     https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play
  - Boxscore:         https://api-web.nhle.com/v1/gamecenter/{gameId}/boxscore
  - Player landing:   https://api-web.nhle.com/v1/player/{playerId}/landing

• Русские имена:
  - Строго sports.ru: https://www.sports.ru/hockey/person/{first-last}/
  - Приоритет: <h1 class="titleH1"> → <h1> → og:title → <title>, только если есть кириллица.
  - НЕТ кириллицы → считаем НЕ НАЙДЕНО, падаем с ошибкой, пост не отправляем.
  - Кэш удачных:  ru_names_sportsru.json  → { "<id>": {"ru_first","ru_last","url"} }
  - Проблемные:   ru_pending_sportsru.json → [{id, first, last, tried_slugs[]}]

• Время голов: ММ.СС от старта матча (ОТ → 60+, SO → 65.00).
• Буллиты: только «Победный буллит».

Требования:
  requests==2.32.3
  beautifulsoup4==4.12.3
"""

import os, sys, re, json, time, unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------- ENV ----------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

API = "https://api-web.nhle.com/v1"

# -------------- HTTP -----------------
def make_session():
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.7,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"], raise_on_status=False
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-DailyResultsBot/sportsru-h1-only/1.4",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

def log(*a): print(*a, file=sys.stderr)

# ------------- Dates -----------------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"

def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 7 else now_et.date()

# ------------- Teams -----------------
TEAM_RU = {
    "ANA": ("Анахайм","🦆"), "ARI": ("Аризона","🤠"), "BOS": ("Бостон","🐻"), "BUF": ("Баффало","🦬"),
    "CGY": ("Калгари","🔥"), "CAR": ("Каролина","🌪️"), "COL": ("Колорадо","⛰️"), "CBJ": ("Коламбус","💣"),
    "DAL": ("Даллас","⭐"), "DET": ("Детройт","🔴"), "EDM": ("Эдмонтон","🛢️"), "FLA": ("Флорида","🐆"),
    "LAK": ("Лос-Анджелес","👑"), "MIN": ("Миннесота","🌲"), "MTL": ("Монреаль","🇨🇦"), "NSH": ("Нэшвилл","🐯"),
    "NJD": ("Нью-Джерси","😈"), "NYI": ("Айлендерс","🟠"), "NYR": ("Рейнджерс","🗽"), "OTT": ("Оттава","🛡"),
    "PHI": ("Филадельфия","🛩"), "PIT": ("Питтсбург","🐧"), "SJS": ("Сан-Хосе","🦈"), "SEA": ("Сиэтл","🦑"),
    "STL": ("Сент-Луис","🎵"), "TBL": ("Тампа-Бэй","⚡"), "TOR": ("Торонто","🍁"), "VAN": ("Ванкувер","🐳"),
    "VGK": ("Вегас","🎰"), "WSH": ("Вашингтон","🦅"), "WPG": ("Виннипег","✈️"), "UTA": ("Юта","🦣"),
    "CHI": ("Чикаго","🦅"),
}
def team_ru_and_emoji(abbr: str) -> tuple[str,str]:
    return TEAM_RU.get((abbr or "").upper(), ((abbr or "").upper(),"🏒"))

# ------------- Time helpers ---------
def abs_seconds(period_index: int, sec_in_period: int) -> int:
    if period_index == 5:   # SO
        return 65*60 + (sec_in_period or 0)
    if period_index >= 4:   # OT
        return 60*60 + (sec_in_period or 0)
    return (period_index - 1)*20*60 + (sec_in_period or 0)

# ------------- Schedule -------------
def fetch_games_for_date(day: date) -> list[dict]:
    out = []
    def eat(bucket_games):
        for g in bucket_games:
            if str(g.get("gameState","")).upper() not in {"OFF","FINAL"}:
                continue
            hm, aw = g.get("homeTeam",{}) or {}, g.get("awayTeam",{}) or {}
            pd = (g.get("periodDescriptor") or {})
            out.append({
                "gameId": int(g.get("id") or g.get("gameId")),
                "homeAbbrev": (hm.get("abbrev") or hm.get("triCode") or "").upper(),
                "awayAbbrev": (aw.get("abbrev") or aw.get("triCode") or "").upper(),
                "homeScore": int(hm.get("score", 0)),
                "awayScore": int(aw.get("score", 0)),
                "periodType": (pd or {}).get("periodType") or (g.get("periodType") or ""),
                "homeId": int(hm.get("id") or hm.get("teamId") or 0),
                "awayId": int(aw.get("id") or aw.get("teamId") or 0),
            })
    j = _get_json(f"{API}/schedule/{day.isoformat()}")
    for bucket in j.get("gameWeek", []):
        if bucket.get("date") == day.isoformat():
            eat(bucket.get("games") or [])
    if not out:
        j = _get_json(f"{API}/score/{day.isoformat()}"); eat(j.get("games") or [])
    if not out:
        j = _get_json(f"{API}/scoreboard/{day.isoformat()}"); eat(j.get("games") or [])
    return out

# ------------- Boxscore -------------
_en_name_cache: dict[int, tuple[str,str]] = {}
_display_cache: dict[int, str] = {}

def _extract_names_from_player_obj(p: dict) -> tuple[str,str,str]:
    first = p.get("firstName"); last = p.get("lastName")
    if isinstance(first, dict): first = first.get("default") or ""
    if isinstance(last, dict):  last  = last.get("default") or ""
    first = (first or "").strip(); last = (last or "").strip()
    disp = ""
    for key in ("firstInitialLastName","playerName","name","playerNameWithNumber","fullName"):
        v = p.get(key)
        if isinstance(v, dict): v = v.get("default") or ""
        if v and not disp: disp = str(v).strip()
    if (not first or not last) and disp:
        parts = [x for x in re.split(r"\s+", disp.replace("#"," ").strip()) if x and x != "-"]
        if len(parts) >= 2:
            last  = last  or parts[-1]
            first = first or parts[0].replace(".","")
    return first, last, disp

def fetch_box_map(game_id: int) -> dict[int, dict]:
    data = _get_json(f"{API}/gamecenter/{game_id}/boxscore") or {}
    out = {}
    def eat(team_block: dict):
        for grp in ("forwards","defense","goalies"):
            for p in team_block.get(grp, []) or []:
                pid = p.get("playerId")
                if not pid: continue
                pid = int(pid)
                f,l,d = _extract_names_from_player_obj(p)
                out[pid] = {"firstName": f, "lastName": l}
                if f or l: _en_name_cache[pid] = (f,l)
                if d: _display_cache[pid] = d
    stats = data.get("playerByGameStats",{}) or {}
    eat(stats.get("homeTeam",{}) or {})
    eat(stats.get("awayTeam",{}) or {})
    return out

# ---- Player landing helpers ----
def fetch_player_en_name_force(pid: int) -> tuple[str,str]:
    """Игнорирует кэш и тянет полное имя из landing."""
    j = _get_json(f"{API}/player/{pid}/landing") or {}
    fn, ln = j.get("firstName"), j.get("lastName")
    if isinstance(fn, dict): fn = fn.get("default") or ""
    if isinstance(ln, dict): ln = ln.get("default") or ""
    fn, ln = (fn or "").strip(), (ln or "").strip()
    # обновим кэш нормальным именем
    if fn or ln:
        _en_name_cache[pid] = (fn, ln)
    return fn, ln

def fetch_player_en_name(pid: int) -> tuple[str,str]:
    """Обычная версия, использует кэш; если в кэше инициала — дёргает force."""
    if pid in _en_name_cache:
        fn, ln = _en_name_cache[pid]
        if (not fn) or len(fn) <= 2 or re.fullmatch(r"[A-Za-z]\.?", fn or ""):
            return fetch_player_en_name_force(pid)
        return (fn or "").strip(), (ln or "").strip()
    return fetch_player_en_name_force(pid)

def ensure_full_en_name(pid: int, first: str, last: str) -> tuple[str,str]:
    """
    Если first похож на инициалу (1–2 символа) либо пуст, или last пуст —
    принудительно тянем landing, игнорируя кэш-инициалы.
    """
    need = False
    if not first or len(first) <= 2 or re.fullmatch(r"[A-Za-z]\.?", first or ""):
        need = True
    if not last:
        need = True
    if need:
        f2, l2 = fetch_player_en_name_force(pid)
        first = f2 or first
        last  = l2 or last
    return (first or "").strip(), (last or "").strip()

# --------- sports.ru lookup (STRICT) ---------
RU_CACHE_PATH   = "ru_names_sportsru.json"     # id -> {ru_first, ru_last, url}
RU_PENDING_PATH = "ru_pending_sportsru.json"   # [{"id","first","last","tried_slugs":[]}]

RU_CACHE: dict[str, dict] = {}
RU_PENDING: list[dict] = []
RU_MISSES: list[dict] = []
_session_pending_ids: set[int] = set()
_attempted_slugs: set[str] = set()

SPORTS_ROOT = "https://www.sports.ru/hockey/person/"

def _norm_ascii(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = s.replace("'", "").replace("’","").replace("."," ").replace("/"," ")
    s = re.sub(r"[^a-z0-9\\- ]+", " ", s)
    s = re.sub(r"\\s+", "-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
    return s

def _slug_variants(first: str, last: str) -> list[str]:
    f = _norm_ascii(first)
    l = _norm_ascii(last)
    cand = []
    if f and l:
        cand.append(f"{f}-{l}")                     # leon-draisaitl
        if "-" in l: cand.append(f"{f}-{l.replace('-', '')}")   # nugent-hopkins -> nugenthopkins
        if "-" in f: cand.append(f"{f.replace('-', '')}-{l}")
    if l:
        cand.append(l)                              # просто фамилия
    out, seen = [], set()
    for c in cand:
        if c and c not in seen:
            out.append(c); seen.add(c)
    return out

def _has_cyrillic(s: str) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", s or ""))

def _extract_ru_pair_from_text(t: str) -> tuple[str,str] | None:
    if not t: return None
    t = re.split(r"\\s[–—\\-|]\\s", t)[0]
    t = re.sub(r"\\(.*?\\)", "", t)
    t = " ".join(t.split()).strip()
    words = [w for w in t.split() if re.match(r"^[А-ЯЁ][а-яё\\-]+$", w)]
    if len(words) >= 2:
        return words[0], words[-1]
    return None

def _sportsru_fetch_ru_name_by_slug(slug: str) -> tuple[str,str,str] | None:
    if not slug or slug in _attempted_slugs:
        return None
    _attempted_slugs.add(slug)

    url = SPORTS_ROOT + slug + "/"
    try:
        r = S.get(url, timeout=20, allow_redirects=True)
    except Exception:
        return None
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    h1t = soup.select_one("h1.titleH1")
    if h1t:
        txt = h1t.get_text(" ", strip=True)
        pair = _extract_ru_pair_from_text(txt)
        if pair and _has_cyrillic(" ".join(pair)):
            rf, rl = pair
            return rf, rl, r.url

    h1 = soup.find("h1")
    if h1:
        txt = h1.get_text(" ", strip=True)
        pair = _extract_ru_pair_from_text(txt)
        if pair and _has_cyrillic(" ".join(pair)):
            rf, rl = pair
            return rf, rl, r.url

    og = soup.find("meta", attrs={"property":"og:title"})
    if og and og.get("content"):
        pair = _extract_ru_pair_from_text(og["content"])
        if pair and _has_cyrillic(" ".join(pair)):
            rf, rl = pair
            return rf, rl, r.url

    title = soup.find("title")
    if title:
        pair = _extract_ru_pair_from_text(title.get_text(" ", strip=True))
        if pair and _has_cyrилlic(" ".join(pair)):
            rf, rl = pair
            return rf, rl, r.url

    return None

def _queue_pending(pid: int, first: str, last: str, tried: list[str]):
    if not pid or pid in _session_pending_ids:
        return
    RU_PENDING.append({"id": pid, "first": first or "", "last": last or "", "tried_slugs": tried})
    RU_MISSES.append({"id": pid, "first": first or "", "last": last or "", "tried_slugs": tried})
    _session_pending_ids.add(pid)

def ru_initial_from_sportsru(pid: int, en_first: str, en_last: str, display: str | None) -> str:
    """
    Возвращает «И. Фамилия» ТОЛЬКО если найдена кириллица на sports.ru.
    Если нет — добавляет в pending/misses и возвращает спец-маркер.
    """
    cached = RU_CACHE.get(str(pid))
    if cached:
        ini = (cached.get("ru_first","")[:1] or "?")
        return f"{ini}. {cached.get('ru_last','')}"

    # ВАЖНО: гарантируем полные имена перед построением слуга (игнорируем кэш-инициалы)
    en_first, en_last = ensure_full_en_name(pid, en_first, en_last)

    tried = _slug_variants(en_first, en_last)
    for slug in tried:
        got = _sportsru_fetch_ru_name_by_slug(slug)
        if got:
            ru_first, ru_last, url = got
            RU_CACHE[str(pid)] = {"ru_first": ru_first, "ru_last": ru_last, "url": url}
            ini = (ru_first[:1] or "?")
            return f"{ini}. {ru_last}"

    _queue_pending(pid, en_first, en_last, tried)
    return "[имя не найдено]"

# ------------- Play-by-play (goals) -------------
def fetch_goals(game_id: int) -> list[dict]:
    data = _get_json(f"{API}/gamecenter/{game_id}/play-by-play") or {}
    plays = data.get("plays", []) or []
    out = []
    for ev in plays:
        if ev.get("typeDescKey") != "goal":
            continue
        det = ev.get("details", {}) or {}
        pd  = ev.get("periodDescriptor", {}) or {}

        time_in = str(ev.get("timeInPeriod") or det.get("timeInPeriod") or "0:00")
        try:
            m, s = time_in.split(":"); sec_in = int(m)*60 + int(s)
        except Exception:
            sec_in = 0

        pt = (pd.get("periodType") or "").upper()
        num = pd.get("number") or 1
        if pt == "OT": pidx = 4
        elif pt == "SO": pidx = 5
        else: pidx = max(1, int(num))

        if pidx == 5:      totsec = 65*60 + sec_in
        elif pidx >= 4:    totsec = 60*60 + sec_in
        else:              totsec = (pidx - 1)*20*60 + sec_in

        sid = det.get("scoringPlayerId")
        a1  = det.get("assist1PlayerId") or det.get("secondaryAssistPlayerId")
        a2  = det.get("assist2PlayerId") or det.get("tertiaryAssistPlayerId")
        team_id = det.get("eventOwnerTeamId") or ev.get("teamId") or det.get("teamId")
        try: team_id = int(team_id) if team_id is not None else None
        except Exception: team_id = None

        players = ev.get("playersInvolved") or []
        if (not sid) and players:
            for p in players:
                tpe = (p.get("playerType") or "").lower()
                if tpe == "scorer": sid = p.get("playerId")
                elif tpe == "assist":
                    if not a1: a1 = p.get("playerId")
                    elif not a2: a2 = p.get("playerId")

        out.append({
            "period": pidx, "sec": sec_in, "totsec": totsec,
            "home": int(det.get("homeScore", 0)), "away": int(det.get("awayScore", 0)),
            "scorerId": int(sid) if sid else None,
            "a1": int(a1) if a1 else None, "a2": int(a2) if a2 else None,
            "periodType": pt,
            "playersInvolved": players,
            "teamId": team_id,
        })
    out.sort(key=lambda x: (x["period"], x["sec"]))
    return out

# -------- name resolver per event -------
def resolve_player_display(pid: int, boxmap: dict, players_involved: list) -> str:
    # 1) boxscore
    if pid and pid in boxmap:
        f = boxmap[pid].get("firstName",""); l = boxmap[pid].get("lastName","")
        d = _display_cache.get(pid)
        f, l = ensure_full_en_name(pid, f, l)  # ← гарантируем полные имена
        return ru_initial_from_sportsru(pid, f, l, d)
    # 2) playersInvolved
    for p in (players_involved or []):
        if p.get("playerId") == pid:
            f,l,d = _extract_names_from_player_obj(p)
            f, l = ensure_full_en_name(pid, f, l)
            return ru_initial_from_sportsru(pid, f, l, d)
    # 3) landing напрямую
    f,l = fetch_player_en_name_force(pid)  # ← сразу force, чтобы точно не инициалы
    return ru_initial_from_sportsru(pid, f, l, None)

# ------------- Game block ---------------
def build_game_block(game: dict) -> str:
    gid = game["gameId"]
    home_ab, away_ab = game["homeAbbrev"], game["awayAbbrev"]
    home_ru, emh = team_ru_and_emoji(home_ab)
    away_ru, ema = team_ru_and_emoji(away_ab)
    hs, as_ = game["homeScore"], game["awayScore"]

    goals = fetch_goals(gid)
    box   = fetch_box_map(gid)

    last_pt = (goals[-1].get("periodType") if goals else "") or game.get("periodType") or ""
    suffix = " (ОТ)" if last_pt == "OT" else (" (Б)" if last_pt == "SO" else "")

    head = f"{emh} «{home_ru}»: {hs}\n{ema} «{away_ru}»: {as_}{suffix}\n\n"

    if not goals:
        return head + "— подробная запись голов недоступна"

    reg_goals = [g for g in goals if g["period"] != 5]
    so_goals  = [g for g in goals if g["period"] == 5]

    lines = []
    current_period = None

    for g in reg_goals:
        if g["period"] != current_period:
            current_period = g["period"]
            if lines: lines.append("")
            if current_period <= 3:
                lines.append(f"<i>{current_period}-й период</i>")
            else:
                lines.append(f"<i>Овертайм №{current_period-3}</i>")

        scorer = resolve_player_display(g["scorerId"], box, g.get("playersInvolved"))
        a1 = resolve_player_display(g["a1"], box, g.get("playersInvolved")) if g.get("a1") else None
        a2 = resolve_player_display(g["a2"], box, g.get("playersInvolved")) if g.get("a2") else None

        assists = []
        if a1: assists.append(a1)
        if a2: assists.append(a2)
        ast_txt = f" ({', '.join(assists)})" if assists else ""

        mm = g["totsec"] // 60
        ss = g["totsec"] % 60
        t_abs = f"{mm}.{ss:02d}"

        # аккуратный пробел после «И.»
        scorer = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", scorer)
        ast_txt = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", ast_txt)

        lines.append(f"{g['home']}:{g['away']} – {t_abs} {scorer}{ast_txt}")

    if so_goals:
        winner_team_id = (game["homeId"] if hs > as_ else game["awayId"] if as_ > hs else None)
        winning_shot = None
        if winner_team_id:
            for g in reversed(so_goals):
                if g.get("teamId") == winner_team_id:
                    winning_shot = g
                    break
        if not winning_shot:
            winning_shot = so_goals[-1]

        lines.append("")
        lines.append("<i>Победный буллит</i>")
        scorer = resolve_player_display(winning_shot.get("scorerId"), box, winning_shot.get("playersInvolved"))
        scorer = re.sub(r"\.([A-Za-zА-Яа-я])", r". \1", scorer)
        mm = winning_shot["totsec"] // 60
        ss = winning_shot["totsec"] % 60
        t_abs = f"{mm}.{ss:02d}"
        lines.append(f"{winning_shot['home']}:{winning_shot['away']} – {t_abs} {scorer}")

    return head + "\n".join(lines)

# ------------- Full post ---------------
def build_post(day: date) -> str:
    games = fetch_games_for_date(day)
    for shift in (1,2):
        if games: break
        d2 = day - timedelta(days=shift)
        g2 = fetch_games_for_date(d2)
        if g2:
            day, games = d2, g2
            break

    n = len(games)
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(day)} • {n} " + \
            ("матч" if n==1 else "матча" if n%10 in (2,3,4) and not 12<=n%100<=14 else "матчей")
    title += "\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    if not games:
        return title.strip()

    blocks = []
    for i,g in enumerate(games,1):
        try:
            blocks.append(build_game_block(g))
        except Exception as e:
            log("[WARN game]", g.get("gameId"), e)
            hr, emh = team_ru_and_emoji(g["homeAbbrev"])
            ar, ema = team_ru_and_emoji(g["awayAbbrev"])
            blocks.append(f"{emh} «{hr}»: {g['homeScore']}\n{ema} «{ar}»: {g['awayScore']}\n\n— события матча недоступны")
        if i < len(games): blocks.append("")
    return title + "\n".join(blocks).strip()

# ------------- Telegram ---------------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    MAX = 3500
    t = text
    parts=[]
    while t:
        if len(t) <= MAX: parts.append(t); break
        cut = t.rfind("\n\n", 0, MAX)
        if cut == -1: cut = MAX
        parts.append(t[:cut]); t = t[cut:].lstrip()
    for part in parts:
        r = S.post(url, json={"chat_id": CHAT_ID, "text": part, "parse_mode": "HTML",
                              "disable_web_page_preview": True}, timeout=25)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")
        time.sleep(0.25)

# ------------- I/O helpers ------------
def _load_json(path: str, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def _save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ---------------- Main ----------------
RU_CACHE_PATH   = "ru_names_sportsru.json"
RU_PENDING_PATH = "ru_pending_sportsru.json"

if __name__ == "__main__":
    try:
        # загрузка кэшей
        cached = _load_json(RU_CACHE_PATH, {})
        if isinstance(cached, dict): RU_CACHE.update(cached)
        pend = _load_json(RU_PENDING_PATH, [])
        if isinstance(pend, list): RU_PENDING.extend(pend)

        d = pick_report_date()
        text = build_post(d)

        # если есть хотя бы один игрок без кириллицы — падаем и ничего не отправляем
        if RU_MISSES:
            _save_json(RU_CACHE_PATH, RU_CACHE)
            _save_json(RU_PENDING_PATH, RU_PENDING)
            sample = "\n".join(
                f"- id={m['id']} | {m['first']} {m['last']} | tried: {', '.join(m.get('tried_slugs', [])[:4])}"
                for m in RU_MISSES[:20]
            )
            raise RuntimeError(
                "Не удалось получить имена на кириллице для некоторых игроков sports.ru.\n"
                "Список (первые):\n" + sample +
                ("\n… (см. ru_pending_sportsru.json)" if len(RU_MISSES) > 20 else "")
            )

        # всё хорошо — отправляем
        tg_send(text)

        _save_json(RU_CACHE_PATH, RU_CACHE)
        _save_json(RU_PENDING_PATH, RU_PENDING)

        print(f"OK — RU names cached: {len(RU_CACHE)}, pending: {len(RU_PENDING)}")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
