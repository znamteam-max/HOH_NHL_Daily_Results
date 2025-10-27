#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NHL Daily Results -> Telegram (RU) via championat.com

Источник:
  - Календарь: https://www.championat.com/hockey/_nhl/tournament/6606/calendar/
  - Страница матча: /hockey/_nhl/xxxxxx/match/zzzzzz.html (внутри — голы, ассисты, буллиты на русском)

Правила включения матчей:
  - Для поста от даты D (по МСК) берём:
      * все матчи с датой начала = D (МСК),
      * все матчи с датой начала = D-1, у которых время старта >= 15:00 МСК.
Формат событий:
  - «Счёт – MM.SS Автор (Ассистент1, Ассистент2)»
  - Время — абсолютное от старта матча (1-й период 0–20, 2-й 20–40, 3-й 40–60, ОТ — 60+)
  - Серия буллитов: только «Победный буллит» и автор.

Требуются переменные окружения:
  - TELEGRAM_BOT_TOKEN
  - TELEGRAM_CHAT_ID
Опционально:
  - REPORT_DATE=YYYY-MM-DD  (дата поста по МСК)

Зависимости:
  requests==2.32.3
  beautifulsoup4==4.12.3
"""

import os, re, sys, time, json, random, datetime as dt
from zoneinfo import ZoneInfo
from html import escape

import requests
from bs4 import BeautifulSoup

# ───── Константы/конфиг
CAL_URL = "https://www.championat.com/hockey/_nhl/tournament/6606/calendar/"
BASE    = "https://www.championat.com"
TZ_MSK  = ZoneInfo("Europe/Moscow")
TZ_CHAT = TZ_MSK  # заголовок дат тоже по МСК

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
REPORT_DATE_ENV = os.getenv("REPORT_DATE", "").strip()

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/125.0 Safari/537.36")

JITTER = (0.4, 0.9)

# ───── HTTP
S = requests.Session()
S.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
})

def jitter():
    time.sleep(random.uniform(*JITTER))

def get_html(url: str) -> str:
    try:
        r = S.get(url, timeout=25)
        if r.status_code == 200:
            return r.text
        # иногда страница вешается — повтор
        if r.status_code in (403, 429, 500, 502, 503, 504):
            jitter()
            r2 = S.get(url, timeout=25)
            if r2.status_code == 200:
                return r2.text
        print(f"[WARN] GET {url} -> {r.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"[ERR ] GET {url} -> {e}", file=sys.stderr)
    return ""

# ───── Даты/текст
RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}
def ru_date(d: dt.date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def pick_report_date() -> dt.date:
    if REPORT_DATE_ENV:
        try:
            return dt.date.fromisoformat(REPORT_DATE_ENV)
        except Exception:
            pass
    now = dt.datetime.now(TZ_MSK)
    return now.date()

# ───── Утилиты форматирования
def initial_ru(full_ru: str) -> str:
    """
    «Имя Фамилия»/«Имя-Имя Фамилия-Фамилия» -> «И. Фамилия-Фамилия»
    Если фамилия многословная — берём последний токен (с дефисами сохраняем).
    """
    t = (full_ru or "").strip()
    if not t:
        return ""
    # уберём лишние пробелы
    t = re.sub(r"\s+", " ", t)
    parts = t.split(" ")
    if len(parts) == 1:
        # бывает только фамилия
        fam = parts[0]
        return fam
    ini = parts[0][0] + "."
    fam = parts[-1]
    return f"{ini} {fam}"

def abs_time_from_period(period: int, mmss: str) -> str:
    """Преобразует время периода (MM:SS) в абсолютное MM.SS (1п=0–20, 2п=20–40...)"""
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", mmss)
    if not m:
        return mmss.replace(":", ".")
    mm, ss = int(m.group(1)), int(m.group(2))
    if period >= 4:  # OT
        return f"{60 + mm}.{ss:02d}"
    return f"{(period-1)*20 + mm}.{ss:02d}"

def bold_winner_line(name: str, score: int, winner: bool) -> str:
    s = f"{escape(name)}: {score}"
    return f"<b>{s}</b>" if winner else s

# ───── Парсинг календаря ⇒ ссылки матчей
def find_match_links_for_date_range(cal_html: str, dates_keep: set[dt.date]) -> list[str]:
    """
    Из HTML календаря вытаскивает все ссылки матчей '/hockey/_nhl/.../match/....html'
    и отфильтровывает их по датам начала из самой страницы матча (по МСК).
    Чтобы не полагаться на структуру календаря, время матча берём с match-страницы.
    """
    soup = BeautifulSoup(cal_html, "html.parser")
    # все ссылки на матчи
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/match/" in href and href.endswith(".html"):
            if not href.startswith("http"):
                href = BASE + href
            links.append(href)
    # уникализируем
    seen, uniq = set(), []
    for u in links:
        if u not in seen:
            uniq.append(u); seen.add(u)

    # Фильтруем по дате старта (достаём из страницы матча)
    out = []
    for url in uniq:
        d_start = get_match_start_date_msk(url)
        if not d_start:
            # если дату не распознали, оставим — возможно нужный матч; лишнее отсеем позже
            out.append(url)
        elif d_start in dates_keep:
            out.append(url)
    return out

def get_match_start_date_msk(match_url: str) -> dt.date | None:
    """
    Пытаемся достать дату начала матча по МСК со страницы матча.
    Ищем <time datetime="YYYY-MM-DDTHH:MM:SS+03:00">, либо текст вида 'Начало: 02:30 ... 27 октября ...'
    Возвращаем только дату (по МСК).
    """
    html = get_html(match_url)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")

    # Популярный вариант — <time datetime="...+03:00">
    for t in soup.find_all("time"):
        dt_attr = t.get("datetime") or t.get("content")
        if dt_attr:
            try:
                # Пытаемся распарсить ISO с таймзоной
                dt_full = dt.datetime.fromisoformat(dt_attr.replace("Z", "+00:00"))
                # Переведём в МСК
                d_msk = dt_full.astimezone(TZ_MSK).date()
                return d_msk
            except Exception:
                pass

    # Фолбэк: ищем текстовую дату в шапке
    full_text = soup.get_text("\n", strip=True)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})", full_text)
    if m:
        y, mo, d, hh, mm = map(int, m.groups())
        try:
            dtt = dt.datetime(y, mo, d, hh, mm, tzinfo=TZ_MSK)
            return dtt.date()
        except Exception:
            pass

    # Если совсем не нашли — None
    return None

# ───── Парсинг страницы матча
GOAL_LINE_RE = re.compile(
    r"(?P<score>\d+:\d+)\s*[—–-]\s*(?P<time>\d{1,2}[:.]\d{2})\s+(?P<who>[А-ЯЁA-Z][^()\n\r]*?)(?:\s*\((?P<ass>[^)]*)\))?(?=\s|$)",
    re.U
)
PERIOD_RE_LIST = [
    (re.compile(r"\b1[-–]?й\s+период\b", re.I | re.U), 1),
    (re.compile(r"\b2[-–]?й\s+период\b", re.I | re.U), 2),
    (re.compile(r"\b3[-–]?й\s+период\b", re.I | re.U), 3),
    (re.compile(r"\bОвертайм(?:\s*№\s*(\d+))?\b", re.I | re.U), 4),  # базово 4, если №N → 3+N
]

def parse_match_page(match_url: str) -> dict:
    """
    Возвращает:
      {
        "home": "Тампа-Бэй", "away": "Анахайм",
        "home_score": 4, "away_score": 3,
        "ot": False, "so": True/False,
        "goals": [ {"score":"1:0","abs_time":"9.10","scorer":"Д. Гентцель","ass":["Б. Хэйгел","А. Чирелли"]}, ... ],
        "so_winner": "Т. Зеграс" | None
      }
    """
    html = get_html(match_url)
    if not html:
        raise RuntimeError(f"Не удалось загрузить страницу матча: {match_url}")
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"[ \t]+", " ", text)
    text = text.replace("—", "–").replace("−", "–").replace("‒", "–")

    # Заголовок: команды и итоговый счёт
    # Пробуем вытащить из <h1>
    h1 = soup.find("h1")
    h1txt = h1.get_text(" ", strip=True) if h1 else ""
    # Часто встречающийся формат: «Тампа-Бэй» – «Анахайм» 4:3 (..)
    m_hdr = re.search(r"«?([А-ЯЁA-Za-z \-\.]+?)»?\s*[–-]\s*«?([А-ЯЁA-Za-z \-\.]+?)»?\s+(\d+):(\d+)(?:\s*\((.*?)\))?\s*(\(?(ОТ|Б)\)?)?", h1txt)
    if not m_hdr:
        # фолбэк — искать похожее в тексте
        m_hdr = re.search(r"([А-ЯЁA-Za-z \-\.]+?)\s*[–-]\s*([А-ЯЁA-Za-z \-\.]+?)\s+(\d+):(\d+)", text)
    if not m_hdr:
        raise RuntimeError(f"Не разобрал заголовок/счёт: {match_url}")

    home = m_hdr.group(1).strip().strip("«»")
    away = m_hdr.group(2).strip().strip("«»")
    home_score = int(m_hdr.group(3))
    away_score = int(m_hdr.group(4))
    winner_home = home_score > away_score

    # Определяем, был ли ОТ/Б
    so = bool(re.search(r"\bБуллиты\b|\(Б\)|в серии буллитов", text, re.I))
    ot = bool(re.search(r"\bОвертайм\b|\(ОТ\)", text, re.I))

    # Выделяем секцию с голами (между заголовками разделов)
    # Ищем начало по «1-й период»/«Голы» и конец по «Удаления|Штраф|Буллиты|Статистика» и т.п.
    start_idx = None
    for m in re.finditer(r"(1[-–]?й\s+период|Голы|Ход матча)", text, re.I):
        start_idx = m.start(); break
    if start_idx is None:
        # последняя попытка — с начала h1 вниз
        start_idx = text.find(h1txt) + len(h1txt) if h1txt else 0
    end_m = re.search(r"(Удаления|Штраф|Буллиты|Серия буллитов|Статистика|Составы|Видео)", text, re.I)
    end_idx = end_m.start() if end_m else len(text)

    goals_section = text[start_idx:end_idx]

    # Парсим по периодам
    period = 1
    goals = []
    # Разобьём на строки, чтобы увидеть заголовки периодов
    lines = [ln.strip() for ln in goals_section.split("\n") if ln.strip()]
    for ln in lines:
        # Детект заголовка периода
        hit_period = False
        for rx, base_p in PERIOD_RE_LIST:
            m = rx.search(ln)
            if m:
                if base_p == 4 and m.lastindex == 1 and m.group(1):
                    # «Овертайм №N»
                    try:
                        n = int(m.group(1))
                    except Exception:
                        n = 1
                    period = 3 + max(1, n)
                else:
                    period = base_p
                hit_period = True
                break
        if hit_period:
            continue

        # Ищем строки голов
        for m in GOAL_LINE_RE.finditer(ln):
            score = m.group("score").strip()
            t_in  = m.group("time").replace(".", ":")
            who   = m.group("who").strip()
            ass   = (m.group("ass") or "").strip()

            # чистим имя/ассистентов (non-breaking и латиницу)
            who = re.sub(r"\s+", " ", who)
            # Иногда после имени добавляют лишние фрагменты типа «— команда», отрежем по « – » или « — »
            who = re.split(r"\s+[–-]\s+", who)[0].strip()

            # Превращаем в «И. Фамилия»
            who_sh = initial_ru(who)

            assists = []
            if ass:
                for a in ass.split(","):
                    aa = a.strip()
                    # отрезаем возможные «— команда»
                    aa = re.split(r"\s+[–-]\s+", aa)[0].strip()
                    if aa:
                        assists.append(initial_ru(aa))

            abs_t = abs_time_from_period(period, t_in)
            goals.append({
                "score": score,
                "abs_time": abs_t,
                "scorer": who_sh,
                "ass": assists
            })

    # Победный буллит
    so_winner = None
    if so:
        # Явная формулировка
        m = re.search(r"Победный\s+буллит[:\s–-]+([А-ЯЁA-Z][^,\n\r]+)", text, re.I)
        if m:
            so_winner = initial_ru(m.group(1).strip())
        else:
            # Иногда в блоке буллитов отмечают «решающий» — попробуем
            m2 = re.search(r"(решающ|победн)[^:\n\r]*[:\s–-]+([А-ЯЁA-Z][^,\n\r]+)", text, re.I)
            if m2:
                so_winner = initial_ru(m2.group(2).strip())

    return {
        "home": home, "away": away,
        "home_score": home_score, "away_score": away_score,
        "winner_home": winner_home,
        "ot": ot, "so": so,
        "goals": goals,
        "so_winner": so_winner,
        "url": match_url
    }

# ───── Сбор матчей по правилу «день D + день D-1 после 15:00»
def collect_match_urls_for_report(d_report: dt.date) -> list[str]:
    """
    Грузим календарь (основной), достаём все ссылки матчей, фильтруем по датам начала:
      - дата == D
      - дата == D-1 и время >= 15:00 МСК
    Чтобы не зависеть от того, отдаёт ли календарь сразу обе даты, подстрахуемся:
      - сначала берём базовый календарь,
      - если D-1 другой месяц, пробуем ещё раз с параметром ?date=YYYY-MM-01
    """
    html_main = get_html(CAL_URL)
    if not html_main:
        raise RuntimeError("Не удалось открыть календарь Championat")

    # Список кандидатов (мы всё равно сверим дату на странице матча)
    cand = []
    # Из основного календаря
    cand += find_match_links_for_date_range(html_main, {d_report, d_report - dt.timedelta(days=1)})

    # Если D-1 в другом месяце — подстрахуемся
    d_prev = d_report - dt.timedelta(days=1)
    if d_prev.month != d_report.month:
        url_prev_month = CAL_URL + f"?date={d_prev.strftime('%Y-%m-01')}"
        html_prev = get_html(url_prev_month)
        if html_prev:
            cand += find_match_links_for_date_range(html_prev, {d_prev})

    # Уникализируем
    seen, uniq = set(), []
    for u in cand:
        if u not in seen:
            uniq.append(u); seen.add(u)

    # Отфильтруем по точному правилу времени
    selected = []
    for url in uniq:
        # попробуем достать точное время старта из <time datetime=...>
        html = get_html(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        start_dt = None
        for t in soup.find_all("time"):
            dt_attr = t.get("datetime") or t.get("content")
            if dt_attr:
                try:
                    dtt = dt.datetime.fromisoformat(dt_attr.replace("Z", "+00:00"))
                    start_dt = dtt.astimezone(TZ_MSK)
                    break
                except Exception:
                    pass
        # фолбэк — не нашли <time> → берём только по дате страницы (как раньше)
        if not start_dt:
            donly = get_match_start_date_msk(url)
            if not donly:
                continue
            # неизвестно время — пусть решит по дате
            if donly == d_report:
                selected.append(url)
            elif donly == d_prev:
                # без времени не можем гарантировать фильтр 15:00 — пропустим
                continue
            continue

        d_local = start_dt.date()
        if d_local == d_report:
            selected.append(url)
        elif d_local == d_prev and start_dt.time() >= dt.time(15, 0):
            selected.append(url)

    return selected

# ───── Формирование текста поста
def build_post_text(d_report: dt.date) -> str:
    urls = collect_match_urls_for_report(d_report)
    # Парсим все матчи
    matches = []
    for u in urls:
        try:
            jitter()
            matches.append(parse_match_page(u))
        except Exception as e:
            print(f"[WARN] {u}: {e}", file=sys.stderr)

    # Отбрасываем пустые
    matches = [m for m in matches if m.get("goals") or m.get("so") or (m.get("home") and m.get("away"))]
    n = len(matches)

    # Заголовок
    title = f"🗓 Регулярный чемпионат НХЛ • {ru_date(d_report)} • {n} " + \
            ("матч" if n == 1 else "матча" if n % 10 in (2, 3, 4) and not 12 <= n % 100 <= 14 else "матчей")
    head = f"{title}\n\nРезультаты надёжно спрятаны 👇\n\n——————————————————\n\n"

    if not matches:
        return head.strip()

    # Сортировка по времени начала? Сейчас порядок календаря/ссылок.
    blocks = []
    for idx, m in enumerate(matches, 1):
        home_line = bold_winner_line(m["home"], m["home_score"], m["winner_home"])
        away_line = bold_winner_line(m["away"], m["away_score"], not m["winner_home"])

        suffix = ""
        if m["so"]:
            suffix = " (Б)"
        elif m["ot"]:
            suffix = " (ОТ)"

        lines = [f"{home_line}{suffix}", f"{away_line}"]

        # события
        for ev in m["goals"]:
            ass = f" ({', '.join(escape(a) for a in ev['ass'])})" if ev["ass"] else ""
            lines.append(f"{escape(ev['score'])} – {escape(ev['abs_time'])} {escape(ev['scorer'])}{ass}")

        if m["so"] and m.get("so_winner"):
            lines.append("<i>Победный буллит</i>")
            lines.append(f"65.00 {escape(m['so_winner'])}")

        blocks.append("\n".join(lines))
        if idx < len(matches):
            blocks.append("")

    return head + "\n".join(blocks).strip()

# ───── Telegram
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
            if cut == -1:
                cut = maxlen
            chunk, rest = rest[:cut], rest[cut:].lstrip()
        r = S.post(url, json={
            "chat_id": CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=25)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:200]}")
        time.sleep(0.25)

# ───── main
if __name__ == "__main__":
    try:
        d = pick_report_date()
        post = build_post_text(d)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
