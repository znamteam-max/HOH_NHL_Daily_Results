#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, re, unicodedata, sys, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

RU_MAP_PATH     = "ru_map.json"
RU_PENDING_PATH = "ru_pending.json"

SPORTS_RU_HOST    = "https://www.sports.ru"
SPORTS_RU_PERSON  = SPORTS_RU_HOST + "/hockey/person/"
SPORTS_RU_PLAYER  = SPORTS_RU_HOST + "/hockey/player/"
SPORTS_RU_SEARCH  = SPORTS_RU_HOST + "/search/?q="

EXCEPT_LAST = {  # должен совпадать со списком в боте
    "Nylander": "Нюландер", "Ekman-Larsson": "Экман-Ларссон", "Scheifele": "Шайфли", "Iafallo": "Иафалло",
    "Backlund": "Баклунд", "Kadri": "Кадри", "Toews": "Тэйвс", "Morrissey": "Моррисси", "Namestnikov": "Наместников",
    "Kulich": "Кулих", "Samuelsson": "Самуэльссон", "Dahlin": "Далин", "Roy": "Руа", "Cowan": "Коуэн",
    "Coleman": "Колман", "Bahl": "Баль", "Parekh": "Парех", "DeMelo": "Демело", "Vilardi": "Виларди",
    "Hamilton": "Хэмилтон", "Hischier": "Хишир", "Hughes": "Хьюз", "Brown": "Браун", "Carlson": "Карлсон",
    "Lapierre": "Лапьер", "McMichael": "Макмайкл", "Strome": "Строум", "Leonard": "Леонард", "Thompson": "Томпсон",
    "Matthews": "Мэттьюс", "Tavares": "Таварес", "Power": "Пауэр", "Joshua": "Джошуа", "Connor": "Коннор",
    "Byram": "Байрэм", "Benson": "Бенсон", "Krebs": "Кребс", "Carlo": "Карло", "Tuch": "Так", "McLeod": "Маклауд",
    "Eklund": "Эклунд", "Celebrini": "Селебрини", "Mercer": "Мерсер", "Voronkov": "Воронков", "Wilson": "Уилсон",
    "Ovechkin": "Овечкин", "Stanley": "Стэнли", "Frank": "Фрэнк", "Ekholm": "Экхольм", "Nurse": "Нерс",
    "Nugent-Hopkins": "Нюджент-Хопкинс", "Bouchard": "Бушар", "Honzek":"Гонзек", "Monahan":"Монахан",
    "Sourdif":"Сурдиф", "Mateychuk":"Матейчук", "Frost":"Фрост", "Protas":"Протас", "Cowen":"Коуэн",
}

FIRST_INITIAL_MAP = {
    "a":"А","b":"Б","c":"К","d":"Д","e":"Э","f":"Ф","g":"Г","h":"Х","i":"И","j":"Д",
    "k":"К","l":"Л","m":"М","n":"Н","o":"О","p":"П","q":"К","r":"Р","s":"С","t":"Т",
    "u":"У","v":"В","w":"В","x":"К","y":"Й","z":"З"
}

def make_session():
    s = requests.Session()
    retries = Retry(total=5, connect=5, read=5, backoff_factor=0.7,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "NHL-RU-CACHE-UPDATER/1.0",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

def load(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def slugify(first: str, last: str) -> str:
    base = f"{first} {last}".strip()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower().strip()
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    return base

def try_profile_by_slug(first: str, last: str) -> str | None:
    slug = slugify(first, last)
    for root in (SPORTS_RU_PERSON, SPORTS_RU_PLAYER):
        url = root + slug + "/"
        r = S.get(url, timeout=15)
        if r.status_code == 200 and ("/hockey/person/" in r.url or "/hockey/player/" in r.url):
            return url
    return None

def extract_initial_surname_from_profile(url: str) -> str | None:
    try:
        r = S.get(url, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1","h2"])
        if not h: return None
        full = " ".join(h.get_text(" ", strip=True).split())
        parts = [p for p in re.split(r"\s+", full) if p]
        if len(parts) >= 2:
            ini = parts[0][0] + "."
            last = parts[-1]
            return f"{ini} {last}"
    except Exception:
        return None
    return None

def search_initial_surname(first: str, last: str) -> str | None:
    try:
        q = quote_plus(f"{first} {last}".strip())
        r = S.get(SPORTS_RU_SEARCH + q, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        link = soup.select_one('a[href*="/hockey/person/"]') or soup.select_one('a[href*="/hockey/player/"]')
        if not link or not link.get("href"): return None
        href = link["href"]
        if href.startswith("/"): href = SPORTS_RU_HOST + href
        return extract_initial_surname_from_profile(href)
    except Exception:
        return None

def fallback_ru_name(first: str, last: str) -> str:
    ru_last = EXCEPT_LAST.get(last, last or "")
    ini_src = (first or last or "A")[:1].lower()
    ru_ini = FIRST_INITIAL_MAP.get(ini_src, ini_src.upper())
    if len(ru_ini) > 1: ru_ini = ru_ini[0]
    return f"{ru_ini}. {ru_last}".strip()

def main():
    ru_map = load(RU_MAP_PATH, {})
    pending = load(RU_PENDING_PATH, [])
    if not pending:
        print("No pending players.")
        return

    updated = False
    still = []

    for it in pending:
        pid = it.get("id")
        first = (it.get("first") or "").strip()
        last  = (it.get("last")  or "").strip()
        if not pid:
            continue
        if str(pid) in ru_map:
            continue

        ru = None
        if first and last:
            url = try_profile_by_slug(first, last)
            if url:
                ru = extract_initial_surname_from_profile(url)
        if not ru and first and last:
            ru = search_initial_surname(first, last)
        if not ru:
            ru = fallback_ru_name(first, last)

        if ru and any(ch.isalpha() for ch in ru):
            ru_map[str(pid)] = ru
            updated = True
        else:
            still.append(it)

        time.sleep(0.2)

    save(RU_MAP_PATH, ru_map)
    save(RU_PENDING_PATH, still)
    print(f"Resolved: {len(pending)-len(still)}, left: {len(still)}")
    if updated:
        print("ru_map.json updated")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
