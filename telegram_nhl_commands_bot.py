import os
import time
import requests
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
NHL_API = "https://api-web.nhle.com"

PT_TZ = ZoneInfo("America/Los_Angeles")


def tg_request(method: str, payload: dict | None = None):
    url = f"{API_BASE}/{method}"
    r = requests.post(url, json=payload or {}, timeout=60)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram API error: {js}")
    return js


def send_message(chat_id: int | str, text: str):
    return tg_request(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
    )


def get_updates(offset: int | None = None, timeout: int = 30):
    payload = {"timeout": timeout}
    if offset is not None:
        payload["offset"] = offset
    return tg_request("getUpdates", payload)["result"]


def current_hockey_day_pt() -> date:
    now_pt = datetime.now(PT_TZ)
    return now_pt.date() if now_pt.hour >= 6 else (now_pt.date() - timedelta(days=1))


def fetch_schedule_for_date(d: date) -> list[dict]:
    url = f"{NHL_API}/v1/schedule/{d.isoformat()}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()

    games = js.get("games")
    if games is None:
        weeks = js.get("gameWeek") or []
        games = []
        for w in weeks:
            games.extend(w.get("games") or [])
    return games or []


def team_tri(team_obj: dict) -> str:
    abbr = team_obj.get("abbrev")
    if isinstance(abbr, str) and abbr.strip():
        return abbr.strip().upper()

    common = team_obj.get("commonName", {}).get("default")
    place = team_obj.get("placeName", {}).get("default")
    if common and place:
        return f"{place} {common}"
    return "TEAM"


def score_of(team_obj: dict) -> str:
    score = team_obj.get("score")
    if score is None:
        return ""
    return str(score)


def game_state(game: dict) -> str:
    return (game.get("gameState") or game.get("gameStatus") or "").upper().strip()


def is_final(game: dict) -> bool:
    return game_state(game) in {"FINAL", "OFF"}


def is_live(game: dict) -> bool:
    return game_state(game) in {"LIVE", "CRIT", "OVER", "FINAL_OT", "FINAL_SO"}


def is_pre(game: dict) -> bool:
    return game_state(game) in {"PRE", "FUT", "SCHED"}


def matchup_text(game: dict) -> str:
    away = team_tri(game.get("awayTeam", {}))
    home = team_tri(game.get("homeTeam", {}))
    return f"{away} - {home}"


def final_text(game: dict) -> str:
    away_team = game.get("awayTeam", {})
    home_team = game.get("homeTeam", {})
    away = team_tri(away_team)
    home = team_tri(home_team)
    away_score = score_of(away_team)
    home_score = score_of(home_team)

    suffix = ""
    gs = game_state(game)
    if gs == "OFF":
        suffix = ""
    elif gs in {"FINAL_OT"}:
        suffix = " OT"
    elif gs in {"FINAL_SO"}:
        suffix = " SO"

    return f"{away} {away_score}:{home_score} {home}{suffix}".strip()


def pending_text(game: dict) -> str:
    m = matchup_text(game)
    gs = game_state(game)

    if gs in {"PRE", "FUT", "SCHED"}:
        return f"{m} ещё не началась"
    if gs in {"LIVE", "CRIT", "OVER"}:
        return f"{m} ещё не завершилась"
    return f"{m} статус: {gs or 'UNKNOWN'}"


def find_game_by_teams(games: list[dict], t1: str, t2: str) -> dict | None:
    t1 = t1.strip().upper()
    t2 = t2.strip().upper()

    for g in games:
        away = team_tri(g.get("awayTeam", {})).upper()
        home = team_tri(g.get("homeTeam", {})).upper()
        pairs = {(away, home), (home, away)}
        if (t1, t2) in pairs:
            return g
    return None


def build_today_report() -> str:
    d = current_hockey_day_pt()
    games = fetch_schedule_for_date(d)

    if not games:
        return f"NHL\n\nЗа игровой день {d.isoformat()} матчей не найдено."

    lines = [f"NHL · игровой день {d.isoformat()}", ""]

    for g in games:
        if is_final(g):
            lines.append(final_text(g))
        else:
            lines.append(pending_text(g))

    return "\n".join(lines).strip()


def build_single_game_report(t1: str, t2: str) -> str:
    d = current_hockey_day_pt()
    games = fetch_schedule_for_date(d)
    game = find_game_by_teams(games, t1, t2)

    if not game:
        return f"Матч {t1.upper()} - {t2.upper()} за текущий игровой день не найден."

    if is_final(game):
        return final_text(game)
    return pending_text(game)


def help_text() -> str:
    return (
        "Команды:\n"
        "/start — показать все матчи текущего игрового дня\n"
        "/today — то же самое\n"
        "/game VGK NSH — статус или итог конкретного матча"
    )


def handle_message(chat_id: int | str, text: str):
    raw = (text or "").strip()
    if not raw:
        return

    low = raw.lower()

    if low == "/start":
        send_message(chat_id, build_today_report())
        return

    if low == "/today":
        send_message(chat_id, build_today_report())
        return

    if low.startswith("/game"):
        parts = raw.split()
        if len(parts) < 3:
            send_message(chat_id, "Используй так: /game VGK NSH")
            return
        t1, t2 = parts[1], parts[2]
        send_message(chat_id, build_single_game_report(t1, t2))
        return

    send_message(chat_id, help_text())


def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    print("Telegram NHL command bot started")

    offset = None
    while True:
        try:
            updates = get_updates(offset=offset, timeout=30)
            for upd in updates:
                offset = upd["update_id"] + 1

                msg = upd.get("message") or upd.get("edited_message") or {}
                chat = msg.get("chat") or {}
                chat_id = chat.get("id")
                text = msg.get("text", "")

                if chat_id is None:
                    continue

                try:
                    handle_message(chat_id, text)
                except Exception as e:
                    send_message(chat_id, f"Ошибка: {e}")

        except Exception as e:
            print("Polling error:", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
