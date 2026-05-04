from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any

import requests
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

try:
    from api.cron import handle_cron_request, run_bot_once
except ImportError:
    from cron import handle_cron_request, run_bot_once


DEFAULT_WEBHOOK_SECRET = "hook-123"
DEFAULT_TARGET_CHAT = "-1003167239288"
DEFAULT_MENU_CHAT = DEFAULT_TARGET_CHAT

app = FastAPI()


@app.get("/")
def health():
    return {"ok": True, "service": "hoh-nhl-daily-results"}


@app.get("/api/cron")
@app.get("/cron")
def cron(authorization: str = Header(default="")):
    status, payload = handle_cron_request(authorization)
    return JSONResponse(content=payload, status_code=status)


@app.api_route("/api/setup-webhook", methods=["GET", "POST"])
@app.api_route("/setup-webhook", methods=["GET", "POST"])
async def setup_webhook(request: Request, authorization: str = Header(default="")):
    if not _is_management_authorized(request, authorization):
        return JSONResponse(content={"ok": False, "error": "unauthorized"}, status_code=401)

    webhook_url = f"{_public_base_url(request)}/api/telegram"
    result = _telegram_request(
        "setWebhook",
        {
            "url": webhook_url,
            "secret_token": _webhook_secret(),
            "allowed_updates": ["message", "callback_query"],
        },
    )

    send_menu = _query_bool(request, "send_menu", True)
    menu_result: dict[str, Any] | None = None
    if send_menu and result.get("ok"):
        menu_result = _send_menu(_menu_chat_id())

    status_code = 200 if result.get("ok") else 500
    return JSONResponse(
        content={
            "ok": bool(result.get("ok")),
            "webhook_url": webhook_url,
            "webhook_secret": "configured",
            "telegram": result,
            "menu": menu_result,
        },
        status_code=status_code,
    )


@app.api_route("/api/menu", methods=["GET", "POST"])
@app.api_route("/menu", methods=["GET", "POST"])
async def send_menu(request: Request, authorization: str = Header(default="")):
    if not _is_management_authorized(request, authorization):
        return JSONResponse(content={"ok": False, "error": "unauthorized"}, status_code=401)

    chat_id = (request.query_params.get("chat") or _menu_chat_id()).strip()
    if not chat_id:
        return JSONResponse(content={"ok": False, "error": "missing chat"}, status_code=500)

    result = _send_menu(chat_id)
    status_code = 200 if result.get("ok") else 500
    return JSONResponse(content={"ok": bool(result.get("ok")), "chat_id": chat_id, "telegram": result}, status_code=status_code)


@app.post("/api/telegram")
@app.post("/telegram")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str = Header(default=""),
):
    if x_telegram_bot_api_secret_token != _webhook_secret():
        return JSONResponse(content={"ok": False, "error": "unauthorized"}, status_code=401)

    update = await request.json()
    callback = update.get("callback_query") or {}
    if callback:
        return _handle_callback(callback)

    message = update.get("message") or update.get("channel_post") or {}
    text = (message.get("text") or "").strip()
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id:
        command = _command_name(text)
        if command in ("/start", "/menu", "/help"):
            _send_menu(chat_id)
        elif command == "/latest":
            _send_text(chat_id, _latest_matches_text())
        elif command == "/schedule":
            _send_text(chat_id, _schedule_overview_text())
        elif command in ("/reload", "/resend"):
            _send_text(chat_id, "Запускаю повторную отправку последнего игрового дня.")
            status, payload = run_bot_once(resend_last_day=True, target_chat_id=_menu_chat_id())
            if status == 200:
                _send_text(chat_id, "Готово: последний игровой день отправлен повторно.")
            else:
                _send_text(chat_id, f"Не получилось повторно отправить: {payload.get('error', 'unknown error')}")

    return {"ok": True}


def _webhook_secret() -> str:
    return os.getenv("TELEGRAM_WEBHOOK_SECRET", DEFAULT_WEBHOOK_SECRET).strip() or DEFAULT_WEBHOOK_SECRET


def _management_secret() -> str:
    return os.getenv("WEBHOOK_SETUP_SECRET", _webhook_secret()).strip() or _webhook_secret()


def _menu_chat_id() -> str:
    return (
        os.getenv("TELEGRAM_MENU_CHAT_ID", "").strip()
        or os.getenv("TELEGRAM_CHAT_ID", "").strip()
        or DEFAULT_MENU_CHAT
    )


def _is_management_authorized(request: Request, authorization: str) -> bool:
    expected = _management_secret()
    provided = (request.query_params.get("secret") or "").strip()
    return provided == expected or authorization == f"Bearer {expected}"


def _query_bool(request: Request, name: str, default: bool) -> bool:
    raw = request.query_params.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _public_base_url(request: Request) -> str:
    env_url = (
        os.getenv("PUBLIC_BASE_URL", "").strip()
        or os.getenv("VERCEL_PROJECT_PRODUCTION_URL", "").strip()
        or os.getenv("VERCEL_URL", "").strip()
    )
    if env_url:
        if not env_url.startswith(("http://", "https://")):
            env_url = f"https://{env_url}"
        return env_url.rstrip("/")

    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}".rstrip("/")


def _command_name(text: str) -> str:
    if not text:
        return ""
    command = text.split()[0].lower()
    return command.split("@", 1)[0]


def _is_menu_command(text: str) -> bool:
    return _command_name(text) in ("/start", "/menu", "/help")


def _handle_callback(callback: dict) -> JSONResponse:
    callback_id = callback.get("id")
    data = callback.get("data")
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")

    if data == "latest_matches":
        _answer_callback(callback_id, "Показываю последние матчи...")
        if chat_id:
            _send_text(chat_id, _latest_matches_text())
        return JSONResponse(content={"ok": True, "action": data})

    if data == "resend_last_day":
        _answer_callback(callback_id, "Запускаю повторную отправку...")
        if chat_id:
            _send_text(chat_id, "Запускаю повторную отправку последнего игрового дня.")
        status, payload = run_bot_once(resend_last_day=True, target_chat_id=_menu_chat_id())
        if chat_id:
            if status == 200:
                _send_text(chat_id, "Готово: последний игровой день отправлен повторно.")
            else:
                _send_text(chat_id, f"Не получилось повторно отправить: {payload.get('error', 'unknown error')}")
        return JSONResponse(content=payload, status_code=status)

    if data == "schedule_overview":
        _answer_callback(callback_id, "Показываю расписание...")
        if chat_id:
            _send_text(chat_id, _schedule_overview_text())
        return JSONResponse(content={"ok": True, "action": data})

    _answer_callback(callback_id, "Неизвестная команда")
    return JSONResponse(content={"ok": False, "error": "unknown callback"}, status_code=400)


def _send_menu(chat_id) -> dict[str, Any]:
    return _send_text(
        chat_id,
        "Меню HOH NHL Results",
        reply_markup={
            "inline_keyboard": [
                [
                    {
                        "text": "Показать последние матчи",
                        "callback_data": "latest_matches",
                    }
                ],
                [
                    {
                        "text": "Загрузить заново последний игровой день",
                        "callback_data": "resend_last_day",
                    }
                ],
                [
                    {
                        "text": "Расписание по дням",
                        "callback_data": "schedule_overview",
                    }
                ]
            ]
        },
    )


def _bot_module():
    import nhl_single_result_bot

    return nhl_single_result_bot


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, "").strip())
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def _hockey_base_day() -> date:
    bot = _bot_module()
    return datetime.fromisoformat(bot._current_hockey_day_pt()).date()


def _date_range(base_day: date, days_back: int, days_forward: int) -> list[date]:
    return [base_day + timedelta(days=offset) for offset in range(-days_back, days_forward + 1)]


def _fetch_games_for_day(day: date) -> list[dict]:
    bot = _bot_module()
    response = requests.get(bot.SCHED_FMT.format(ymd=day.isoformat()), timeout=30)
    response.raise_for_status()
    payload = response.json()
    games = payload.get("games")
    if games is None:
        games = []
        for week_day in payload.get("gameWeek") or []:
            games.extend(week_day.get("games") or [])

    exact = [game for game in games if str(game.get("gameDate") or "") == day.isoformat()]
    if any("gameDate" in game for game in games):
        games = exact

    seen = set()
    unique = []
    for game in games or []:
        gid = game.get("id") or game.get("gameId") or game.get("gamePk")
        if gid in seen:
            continue
        seen.add(gid)
        unique.append(game)
    return unique


def _metas_for_day(day: date) -> list[Any]:
    bot = _bot_module()
    metas = [bot._game_to_meta(game) for game in _fetch_games_for_day(day)]
    return [meta for meta in metas if meta]


def _team_label(tricode: str) -> str:
    bot = _bot_module()
    emoji = bot.TEAM_EMOJI.get(tricode, "")
    name = bot.TEAM_RU.get(tricode, tricode)
    return f"{emoji} {name}".strip()


def _plural_ru(n: int, one: str, few: str, many: str) -> str:
    n_abs = abs(n)
    if 11 <= n_abs % 100 <= 14:
        return many
    if n_abs % 10 == 1:
        return one
    if 2 <= n_abs % 10 <= 4:
        return few
    return many


def _status_counts(metas: list[Any]) -> tuple[int, int, int]:
    bot = _bot_module()
    final_count = sum(1 for meta in metas if bot._is_final_state(meta.state))
    live_count = sum(1 for meta in metas if bot._is_liveish_state(meta.state))
    upcoming_count = max(0, len(metas) - final_count - live_count)
    return final_count, live_count, upcoming_count


def _series_text(meta: Any) -> str:
    pieces = []
    if meta.series_game:
        pieces.append(f"Матч №{meta.series_game}")
    if meta.home_series_wins is not None and meta.away_series_wins is not None:
        pieces.append(f"серия {meta.home_series_wins}-{meta.away_series_wins}")
    return ", ".join(pieces)


def _match_line(meta: Any) -> str:
    bot = _bot_module()
    day_text = meta.gameDateUTC.astimezone(bot.PT_TZ).strftime("%d.%m")
    home = _team_label(meta.home_tri)
    away = _team_label(meta.away_tri)
    details = _series_text(meta)
    suffix = f" · {details}" if details else ""
    return f"{day_text} · {home} {meta.home_score}:{meta.away_score} {away}{suffix}"


def _latest_matches_text() -> str:
    bot = _bot_module()
    base_day = _hockey_base_day()
    days_back = _env_int("MENU_LATEST_DAYS_BACK", 6, 1, 14)
    limit = _env_int("MENU_LATEST_LIMIT", 12, 3, 25)

    metas: list[Any] = []
    for day in _date_range(base_day, days_back, 1):
        metas.extend(_metas_for_day(day))

    seen = set()
    finals = []
    for meta in sorted(metas, key=lambda x: x.gameDateUTC, reverse=True):
        if meta.gamePk in seen or not bot._is_final_state(meta.state):
            continue
        seen.add(meta.gamePk)
        finals.append(meta)

    if not finals:
        return "Последние завершённые матчи не найдены."

    lines = ["Последние завершённые матчи", ""]
    lines.extend(_match_line(meta) for meta in finals[:limit])
    return "\n".join(lines)


def _schedule_overview_text() -> str:
    base_day = _hockey_base_day()
    days_back = _env_int("MENU_SCHEDULE_DAYS_BACK", 2, 0, 7)
    days_forward = _env_int("MENU_SCHEDULE_DAYS_FORWARD", 10, 1, 21)
    weekdays = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

    lines = ["Расписание NHL по игровым дням", ""]
    for day in _date_range(base_day, days_back, days_forward):
        metas = _metas_for_day(day)
        final_count, live_count, upcoming_count = _status_counts(metas)
        total = len(metas)
        match_word = _plural_ru(total, "матч", "матча", "матчей")
        status = f"{final_count} завершено, {live_count} в игре, {upcoming_count} впереди"
        lines.append(f"{day.strftime('%d.%m')} {weekdays[day.weekday()]} — {total} {match_word}: {status}")
    return "\n".join(lines)


def _send_text(chat_id, text: str, reply_markup=None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    thread = os.getenv("TELEGRAM_THREAD_ID", "").strip()
    if thread:
        try:
            payload["message_thread_id"] = int(thread)
        except ValueError:
            pass
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return _telegram_request("sendMessage", payload)


def _answer_callback(callback_id: str | None, text: str) -> dict[str, Any] | None:
    if not callback_id:
        return None
    return _telegram_request("answerCallbackQuery", {"callback_query_id": callback_id, "text": text})


def _telegram_request(method: str, payload: dict) -> dict[str, Any]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return {"ok": False, "error": "missing TELEGRAM_BOT_TOKEN"}

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/{method}",
            json=payload,
            timeout=30,
        )
        try:
            data = response.json()
        except ValueError:
            data = {"ok": False, "raw": response.text}
        return {
            "ok": response.status_code == 200 and bool(data.get("ok")),
            "status_code": response.status_code,
            "response": data,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
