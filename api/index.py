from __future__ import annotations

import os
from typing import Any

import requests
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

try:
    from api.cron import handle_cron_request, run_bot_once
except ImportError:
    from cron import handle_cron_request, run_bot_once


DEFAULT_WEBHOOK_SECRET = "hook-123"
DEFAULT_MENU_CHAT = "@hoh_nhl_records"

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
    if chat_id and _is_menu_command(text):
        _send_menu(chat_id)

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


def _is_menu_command(text: str) -> bool:
    if not text:
        return False
    command = text.split()[0].lower()
    command = command.split("@", 1)[0]
    return command in ("/start", "/menu", "/help")


def _handle_callback(callback: dict) -> JSONResponse:
    callback_id = callback.get("id")
    data = callback.get("data")
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")

    if data == "resend_last_day":
        _answer_callback(callback_id, "Запускаю повторную отправку...")
        if chat_id:
            _send_text(chat_id, "Запускаю повторную отправку последнего игрового дня.")
        status, payload = run_bot_once(resend_last_day=True)
        if chat_id:
            if status == 200:
                _send_text(chat_id, "Готово: последний игровой день отправлен повторно.")
            else:
                _send_text(chat_id, f"Не получилось повторно отправить: {payload.get('error', 'unknown error')}")
        return JSONResponse(content=payload, status_code=status)

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
                        "text": "Повторно выслать последний игровой день",
                        "callback_data": "resend_last_day",
                    }
                ]
            ]
        },
    )


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
