from __future__ import annotations

import os

import requests
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

try:
    from api.cron import handle_cron_request, run_bot_once
except ImportError:
    from cron import handle_cron_request, run_bot_once


app = FastAPI()


@app.get("/")
def health():
    return {"ok": True, "service": "hoh-nhl-daily-results"}


@app.get("/api/cron")
@app.get("/cron")
def cron(authorization: str = Header(default="")):
    status, payload = handle_cron_request(authorization)
    return JSONResponse(content=payload, status_code=status)


@app.post("/api/telegram")
@app.post("/telegram")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str = Header(default=""),
):
    expected_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    if expected_secret and x_telegram_bot_api_secret_token != expected_secret:
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


def _send_menu(chat_id) -> None:
    _send_text(
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


def _send_text(chat_id, text: str, reply_markup=None) -> None:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    _telegram_request("sendMessage", payload)


def _answer_callback(callback_id: str | None, text: str) -> None:
    if not callback_id:
        return
    _telegram_request("answerCallbackQuery", {"callback_query_id": callback_id, "text": text})


def _telegram_request(method: str, payload: dict) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return
    requests.post(
        f"https://api.telegram.org/bot{token}/{method}",
        json=payload,
        timeout=20,
    )
