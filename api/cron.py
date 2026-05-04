from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import sys
import time
import traceback
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def handle_cron_request(auth_header: str) -> tuple[int, dict]:
    cron_secret = os.getenv("CRON_SECRET", "").strip()
    if not cron_secret or auth_header != f"Bearer {cron_secret}":
        return 401, {"ok": False, "error": "unauthorized"}

    return run_bot_once()


def run_bot_once(resend_last_day: bool = False, target_chat_id: str | None = None) -> tuple[int, dict]:
    state_token = _state_token()
    if not state_token:
        return 500, {
            "ok": False,
            "error": "missing GITHUB_STATE_TOKEN",
            "hint": "Set GITHUB_STATE_TOKEN in Vercel with Contents read/write access, then redeploy.",
        }

    stdout = io.StringIO()
    stderr = io.StringIO()

    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            import nhl_single_result_bot

            _patch_github_state(nhl_single_result_bot, state_token)
            previous_resend = os.environ.get("RESEND_LAST_DAY")
            previous_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
            os.environ["RESEND_LAST_DAY"] = "1" if resend_last_day else "0"
            if target_chat_id:
                os.environ["TELEGRAM_CHAT_ID"] = str(target_chat_id)
            try:
                nhl_single_result_bot.main()
            finally:
                if previous_resend is None:
                    os.environ.pop("RESEND_LAST_DAY", None)
                else:
                    os.environ["RESEND_LAST_DAY"] = previous_resend
                if target_chat_id:
                    if previous_chat_id is None:
                        os.environ.pop("TELEGRAM_CHAT_ID", None)
                    else:
                        os.environ["TELEGRAM_CHAT_ID"] = previous_chat_id
        return 200, {
            "ok": True,
            "resend_last_day": resend_last_day,
            "target_chat_id": target_chat_id,
            "stdout": stdout.getvalue()[-12000:],
            "stderr": stderr.getvalue()[-12000:],
        }
    except Exception as exc:
        return 500, {
            "ok": False,
            "error": str(exc),
            "traceback": traceback.format_exc()[-12000:],
            "stdout": stdout.getvalue()[-12000:],
            "stderr": stderr.getvalue()[-12000:],
        }


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _state_token() -> str:
    return _env("GITHUB_STATE_TOKEN") or _env("GH_STATE_TOKEN") or _env("GITHUB_TOKEN")


def _patch_github_state(bot, token: str) -> None:
    repo = _env("GITHUB_STATE_REPO", "znamteam-max/HOH_NHL_Daily_Results")
    branch = _env("GITHUB_STATE_BRANCH", "main") or "main"
    requests = bot.requests

    def headers() -> dict:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def url_for(path: str) -> str:
        clean_path = path.replace("\\", "/").lstrip("/")
        return f"https://api.github.com/repos/{repo}/contents/{clean_path}"

    def fetch(path: str) -> tuple[dict, str | None]:
        response = requests.get(
            url_for(path),
            headers=headers(),
            params={"ref": branch},
            timeout=30,
        )
        if response.status_code == 404:
            return {"posted": {}}, None
        if response.status_code >= 400:
            raise RuntimeError(
                f"GitHub state fetch failed: HTTP {response.status_code} {response.text[:500]}"
            )
        response.raise_for_status()
        payload = response.json()
        raw = base64.b64decode(payload.get("content", "")).decode("utf-8")
        return (json.loads(raw or "{}") or {"posted": {}}), payload.get("sha")

    def merge_state(base: dict, current: dict) -> dict:
        merged = dict(base or {})
        posted = dict((base or {}).get("posted", {}) or {})
        posted.update((current or {}).get("posted", {}) or {})
        merged["posted"] = posted
        if "force_repost" in (current or {}):
            force_repost = dict((current or {}).get("force_repost", {}) or {})
            if force_repost:
                merged["force_repost"] = force_repost
            else:
                merged.pop("force_repost", None)
        return merged

    def load_state(path: str) -> dict:
        data, _ = fetch(path)
        return data

    def save_state(path: str, data: dict) -> None:
        for attempt in range(3):
            latest, sha = fetch(path)
            merged = merge_state(latest, data)
            if sha and merged == latest:
                return
            body = {
                "message": "Update posted games state",
                "content": base64.b64encode(
                    json.dumps(merged, ensure_ascii=False, indent=2).encode("utf-8")
                ).decode("ascii"),
                "branch": branch,
            }
            if sha:
                body["sha"] = sha

            response = requests.put(url_for(path), headers=headers(), json=body, timeout=30)
            if response.status_code in (200, 201):
                return
            if response.status_code == 409 and attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            if response.status_code >= 400:
                raise RuntimeError(
                    f"GitHub state save failed: HTTP {response.status_code} {response.text[:500]}"
                )
            response.raise_for_status()

    bot.load_state = load_state
    bot.save_state = save_state
