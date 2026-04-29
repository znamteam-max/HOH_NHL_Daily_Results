from __future__ import annotations

import contextlib
import base64
import io
import json
import os
import sys
import time
import traceback
from http.server import BaseHTTPRequestHandler
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        cron_secret = os.getenv("CRON_SECRET", "").strip()
        auth_header = self.headers.get("authorization", "")

        if not cron_secret or auth_header != f"Bearer {cron_secret}":
            self._send_json(401, {"ok": False, "error": "unauthorized"})
            return

        stdout = io.StringIO()
        stderr = io.StringIO()

        try:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                import nhl_single_result_bot

                _patch_github_state(nhl_single_result_bot)
                nhl_single_result_bot.main()
            self._send_json(
                200,
                {
                    "ok": True,
                    "stdout": stdout.getvalue()[-12000:],
                    "stderr": stderr.getvalue()[-12000:],
                },
            )
        except Exception as exc:
            self._send_json(
                500,
                {
                    "ok": False,
                    "error": str(exc),
                    "traceback": traceback.format_exc()[-12000:],
                    "stdout": stdout.getvalue()[-12000:],
                    "stderr": stderr.getvalue()[-12000:],
                },
            )

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _patch_github_state(bot) -> None:
    token = _env("GITHUB_STATE_TOKEN") or _env("GH_STATE_TOKEN") or _env("GITHUB_TOKEN")
    if not token:
        return

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
        response.raise_for_status()
        payload = response.json()
        raw = base64.b64decode(payload.get("content", "")).decode("utf-8")
        return (json.loads(raw or "{}") or {"posted": {}}), payload.get("sha")

    def merge_state(base: dict, current: dict) -> dict:
        merged = dict(base or {})
        posted = dict((base or {}).get("posted", {}) or {})
        posted.update((current or {}).get("posted", {}) or {})
        merged["posted"] = posted
        return merged

    def load_state(path: str) -> dict:
        data, _ = fetch(path)
        return data

    def save_state(path: str, data: dict) -> None:
        for attempt in range(3):
            latest, sha = fetch(path)
            merged = merge_state(latest, data)
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
            response.raise_for_status()

    bot.load_state = load_state
    bot.save_state = save_state
