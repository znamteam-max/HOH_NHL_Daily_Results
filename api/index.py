from __future__ import annotations

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse

try:
    from api.cron import handle_cron_request
except ImportError:
    from cron import handle_cron_request


app = FastAPI()


@app.get("/")
def health():
    return {"ok": True, "service": "hoh-nhl-daily-results"}


@app.get("/api/cron")
@app.get("/cron")
def cron(authorization: str = Header(default="")):
    status, payload = handle_cron_request(authorization)
    return JSONResponse(content=payload, status_code=status)
