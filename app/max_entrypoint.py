"""
Точка входа для приёма обновлений MAX (webhook).
Запуск: ENABLE_MAX=true MAX_BOT_TOKEN=... python -m app.max_entrypoint

На Railway задайте PORT (и при необходимости отдельный сервис только для webhook).

Без ``from __future__ import annotations``: у вложенной ``async def webhook(request: Request)``
иначе аннотация превращается в строку ``"Request"``, FastAPI принимает её за JSON-body
и отвечает 422 на POST /webhook/max.
"""

import logging
import os

from app.config import (
    APP_REVISION,
    ENABLE_MAX,
    LOG_LEVEL,
    MAX_BOT_TOKEN,
    MAX_API_BASE_URL,
    MAX_WEBHOOK_PATH,
    MAX_AUTH_BEARER_PREFIX,
    MAX_GROUP_CHAT_QA_ENABLED,
    MAX_WEBHOOK_SECRET,
)
from app.platforms.max import MaxApiClient, MaxAdapter, parse_max_update
from app.platforms.max.router import MaxActionRouter

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
_root_lvl = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
if isinstance(_root_lvl, int) and _root_lvl >= logging.INFO:
    for _name in ("httpx", "httpcore"):
        logging.getLogger(_name).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
if APP_REVISION:
    logger.info("[MAX_ENTRYPOINT] APP_REVISION=%s (сверьте с main при отладке деплоя)", APP_REVISION)
if not MAX_GROUP_CHAT_QA_ENABLED:
    logger.warning(
        "[MAX_ENTRYPOINT] MAX_GROUP_CHAT_QA_ENABLED=false — ответы RAG в групповых чатах MAX отключены"
    )


def _is_google_sheets_quota_error(exc: BaseException) -> bool:
    s = str(exc)
    if "429" not in s:
        return False
    mod = (type(exc).__module__ or "").lower()
    if "gspread" in mod:
        return True
    s_low = s.lower()
    return any(
        x in s_low
        for x in ("quota", "sheets.googleapis.com", "read requests", "rate limit exceeded")
    )


def _create_app():
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse

    app = FastAPI(title="Vobla MAX Webhook")

    if not ENABLE_MAX or not MAX_BOT_TOKEN:
        logger.warning(
            "MAX webhook disabled: ENABLE_MAX=%s, MAX_BOT_TOKEN set=%s",
            ENABLE_MAX,
            bool(MAX_BOT_TOKEN),
        )
        return app

    client = MaxApiClient(
        token=MAX_BOT_TOKEN,
        base_url=MAX_API_BASE_URL,
        use_bearer_prefix=MAX_AUTH_BEARER_PREFIX,
    )
    adapter = MaxAdapter(client)
    action_router = MaxActionRouter(max_client=client)

    @app.post(MAX_WEBHOOK_PATH)
    async def webhook(request: Request):
        if MAX_WEBHOOK_SECRET:
            got = (request.headers.get("X-Max-Bot-Api-Secret") or "").strip()
            if got != MAX_WEBHOOK_SECRET:
                logger.warning("MAX webhook: rejected (bad or missing X-Max-Bot-Api-Secret)")
                return JSONResponse(content={"ok": False}, status_code=403)

        try:
            body = await request.json()
        except Exception as e:
            logger.warning("MAX webhook invalid JSON: %s", e)
            return JSONResponse(content={"ok": False}, status_code=400)

        if not isinstance(body, dict):
            logger.info("MAX webhook: body is not an object")
            return JSONResponse(content={"ok": True})

        event = parse_max_update(body)
        if not event:
            ut = body.get("update_type") or body.get("type")
            logger.info(
                "MAX webhook: parse_max_update returned None (update_type=%r keys=%s)",
                ut,
                list(body.keys()),
            )
            return JSONResponse(content={"ok": True})

        try:
            await action_router.route(adapter, event)
            return JSONResponse(content={"ok": True})
        except Exception as e:
            if _is_google_sheets_quota_error(e):
                logger.warning(
                    "MAX webhook: Google Sheets quota/rate limit, ack 200 to avoid retry storm: %s",
                    e,
                )
                return JSONResponse(content={"ok": True, "degraded": True})
            logger.exception("MAX webhook handler error: %s", e)
            return JSONResponse(content={"ok": False}, status_code=500)

    @app.get("/health")
    async def health():
        return {"status": "ok", "max": ENABLE_MAX}

    return app


def main():
    if not ENABLE_MAX or not MAX_BOT_TOKEN:
        raise SystemExit("Set ENABLE_MAX=true and MAX_BOT_TOKEN to run MAX webhook.")

    app = _create_app()
    import uvicorn

    host = os.getenv("MAX_WEBHOOK_HOST", "0.0.0.0")
    port_str = os.getenv("PORT") or os.getenv("MAX_WEBHOOK_PORT", "8080")
    port = int(port_str)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
