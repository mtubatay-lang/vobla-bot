"""
Точка входа для приёма обновлений MAX (webhook).
Запуск: ENABLE_MAX=true MAX_BOT_TOKEN=... python -m app.max_entrypoint
"""

from __future__ import annotations

import asyncio
import logging
import os

# Загружаем конфиг (требует BOT_TOKEN и прочие переменные)
from app.config import (
    ENABLE_MAX,
    MAX_BOT_TOKEN,
    MAX_API_BASE_URL,
    MAX_WEBHOOK_PATH,
)
from app.core.types import CallbackEvent
from app.core.handlers import (
    handle_start,
    handle_start_auth_callback,
    handle_auth_code,
    handle_help,
    is_pending_auth,
)
from app.core.callbacks import START_AUTH
from app.platforms.max import MaxApiClient, MaxAdapter, parse_max_update

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _create_app():
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse

    app = FastAPI(title="Vobla MAX Webhook")

    if not ENABLE_MAX or not MAX_BOT_TOKEN:
        logger.warning("MAX webhook disabled: ENABLE_MAX=%s, MAX_BOT_TOKEN set=%s", ENABLE_MAX, bool(MAX_BOT_TOKEN))
        return app

    client = MaxApiClient(token=MAX_BOT_TOKEN, base_url=MAX_API_BASE_URL)
    adapter = MaxAdapter(client)

    @app.post(MAX_WEBHOOK_PATH)
    async def webhook(request: Request):
        try:
            body = await request.json()
        except Exception as e:
            logger.warning("MAX webhook invalid JSON: %s", e)
            return JSONResponse(content={"ok": False}, status_code=400)

        event = parse_max_update(body)
        if not event:
            return JSONResponse(content={"ok": True})

        try:
            if isinstance(event, CallbackEvent):
                if event.data == START_AUTH:
                    await handle_start_auth_callback(adapter, event)
                else:
                    logger.debug("MAX callback not handled: %s", event.data)
                return JSONResponse(content={"ok": True})
            # IncomingMessage
            text = (event.text or "").strip()
            if text == "/start":
                await handle_start(adapter, event)
            elif text == "/help":
                await handle_help(adapter, event)
            elif is_pending_auth("max", event.user.id):
                await handle_auth_code(adapter, event)
            else:
                logger.debug("MAX message not handled: %s", text[:50])
            return JSONResponse(content={"ok": True})
        except Exception as e:
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
    port = int(os.getenv("MAX_WEBHOOK_PORT", "8080"))
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
