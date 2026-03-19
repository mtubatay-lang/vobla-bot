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
    ENABLE_MAX,
    MAX_BOT_TOKEN,
    MAX_API_BASE_URL,
    MAX_WEBHOOK_PATH,
    MAX_AUTH_BEARER_PREFIX,
    MAX_WEBHOOK_SECRET,
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


def _normalize_slash_command(text: str) -> str:
    """Убирает @botname у команд вида /start@MyBot."""
    t = (text or "").strip()
    if not t.startswith("/"):
        return t
    cmd = t.split(None, 1)[0]
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd


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
            if isinstance(event, CallbackEvent):
                if event.data == START_AUTH:
                    await handle_start_auth_callback(adapter, event)
                else:
                    logger.debug("MAX callback not handled: %s", event.data)
                return JSONResponse(content={"ok": True})

            text = (event.text or "").strip()
            norm = _normalize_slash_command(text)
            if norm == "/start":
                await handle_start(adapter, event)
            elif norm == "/help":
                await handle_help(adapter, event)
            elif is_pending_auth("max", event.user.id):
                await handle_auth_code(adapter, event)
            else:
                logger.info(
                    "MAX message not handled (user_id=%s text_prefix=%r)",
                    event.user.id,
                    text[:80],
                )
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
    port_str = os.getenv("PORT") or os.getenv("MAX_WEBHOOK_PORT", "8080")
    port = int(port_str)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
