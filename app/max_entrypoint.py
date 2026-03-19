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
from app.core.types import CallbackEvent, IncomingMessage, OutgoingMessage
from app.core.handlers import (
    handle_start,
    handle_start_auth_callback,
    handle_auth_code,
    handle_help,
    is_pending_auth,
    _pending_auth_uid,
)
from app.core.callbacks import QA_EXIT, QA_START, START_AUTH
from app.platforms.max import MaxApiClient, MaxAdapter, parse_max_update
from app.services.auth_service import find_user_by_platform_id
from app.services.max_qa_simple import max_simple_rag_answer
from app.ui.keyboards import qa_kb_rows

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Пользователи MAX в режиме «Задать вопрос» (после callback qa_start)
_max_qa_sessions: set[int] = set()


def _max_user_int(user_id: int | str) -> int:
    return _pending_auth_uid(user_id)


async def _safe_max_answer_callback(adapter: MaxAdapter, callback_id: str | None) -> None:
    if not callback_id:
        return
    try:
        await adapter.answer_callback(str(callback_id))
    except Exception as e:
        logger.warning("MAX answer_callback: %s", e)


async def _handle_max_qa_start(adapter: MaxAdapter, event: CallbackEvent) -> None:
    await _safe_max_answer_callback(adapter, event.callback_id)
    uid = _max_user_int(event.user.id)
    user = find_user_by_platform_id("max", uid)
    if not user:
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text="Сначала пройдите авторизацию: /start → кнопка «Авторизация».",
                is_group_chat=event.chat.is_group,
            )
        )
        return
    _max_qa_sessions.add(uid)
    await adapter.send_message(
        OutgoingMessage(
            chat_id=event.chat.id,
            platform="max",
            text=(
                "🧠 <b>Навык: Ответы на вопросы</b>\n\n"
                "Напишите вопрос одним сообщением.\n"
                "Можно задавать вопросы подряд.\n\n"
                "Чтобы выйти — «✅ Завершить навык»."
            ),
            keyboard_rows=qa_kb_rows(),
            is_group_chat=event.chat.is_group,
        )
    )


async def _handle_max_qa_exit(adapter: MaxAdapter, event: CallbackEvent) -> None:
    await _safe_max_answer_callback(adapter, event.callback_id)
    _max_qa_sessions.discard(_max_user_int(event.user.id))
    await adapter.send_message(
        OutgoingMessage(
            chat_id=event.chat.id,
            platform="max",
            text="Вы вышли из режима «Задать вопрос».",
            is_group_chat=event.chat.is_group,
        )
    )


async def _handle_max_qa_turn(adapter: MaxAdapter, event: IncomingMessage) -> None:
    uid = _max_user_int(event.user.id)
    text = (event.text or "").strip()
    if not text:
        return
    first = text.split()[0].lower() if text.split() else ""
    if first.startswith("/"):
        if first.startswith("/start"):
            _max_qa_sessions.discard(uid)
            await handle_start(adapter, event)
        return
    user = find_user_by_platform_id("max", uid)
    user_name = user.name if user else (event.user.name or "друг")
    await adapter.send_typing(event.chat.id, is_group_chat=event.chat.is_group)
    answer = await max_simple_rag_answer(text, user_name)
    await adapter.send_message(
        OutgoingMessage(
            chat_id=event.chat.id,
            platform="max",
            text=(
                f"{answer}\n\n"
                "Можете задать ещё вопрос или нажать «✅ Завершить навык»."
            ),
            keyboard_rows=qa_kb_rows(),
            is_group_chat=event.chat.is_group,
        )
    )


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
                elif event.data == QA_START:
                    await _handle_max_qa_start(adapter, event)
                elif event.data == QA_EXIT:
                    await _handle_max_qa_exit(adapter, event)
                else:
                    logger.debug("MAX callback not handled: %s", event.data)
                return JSONResponse(content={"ok": True})

            text = (event.text or "").strip()
            norm = _normalize_slash_command(text)
            uid = _max_user_int(event.user.id)
            if norm == "/start":
                _max_qa_sessions.discard(uid)
                await handle_start(adapter, event)
            elif norm == "/help":
                await handle_help(adapter, event)
            elif is_pending_auth("max", event.user.id):
                await handle_auth_code(adapter, event)
            elif uid in _max_qa_sessions:
                await _handle_max_qa_turn(adapter, event)
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
