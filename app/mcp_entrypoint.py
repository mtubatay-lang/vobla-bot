"""
MCP-сервер для внешних рассылок (Telegram + MAX).

Позволяет внешнему агенту с другой платформы:
- узнать доступные платформы, сегменты, регионы и чаты (tool ``list_targets``);
- запустить рассылку текстом и медиа в выбранные платформы/сегменты (tool ``send_broadcast``);
- проверить статус запущенной рассылки (tool ``get_broadcast_status``).

Транспорт: streamable HTTP (по умолчанию путь /mcp). Авторизация: статический ключ
``MCP_API_KEY`` в заголовке ``Authorization: Bearer <ключ>`` (или ``X-Api-Key``).

Запуск: MCP_ENABLED=true MCP_API_KEY=... python -m app.mcp_entrypoint
На Railway задайте PORT и публичный домен (сервис vobla-mcp).
"""

import asyncio
import logging
import os
import uuid
from typing import Any, Dict, List, Optional

from app.config import (
    APP_REVISION,
    BOT_TOKEN,
    ENABLE_MAX,
    LOG_LEVEL,
    MAX_API_BASE_URL,
    MAX_AUTH_BEARER_PREFIX,
    MAX_BOT_TOKEN,
    MCP_API_KEY,
    MCP_PATH,
)
from app.core.types import Platform, Recipient
from app.services.broadcast_media import build_media_json_from_urls
from app.services.broadcast_service import (
    execute_broadcast_multi,
    get_broadcast_draft_by_id,
    get_broadcast_recipients_list,
    read_active_recipients_chats_with_names,
    read_active_regions,
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
if APP_REVISION:
    logger.info("[MCP_ENTRYPOINT] APP_REVISION=%s", APP_REVISION)

_VALID_PLATFORMS = ("telegram", "max")

# Реестр фоновых задач рассылки этого процесса: job_id -> состояние.
_jobs: Dict[str, Dict[str, Any]] = {}
# Удерживаем ссылки на задачи, чтобы их не собрал GC.
_tasks: set[asyncio.Task] = set()

# Ленивые синглтоны адаптеров (создаются в event loop uvicorn при первом вызове).
_tg_adapter = None
_max_adapter = None


def _telegram_adapter():
    global _tg_adapter
    if _tg_adapter is None:
        from aiogram import Bot

        from app.platforms.telegram import TelegramAdapter

        _tg_adapter = TelegramAdapter(Bot(token=BOT_TOKEN))
    return _tg_adapter


def _max_adapter_instance():
    global _max_adapter
    if _max_adapter is None:
        from app.platforms.max import MaxAdapter, MaxApiClient

        _max_adapter = MaxAdapter(
            MaxApiClient(
                token=MAX_BOT_TOKEN,
                base_url=MAX_API_BASE_URL,
                use_bearer_prefix=MAX_AUTH_BEARER_PREFIX,
            )
        )
    return _max_adapter


def _normalize_platforms(platforms: Optional[List[str]]) -> List[Platform]:
    if not platforms:
        return ["telegram"]
    out: List[Platform] = []
    for p in platforms:
        x = str(p).strip().lower()
        if x in _VALID_PLATFORMS and x not in out:
            out.append(x)  # type: ignore[arg-type]
    return out or ["telegram"]


def _build_adapters(platforms: List[Platform]) -> Dict[Platform, Any]:
    adapters: Dict[Platform, Any] = {}
    if "telegram" in platforms:
        adapters["telegram"] = _telegram_adapter()
    if "max" in platforms:
        adapters["max"] = _max_adapter_instance()
    return adapters


async def _run_job(
    job_id: str,
    adapters: Dict[Platform, Any],
    recipients: List[Recipient],
    text: str,
    media_json: str,
    mode: str,
) -> None:
    _jobs[job_id]["status"] = "running"
    try:
        broadcast_id, sent_ok, sent_fail = await execute_broadcast_multi(
            adapters,
            recipients,
            text,
            media_json,
            created_by_username="mcp",
            mode=mode,
        )
        _jobs[job_id].update(
            status="done",
            broadcast_id=broadcast_id,
            sent_ok=sent_ok,
            sent_fail=sent_fail,
        )
        logger.info(
            "[MCP] broadcast job %s done: id=%s ok=%s fail=%s",
            job_id,
            broadcast_id,
            sent_ok,
            sent_fail,
        )
    except Exception as e:  # noqa: BLE001
        logger.exception("[MCP] broadcast job %s failed: %s", job_id, e)
        _jobs[job_id].update(status="error", error=str(e))


def _create_mcp():
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP(
        "Vobla Broadcast",
        instructions=(
            "Рассылки бота Vobla в Telegram и MAX. Сначала вызови list_targets, "
            "чтобы узнать платформы, сегменты, регионы и чаты, затем send_broadcast."
        ),
        stateless_http=True,
        json_response=True,
        streamable_http_path=MCP_PATH,
    )

    @mcp.tool()
    async def list_targets() -> Dict[str, Any]:
        """Доступные платформы, сегменты (modes), регионы и чаты для выбора рассылки."""
        regions_tg, regions_max, chats_tg, chats_max = await asyncio.gather(
            asyncio.to_thread(read_active_regions, "telegram"),
            asyncio.to_thread(read_active_regions, "max"),
            asyncio.to_thread(read_active_recipients_chats_with_names, "telegram"),
            asyncio.to_thread(read_active_recipients_chats_with_names, "max"),
        )
        return {
            "platforms": list(_VALID_PLATFORMS),
            "modes": [
                {"mode": "users", "desc": "Все активные пользователи (личные чаты)"},
                {"mode": "chats", "desc": "Все активные группы/каналы"},
                {"mode": "users_chats", "desc": "Пользователи и чаты вместе"},
                {
                    "mode": "selected_regions",
                    "desc": "Чаты выбранных регионов; нужен regions + audience_platform",
                },
                {
                    "mode": "selected_chats",
                    "desc": "Конкретные chat_id; нужен chat_ids + audience_platform",
                },
            ],
            "regions": {"telegram": regions_tg, "max": regions_max},
            "chats": {"telegram": chats_tg, "max": chats_max},
        }

    @mcp.tool()
    async def send_broadcast(
        text: str = "",
        platforms: Optional[List[str]] = None,
        mode: str = "users",
        regions: Optional[List[str]] = None,
        chat_ids: Optional[List[int]] = None,
        audience_platform: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        test_chat_id: Optional[int] = None,
        test_platform: Optional[str] = None,
        test_is_chat: bool = False,
    ) -> Dict[str, Any]:
        """Запускает рассылку в фоне и сразу возвращает job_id.

        text: текст сообщения (HTML).
        platforms: ["telegram"], ["max"] или оба.
        mode: users | chats | users_chats | selected_regions | selected_chats.
        regions: список регионов для mode=selected_regions.
        chat_ids: список chat_id для mode=selected_chats.
        audience_platform: платформа адресатов для selected_*; по умолчанию первая из platforms.
        attachments: [{"type":"photo|video|document","url":...,"filename"?:...}].
        test_chat_id/test_platform/test_is_chat: тест-отправка одному адресату (минуя сегменты).
        """
        platforms_n = _normalize_platforms(platforms)

        if "max" in platforms_n and not (ENABLE_MAX and MAX_BOT_TOKEN):
            return {"ok": False, "error": "MAX не настроен (ENABLE_MAX/MAX_BOT_TOKEN)."}

        media_json = ""
        if attachments:
            media_json = await build_media_json_from_urls(
                attachments,
                platforms_n,
                max_adapter=_max_adapter_instance() if "max" in platforms_n else None,
            )

        # Получатели: тест-режим или сегмент.
        if test_chat_id is not None:
            dest: Platform = _normalize_platforms([test_platform] if test_platform else platforms_n)[0]
            recipients = [
                Recipient(platform=dest, chat_or_user_id=int(test_chat_id), is_chat=test_is_chat)
            ]
        else:
            mode_extra: Dict[str, Any] = {}
            if mode in ("selected_regions", "selected_chats"):
                ap = (audience_platform or platforms_n[0]).strip().lower()
                if ap not in _VALID_PLATFORMS:
                    return {"ok": False, "error": f"audience_platform должен быть одним из {_VALID_PLATFORMS}"}
                mode_extra["audience_platform"] = ap
                if mode == "selected_regions":
                    mode_extra["regions"] = regions or []
                else:
                    mode_extra["chat_ids"] = chat_ids or []
            recipients = await asyncio.to_thread(
                get_broadcast_recipients_list, mode, mode_extra
            )
            recipients = [r for r in recipients if r.platform in platforms_n]

        if not recipients:
            return {"ok": False, "error": "Нет получателей для заданных параметров."}
        if not (text or media_json):
            return {"ok": False, "error": "Пустое сообщение: нужен text или attachments."}

        adapters = _build_adapters(platforms_n)
        job_id = uuid.uuid4().hex[:12]
        _jobs[job_id] = {
            "status": "started",
            "recipients": len(recipients),
            "platforms": platforms_n,
            "mode": "test" if test_chat_id is not None else mode,
        }
        task = asyncio.create_task(
            _run_job(job_id, adapters, recipients, text, media_json, mode)
        )
        _tasks.add(task)
        task.add_done_callback(_tasks.discard)

        return {
            "ok": True,
            "job_id": job_id,
            "status": "started",
            "recipients": len(recipients),
            "platforms": platforms_n,
        }

    @mcp.tool()
    async def get_broadcast_status(id: str) -> Dict[str, Any]:
        """Статус рассылки по job_id (из send_broadcast) или по broadcast_id (из таблицы)."""
        key = (id or "").strip()
        if key in _jobs:
            return {"ok": True, "source": "job", **_jobs[key]}
        draft = await asyncio.to_thread(get_broadcast_draft_by_id, key)
        if not draft:
            return {"ok": False, "error": "Не найдено по job_id и broadcast_id."}
        return {
            "ok": True,
            "source": "sheet",
            "broadcast_id": key,
            "status": draft.get("status", ""),
            "sent_ok": draft.get("sent_ok", ""),
            "sent_fail": draft.get("sent_fail", ""),
        }

    return mcp


def build_app():
    """Starlette-приложение MCP со streamable HTTP, проверкой ключа и /health."""
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import JSONResponse

    mcp = _create_mcp()

    @mcp.custom_route("/health", methods=["GET"])
    async def health(request):  # noqa: ANN001
        return JSONResponse({"status": "ok", "mcp": True})

    app = mcp.streamable_http_app()

    class ApiKeyMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):  # noqa: ANN001
            if request.url.path.rstrip("/") == "/health":
                return await call_next(request)
            if MCP_API_KEY:
                auth = request.headers.get("Authorization", "")
                token = auth[7:].strip() if auth.lower().startswith("bearer ") else auth.strip()
                if not token:
                    token = request.headers.get("X-Api-Key", "").strip()
                if token != MCP_API_KEY:
                    return JSONResponse({"error": "unauthorized"}, status_code=401)
            return await call_next(request)

    app.add_middleware(ApiKeyMiddleware)
    return app


def main():
    if not MCP_API_KEY:
        logger.warning(
            "[MCP] MCP_API_KEY не задан — MCP-эндпоинт открыт без проверки ключа! "
            "Задайте MCP_API_KEY в продакшене."
        )
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("PORT") or os.getenv("MCP_PORT", "8080"))

    import uvicorn

    logger.info("[MCP] start streamable HTTP on %s:%s path=%s", host, port, MCP_PATH)
    uvicorn.run(build_app(), host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
