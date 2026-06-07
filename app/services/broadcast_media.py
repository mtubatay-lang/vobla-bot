"""Сборка media_json v2 для рассылок из внешних URL (Telegram + MAX).

Внешний агент передаёт вложения списком URL. Для Telegram URL уходит напрямую
(aiogram InputMedia/send_document принимает ссылку). Для MAX файл нужно скачать
и загрузить через POST /upload (MaxAdapter.upload_document_bytes → file_id/token).

Формат результата совпадает с media_json v2, который понимает
``parse_broadcast_media_for_platform`` в broadcast_service.
"""

from __future__ import annotations

import json
import logging
import mimetypes
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

import aiohttp

from app.core.types import Platform

logger = logging.getLogger(__name__)

_TG_TYPES = ("photo", "video", "document")
# Внутренний тип -> тип вложения MAX (image/video/file).
_MAX_TYPE_MAP = {"photo": "image", "video": "video", "document": "file"}


def _normalize_type(raw: str) -> str:
    """Приводит тип вложения к telegram-нотации: photo | video | document."""
    x = (raw or "").strip().lower()
    if x in ("photo", "image", "img"):
        return "photo"
    if x in ("video",):
        return "video"
    if x in ("document", "doc", "file"):
        return "document"
    return "document"


def _filename_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:
        return ""
    if not path:
        return ""
    return unquote(path.rsplit("/", 1)[-1])


async def _download(url: str) -> Tuple[bytes, Optional[str]]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.read()
            ct = resp.headers.get("Content-Type")
            mime = ct.split(";")[0].strip().lower() if ct else None
            return data, mime


async def build_media_json_from_urls(
    attachments: Sequence[Dict[str, Any]],
    platforms: Sequence[Platform],
    max_adapter: Optional[Any] = None,
) -> str:
    """Собирает media_json v2 из списка вложений-URL.

    attachments: [{"type": "photo|video|document", "url": str, "filename"?: str, "mime_type"?: str}].
    platforms: для каких платформ готовить вложения ("telegram", "max").
    max_adapter: MaxAdapter для загрузки файлов (нужен, если "max" в platforms).

    Возвращает JSON-строку: {"version": 2, "telegram": [...], "max": [...]}.
    """
    tg: List[Dict[str, Any]] = []
    mx: List[Dict[str, Any]] = []
    want_tg = "telegram" in platforms
    want_max = "max" in platforms

    for att in attachments or []:
        if not isinstance(att, dict):
            continue
        url = str(att.get("url") or att.get("file_id") or att.get("id_or_url") or "").strip()
        if not url:
            continue
        typ = _normalize_type(str(att.get("type") or "document"))
        filename = str(att.get("filename") or att.get("file_name") or "").strip()

        if want_tg:
            tg.append({"type": typ, "file_id": url})

        if want_max and max_adapter is not None:
            try:
                data, ct = await _download(url)
            except Exception as e:
                logger.warning("MCP media: не удалось скачать %s для MAX: %s", url, e)
                continue
            if not data:
                continue
            mime = str(
                att.get("mime_type")
                or ct
                or mimetypes.guess_type(filename or url)[0]
                or "application/octet-stream"
            ).split(";")[0].strip().lower()
            fn = filename or _filename_from_url(url) or "upload.bin"
            try:
                file_id = await max_adapter.upload_document_bytes(data, fn, mime)
            except Exception as e:
                logger.warning("MCP media: загрузка в MAX не удалась для %s: %s", url, e)
                continue
            if not file_id:
                continue
            entry: Dict[str, Any] = {
                "type": _MAX_TYPE_MAP.get(typ, "file"),
                "id_or_url": file_id,
                "file_id": file_id,
            }
            if filename:
                entry["filename"] = filename
            if mime:
                entry["mime_type"] = mime
            mx.append(entry)

    return json.dumps({"version": 2, "telegram": tg, "max": mx}, ensure_ascii=False)
