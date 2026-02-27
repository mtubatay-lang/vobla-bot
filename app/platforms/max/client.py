"""
HTTP-клиент к platform-api.max.ru для бота MAX.
Базовый URL и эндпоинты могут потребовать уточнения по официальной документации MAX.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class MaxApiClientError(Exception):
    """Ошибка вызова MAX API."""
    def __init__(self, message: str, status: Optional[int] = None, body: Optional[str] = None):
        self.status = status
        self.body = body
        super().__init__(message)


class MaxApiClient:
    """Асинхронный HTTP-клиент к MAX Platform API."""

    def __init__(
        self,
        token: str,
        base_url: str = "https://platform-api.max.ru",
        timeout: float = 30.0,
    ) -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        data: Any = None,
    ) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            async with session.request(
                method,
                url,
                headers=self._headers(),
                json=json,
                data=data,
            ) as resp:
                body = await resp.text()
                try:
                    parsed = await resp.json() if body else {}
                except Exception:
                    parsed = {}
                if resp.status >= 400:
                    raise MaxApiClientError(
                        f"MAX API error: {resp.status}",
                        status=resp.status,
                        body=body,
                    )
                return parsed if isinstance(parsed, dict) else {"result": parsed}

    async def send_message(
        self,
        chat_id: str | int,
        text: str,
        parse_mode: Optional[str] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        reply_to_message_id: Optional[str | int] = None,
    ) -> Dict[str, Any]:
        """Отправить текстовое сообщение.
        Эндпоинт и формат тела зависят от документации MAX.
        """
        body: Dict[str, Any] = {
            "chat_id": str(chat_id),
            "text": text,
        }
        if parse_mode:
            body["parse_mode"] = parse_mode
        if reply_markup:
            body["reply_markup"] = reply_markup
        if reply_to_message_id is not None:
            body["reply_to_message_id"] = str(reply_to_message_id)
        return await self._request("POST", "/v1/messages/send", json=body)

    async def edit_message(
        self,
        chat_id: str | int,
        message_id: str | int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Редактировать сообщение."""
        body: Dict[str, Any] = {
            "chat_id": str(chat_id),
            "message_id": str(message_id),
            "text": text,
        }
        if reply_markup is not None:
            body["reply_markup"] = reply_markup
        if parse_mode:
            body["parse_mode"] = parse_mode
        return await self._request("POST", "/v1/messages/edit", json=body)

    async def answer_callback(
        self,
        callback_id: str,
        text: Optional[str] = None,
    ) -> None:
        """Ответить на callback (убрать индикатор загрузки)."""
        body: Dict[str, Any] = {"callback_id": callback_id}
        if text:
            body["text"] = text
        await self._request("POST", "/v1/callbacks/answer", json=body)

    async def send_typing(self, chat_id: str | int) -> None:
        """Показать индикатор набора (если API поддерживает)."""
        try:
            await self._request("POST", "/v1/chats/typing", json={"chat_id": str(chat_id)})
        except MaxApiClientError as e:
            if e.status != 404:
                logger.warning("MAX send_typing failed: %s", e)

    async def upload_file(self, file_bytes: bytes, filename: str, mime_type: str) -> str:
        """Загрузить файл и получить id/url для вложения.
        Возвращает идентификатор вложения для использования в send_media.
        """
        # Многие API принимают multipart/form-data для загрузки
        url = f"{self._base_url}/v1/upload"
        headers = {"Authorization": f"Bearer {self._token}"}
        form = aiohttp.FormData()
        form.add_field("file", file_bytes, filename=filename, content_type=mime_type)
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            async with session.post(url, headers=headers, data=form) as resp:
                body = await resp.json() if resp.content_type == "application/json" else {}
                if resp.status >= 400:
                    raise MaxApiClientError(
                        f"MAX upload error: {resp.status}",
                        status=resp.status,
                        body=await resp.text(),
                    )
                file_id = body.get("file_id") or body.get("id") or body.get("url") or ""
                return str(file_id)

    async def get_updates(self, offset: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Long polling: получить обновления (если API поддерживает).
        Возвращает список update-объектов.
        """
        params: Dict[str, Any] = {"limit": limit}
        if offset:
            params["offset"] = offset
        # Предполагаем GET /v1/updates или /v1/subscriptions/poll
        result = await self._request("GET", "/v1/updates")
        updates = result.get("updates") or result.get("result") or []
        return updates if isinstance(updates, list) else []
