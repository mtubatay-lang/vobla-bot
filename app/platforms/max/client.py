"""
HTTP-клиент к platform-api.max.ru для бота MAX.
Спецификация: https://dev.max.ru/docs-api
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

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
        *,
        use_bearer_prefix: bool = True,
    ) -> None:
        self._token = token.strip()
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._use_bearer_prefix = use_bearer_prefix

    def _authorization_value(self) -> str:
        """Значение заголовка Authorization (см. https://dev.max.ru/docs-api)."""
        auth = self._token
        if auth.lower().startswith("bearer "):
            return auth
        if self._use_bearer_prefix:
            return f"Bearer {auth}"
        return auth

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": self._authorization_value(),
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        data: Any = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        if params:
            q = {k: v for k, v in params.items() if v is not None}
            if q:
                url = f"{url}?{urlencode(q)}"
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

    def message_id_from_response(self, result: Dict[str, Any]) -> Optional[str]:
        msg = result.get("message")
        if isinstance(msg, dict):
            mid = msg.get("id") or msg.get("message_id")
            if mid is not None:
                return str(mid)
        mid = result.get("message_id") or result.get("id")
        return str(mid) if mid is not None else None

    async def send_message(
        self,
        chat_id: str | int,
        text: str,
        *,
        is_group_chat: bool = False,
        text_format: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        reply_to_message_id: Optional[str | int] = None,
    ) -> Dict[str, Any]:
        """
        POST /messages
        Личка: query user_id; группа: query chat_id.
        """
        params: Dict[str, Any] = {}
        if is_group_chat:
            params["chat_id"] = str(chat_id)
        else:
            params["user_id"] = str(chat_id)

        body: Dict[str, Any] = {"text": text}
        if text_format:
            body["format"] = text_format
        if attachments:
            body["attachments"] = attachments

        return await self._request("POST", "/messages", json=body, params=params)

    async def edit_message(
        self,
        message_id: str | int,
        text: str,
        *,
        text_format: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """PUT /messages?message_id=..."""
        body: Dict[str, Any] = {"text": text}
        if text_format:
            body["format"] = text_format
        if attachments is not None:
            body["attachments"] = attachments
        return await self._request(
            "PUT",
            "/messages",
            json=body,
            params={"message_id": str(message_id)},
        )

    async def answer_callback(
        self,
        callback_id: str,
        *,
        notification: Optional[str] = None,
    ) -> None:
        """POST /answers?callback_id=..."""
        params = {"callback_id": callback_id}
        body: Optional[Dict[str, Any]] = None
        if notification:
            body = {"notification": notification}
        await self._request("POST", "/answers", json=body, params=params)

    async def send_typing(self, chat_id: str | int, *, is_group_chat: bool = False) -> None:
        """Индикатор набора (если метод есть в API)."""
        try:
            params: Dict[str, Any] = (
                {"chat_id": str(chat_id)} if is_group_chat else {"user_id": str(chat_id)}
            )
            await self._request("POST", "/chats/typing", params=params)
        except MaxApiClientError as e:
            if e.status != 404:
                logger.warning("MAX send_typing failed: %s", e)

    async def upload_file(self, file_bytes: bytes, filename: str, mime_type: str) -> str:
        """POST /upload — multipart."""
        url = f"{self._base_url}/upload"
        headers = {"Authorization": self._authorization_value()}
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
        """GET /updates — long polling (dev)."""
        params: Dict[str, Any] = {"limit": limit}
        if offset:
            params["offset"] = offset
        result = await self._request("GET", "/updates", params=params)
        updates = result.get("updates") or result.get("result") or []
        return updates if isinstance(updates, list) else []
