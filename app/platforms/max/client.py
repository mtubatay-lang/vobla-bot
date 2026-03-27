"""
HTTP-клиент к platform-api.max.ru для бота MAX.
Спецификация: https://dev.max.ru/docs-api
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

import aiohttp

logger = logging.getLogger(__name__)


class MaxApiClientError(Exception):
    """Ошибка вызова MAX API."""

    def __init__(self, message: str, status: Optional[int] = None, body: Optional[str] = None):
        self.status = status
        self.body = body
        detail = (body or "").strip()
        if detail and detail not in message:
            message = f"{message}: {detail[:800]}"
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

    async def get_message(self, message_id: str | int) -> Dict[str, Any]:
        """GET /messages/{messageId} — метаданные и body с вложениями (в т.ч. url для скачивания)."""
        mid = str(message_id).strip()
        path_mid = quote(mid, safe="-_.")
        return await self._request("GET", f"/messages/{path_mid}")

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
        """
        POST /answers?callback_id=...
        Разные сборки API MAX по-разному принимают тело; при 400 перебираем варианты.
        """
        cid = (callback_id or "").strip()
        if not cid:
            logger.warning("MAX answer_callback: пустой callback_id, пропуск")
            return
        url = f"{self._base_url}/answers?{urlencode({'callback_id': cid})}"
        auth_h = {"Authorization": self._authorization_value()}

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            if notification:
                headers = {**auth_h, "Content-Type": "application/json"}
                async with session.post(
                    url, headers=headers, json={"notification": notification}
                ) as resp:
                    body = await resp.text()
                    if resp.status >= 400:
                        raise MaxApiClientError(
                            f"MAX API error: {resp.status}",
                            status=resp.status,
                            body=body,
                        )
                return

            attempts: list[tuple[str, str, Any]] = [
                # По логам this variant стабильно проходит у текущего API MAX.
                ("post_json_notification_empty", "json", {"notification": ""}),
                ("post_no_body", "no_json", None),
                ("post_json_empty", "json", {}),
                ("post_raw_braces", "data", b"{}"),
            ]
            last_status, last_body = 0, ""
            for name, kind, payload in attempts:
                try:
                    hdrs = (
                        auth_h
                        if kind == "no_json"
                        else {**auth_h, "Content-Type": "application/json"}
                    )
                    if kind == "no_json":
                        async with session.post(url, headers=hdrs) as resp:
                            last_status, last_body = resp.status, await resp.text()
                    elif kind == "json":
                        async with session.post(url, headers=hdrs, json=payload) as resp:
                            last_status, last_body = resp.status, await resp.text()
                    else:
                        async with session.post(url, headers=hdrs, data=payload) as resp:
                            last_status, last_body = resp.status, await resp.text()
                except Exception as e:
                    logger.debug("MAX /answers [%s] request error: %s", name, e)
                    continue
                if last_status < 400:
                    logger.info("MAX /answers ok via strategy %s", name)
                    return
                logger.debug(
                    "MAX /answers [%s] -> %s body=%r",
                    name,
                    last_status,
                    (last_body or "")[:400],
                )

            logger.warning(
                "MAX /answers failed for all strategies callback_id=%s status=%s body=%r",
                cid,
                last_status or 400,
                (last_body or "")[:400],
            )
            raise MaxApiClientError(
                f"MAX API error: {last_status}",
                status=last_status or 400,
                body=last_body,
            )

    async def get_group_chat(self, chat_id: str | int) -> Dict[str, Any]:
        """
        GET /chats/{chatId} — метаданные группового чата (title и т.д.).
        https://dev.max.ru/docs-api/methods/GET/chats/-chatId-
        """
        cid = str(chat_id).strip()
        data = await self._request("GET", f"/chats/{cid}")
        if isinstance(data.get("chat"), dict):
            return data["chat"]
        return data

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
