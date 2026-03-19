"""
MAX-адаптер: преобразование внутренних типов ↔ MAX API, реализация MessengerAdapter.
Спецификация webhook Update: https://dev.max.ru/docs-api/objects/Update
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.core.types import (
    CallbackEvent,
    IncomingMessage,
    InternalChat,
    InternalUser,
    KeyboardButton,
    KeyboardRow,
    OutgoingMessage,
    Platform,
)

from app.platforms.max.client import MaxApiClient

logger = logging.getLogger(__name__)


def _user_from_max(obj: Optional[Dict[str, Any]]) -> Optional[InternalUser]:
    if not obj or not isinstance(obj, dict):
        return None
    uid = obj.get("user_id")
    if uid is None:
        uid = obj.get("id")
    if uid is None:
        return None
    return InternalUser(
        id=uid,
        platform="max",
        username=obj.get("username"),
        name=obj.get("name") or obj.get("first_name"),
        full_name=obj.get("full_name") or obj.get("name"),
    )


def _parse_recipient_peer(
    recipient: Optional[Dict[str, Any]],
    sender: InternalUser,
) -> tuple[Any, bool, InternalChat]:
    """
    peer_id для POST /messages (user_id или chat_id), is_group, InternalChat.
    """
    if not recipient or not isinstance(recipient, dict):
        uid = sender.id
        chat = InternalChat(
            id=uid,
            platform="max",
            is_group=False,
            title=None,
            username=sender.username,
        )
        return uid, False, chat

    rtype = str(recipient.get("type") or recipient.get("recipient_type") or "").lower()
    chat_nested = recipient.get("chat") if isinstance(recipient.get("chat"), dict) else {}

    # Группа / канал / чат (не путать с type=dialog — в MAX это может быть личка)
    if rtype in ("chat", "group", "channel") or recipient.get("is_group"):
        cid = recipient.get("chat_id") or recipient.get("id") or chat_nested.get("id")
        if cid is None:
            cid = sender.id
        title = recipient.get("title") or chat_nested.get("title")
        chat = InternalChat(
            id=cid,
            platform="max",
            is_group=True,
            title=title,
            username=recipient.get("username") or chat_nested.get("username"),
        )
        return cid, True, chat

    # Личка с ботом: recipient почти всегда указывает на бота (его user_id).
    # POST /messages?user_id=… должен быть id пользователя-отправителя, иначе API отвечает 403.
    uid = sender.id
    chat = InternalChat(
        id=uid,
        platform="max",
        is_group=False,
        title=None,
        username=sender.username,
    )
    return uid, False, chat


def _message_body_text(msg: Dict[str, Any]) -> str:
    body = msg.get("body")
    if isinstance(body, dict):
        t = body.get("text")
        if t is not None:
            return str(t).strip()
    return str(msg.get("text") or msg.get("message") or "").strip()


def _incoming_attachments(msg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    body = msg.get("body")
    atts: List[Any] = []
    if isinstance(body, dict):
        atts = body.get("attachments") or []
    if not atts:
        atts = msg.get("attachments") or msg.get("media") or []
    for att in atts:
        if not isinstance(att, dict):
            continue
        att_type = att.get("type") or att.get("media_type") or "document"
        file_id = att.get("file_id") or att.get("id") or att.get("url")
        if file_id:
            out.append({"type": att_type, "file_id": file_id, "id_or_url": file_id})
    return out


def _reply_to_mid(msg: Dict[str, Any]) -> Optional[str | int]:
    link = msg.get("link")
    if isinstance(link, dict):
        mid = link.get("mid") or link.get("message_id") or link.get("id")
        if mid is not None:
            return mid
    return msg.get("reply_to_message_id")


def parse_max_update(update: Dict[str, Any]) -> Optional[IncomingMessage | CallbackEvent]:
    """
    Разобрать входящий Update от MAX (webhook).
    https://dev.max.ru/docs-api/objects/Update — поле update_type.
    """
    if not update or not isinstance(update, dict):
        return None

    update_type = (
        update.get("update_type")
        or update.get("type")
        or update.get("event_type")
    )
    if not update_type:
        return None

    update_type = str(update_type)

    # --- message_callback ---
    if update_type in ("message_callback", "callback_query", "callback"):
        cb = update.get("callback") or update.get("callback_query") or {}
        if not isinstance(cb, dict):
            cb = {}
        callback_id = cb.get("callback_id") or cb.get("id")
        callback_id_str = str(callback_id) if callback_id is not None else ""
        payload = cb.get("payload") or cb.get("data") or update.get("payload") or ""

        user_dict = (
            update.get("user")
            or cb.get("user")
            or cb.get("from")
            or (cb.get("message") or {}).get("sender")
            or update.get("sender")
        )
        user = _user_from_max(user_dict if isinstance(user_dict, dict) else None)
        if user is None:
            return None

        msg = cb.get("message") or update.get("message") or {}
        if not isinstance(msg, dict):
            msg = {}
        recipient = msg.get("recipient") if isinstance(msg.get("recipient"), dict) else {}
        _, _, chat = _parse_recipient_peer(recipient, user)
        mid = msg.get("id") or msg.get("message_id")

        return CallbackEvent(
            user=user,
            chat=chat,
            data=str(payload),
            original_message_id=mid,
            raw=update,
            callback_id=callback_id_str or None,
        )

    # --- bot_started → как /start ---
    if update_type == "bot_started":
        user = _user_from_max(update.get("user") or update.get("sender"))
        if user is None:
            return None
        _, _, chat = _parse_recipient_peer(None, user)
        return IncomingMessage(
            user=user,
            chat=chat,
            text="/start",
            attachments=[],
            is_command=True,
            reply_to_message_id=None,
            raw=update,
        )

    # --- message_created и прочие с полем message ---
    if update_type not in ("message_created", "message_updated", "message_edited", "message_received"):
        if update.get("message") is None:
            return None

    msg = update.get("message")
    if not isinstance(msg, dict):
        return None

    sender = _user_from_max(msg.get("sender"))
    if sender is None:
        sender = _user_from_max(update.get("user") or update.get("sender"))
    if sender is None:
        return None

    recipient = msg.get("recipient") if isinstance(msg.get("recipient"), dict) else {}
    _, is_group, chat = _parse_recipient_peer(recipient, sender)

    text = _message_body_text(msg)
    attachments = _incoming_attachments(msg)
    reply_mid = _reply_to_mid(msg)

    return IncomingMessage(
        user=sender,
        chat=chat,
        text=text,
        attachments=attachments,
        is_command=text.startswith("/"),
        reply_to_message_id=reply_mid,
        raw=update,
    )


def keyboard_rows_to_max_attachments(rows: Optional[List[KeyboardRow]]) -> Optional[List[Dict[str, Any]]]:
    """
    Вложения MAX: inline_keyboard с кнопками callback / link.
    https://dev.max.ru/docs-api — раздел «Клавиатура».
    """
    if not rows:
        return None
    button_rows: List[List[Dict[str, Any]]] = []
    for row in rows:
        r: List[Dict[str, Any]] = []
        for btn in row:
            if btn.url:
                r.append({"type": "link", "text": btn.text, "url": btn.url})
            elif btn.callback_data:
                r.append(
                    {
                        "type": "callback",
                        "text": btn.text,
                        "payload": btn.callback_data,
                    }
                )
        if r:
            button_rows.append(r)
    if not button_rows:
        return None
    return [{"type": "inline_keyboard", "payload": {"buttons": button_rows}}]


def _parse_mode_to_max_format(parse_mode: Optional[str]) -> Optional[str]:
    if not parse_mode:
        return None
    p = str(parse_mode).upper()
    if p in ("HTML", "PARSEMODE.HTML"):
        return "html"
    if p in ("MARKDOWN", "MD", "PARSEMODE.MARKDOWN"):
        return "markdown"
    if parse_mode.lower() == "html":
        return "html"
    if parse_mode.lower() == "markdown":
        return "markdown"
    return "html"


class MaxAdapter:
    """Адаптер мессенджера для MAX."""

    def __init__(self, client: MaxApiClient) -> None:
        self._client = client

    @property
    def platform(self) -> Platform:
        return "max"

    async def send_message(self, msg: OutgoingMessage) -> Optional[str]:
        """Отправить сообщение. Возвращает message_id (строка)."""
        attachments = keyboard_rows_to_max_attachments(msg.keyboard_rows)
        text_format = _parse_mode_to_max_format(msg.parse_mode) or "html"
        result = await self._client.send_message(
            chat_id=msg.chat_id,
            text=msg.text,
            is_group_chat=msg.is_group_chat,
            text_format=text_format,
            attachments=attachments,
        )
        return self._client.message_id_from_response(result)

    async def edit_message(
        self,
        chat_id: int | str,
        message_id: int | str,
        text: str,
        keyboard_rows: Optional[List[KeyboardRow]] = None,
        parse_mode: Optional[str] = None,
    ) -> None:
        """Редактировать сообщение (PUT /messages). chat_id в MAX для edit не используется в query."""
        attachments = keyboard_rows_to_max_attachments(keyboard_rows)
        text_format = _parse_mode_to_max_format(parse_mode)
        await self._client.edit_message(
            message_id=message_id,
            text=text,
            text_format=text_format,
            attachments=attachments,
        )

    async def answer_callback(
        self,
        callback_id_or_equivalent: str | int,
        text: Optional[str] = None,
    ) -> None:
        """Ответить на callback (POST /answers)."""
        await self._client.answer_callback(
            str(callback_id_or_equivalent),
            notification=text,
        )

    async def upload_document_bytes(self, data: bytes, filename: str, mime_type: str) -> str:
        """Загрузить файл в MAX, вернуть file_id для вложений."""
        return await self._client.upload_file(data, filename, mime_type)

    async def send_typing(self, chat_id: int | str, *, is_group_chat: bool = False) -> None:
        """Показать индикатор набора (лучшее усилие; эндпоинт может отличаться)."""
        await self._client.send_typing(str(chat_id), is_group_chat=is_group_chat)

    async def send_media(
        self,
        chat_id: int | str,
        attachments: List[Dict[str, Any]],
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int | str] = None,
        *,
        is_group_chat: bool = False,
    ) -> None:
        """Отправить медиа через POST /messages с вложениями."""
        if not attachments:
            return None
        max_atts: List[Dict[str, Any]] = []
        for att in attachments:
            file_id = att.get("id_or_url") or att.get("file_id")
            if not file_id:
                continue
            max_atts.append(
                {
                    "type": att.get("type", "file"),
                    "payload": {"file_id": str(file_id)},
                }
            )
        if not max_atts:
            return None
        body_text = caption or ""
        await self._client.send_message(
            chat_id,
            body_text,
            is_group_chat=is_group_chat,
            attachments=max_atts,
        )

    async def download_file(self, file_id_or_url: str) -> bytes:
        """Скачать файл по file_id или URL."""
        import aiohttp

        if file_id_or_url.startswith("http://") or file_id_or_url.startswith("https://"):
            async with aiohttp.ClientSession() as session:
                async with session.get(file_id_or_url) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        result = await self._client._request("GET", f"/files/{file_id_or_url}")
        url = result.get("url") or result.get("file_url")
        if url:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        raise ValueError("MAX API did not return file content or URL")
