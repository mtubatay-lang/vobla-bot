"""
MAX-адаптер: преобразование внутренних типов ↔ MAX API, реализация MessengerAdapter.
"""

from __future__ import annotations

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


def parse_max_update(update: Dict[str, Any]) -> Optional[IncomingMessage | CallbackEvent]:
    """
    Разобрать входящий Update от MAX в IncomingMessage или CallbackEvent.
    Формат update зависит от документации MAX (message_new, message_callback и т.д.).
    """
    if not update:
        return None

    # Типичные поля: type, message, callback_query, chat, from/user
    update_type = update.get("type") or update.get("event_type")
    if not update_type:
        return None

    # Извлекаем пользователя и чат из update
    raw_user = update.get("user") or update.get("from") or update.get("message", {}).get("from") or {}
    raw_chat = update.get("chat") or update.get("message", {}).get("chat") or {}

    user_id = raw_user.get("id") or raw_user.get("user_id")
    if user_id is None:
        return None

    user = InternalUser(
        id=user_id,
        platform="max",
        username=raw_user.get("username"),
        name=raw_user.get("first_name") or raw_user.get("name"),
        full_name=raw_user.get("full_name") or raw_user.get("name"),
    )
    chat_id = raw_chat.get("id") or raw_chat.get("chat_id") or update.get("chat_id")
    if chat_id is None:
        chat_id = user_id  # личка
    chat = InternalChat(
        id=chat_id,
        platform="max",
        is_group=raw_chat.get("type") == "group" or bool(raw_chat.get("is_group")),
        title=raw_chat.get("title"),
        username=raw_chat.get("username"),
    )

    # Callback (нажатие inline-кнопки)
    if update_type in ("message_callback", "callback_query", "callback"):
        callback_data = (
            update.get("callback_data")
            or update.get("data")
            or (update.get("callback_query") or {}).get("data")
            or ""
        )
        return CallbackEvent(
            user=user,
            chat=chat,
            data=str(callback_data),
            original_message_id=update.get("message_id") or (update.get("message") or {}).get("id"),
            raw=update,
        )

    # Обычное сообщение
    msg = update.get("message") or update
    text = (msg.get("text") or msg.get("message") or "").strip()
    attachments: List[Dict[str, Any]] = []
    for att in msg.get("attachments") or msg.get("media") or []:
        att_type = att.get("type") or att.get("media_type") or "document"
        file_id = att.get("file_id") or att.get("id") or att.get("url")
        if file_id:
            attachments.append({"type": att_type, "file_id": file_id, "id_or_url": file_id})

    return IncomingMessage(
        user=user,
        chat=chat,
        text=text,
        attachments=attachments,
        is_command=text.startswith("/"),
        reply_to_message_id=msg.get("reply_to_message_id") or (msg.get("reply_to_message") or {}).get("id"),
        raw=update,
    )


def _rows_to_max_keyboard(rows: Optional[List[KeyboardRow]]) -> Optional[Dict[str, Any]]:
    """Преобразовать ряды кнопок в формат клавиатуры MAX (InlineKeyboardAttachment и т.п.)."""
    if not rows:
        return None
    # Типичный формат: {"inline_keyboard": [[{"text": "...", "callback_data": "..."}], ...]}
    inline = []
    for row in rows:
        r = []
        for btn in row:
            b: Dict[str, Any] = {"text": btn.text}
            if btn.callback_data:
                b["callback_data"] = btn.callback_data
            if btn.url:
                b["url"] = btn.url
            r.append(b)
        if r:
            inline.append(r)
    if not inline:
        return None
    return {"inline_keyboard": inline}


class MaxAdapter:
    """Адаптер мессенджера для MAX."""

    def __init__(self, client: MaxApiClient) -> None:
        self._client = client

    @property
    def platform(self) -> Platform:
        return "max"

    async def send_message(self, msg: OutgoingMessage) -> Optional[str]:
        """Отправить сообщение. Возвращает message_id (строка)."""
        reply_markup = _rows_to_max_keyboard(msg.keyboard_rows)
        result = await self._client.send_message(
            chat_id=msg.chat_id,
            text=msg.text,
            parse_mode=msg.parse_mode,
            reply_markup=reply_markup,
            reply_to_message_id=msg.reply_to_message_id,
        )
        return result.get("message_id") or result.get("id")

    async def edit_message(
        self,
        chat_id: int | str,
        message_id: int | str,
        text: str,
        keyboard_rows: Optional[List[KeyboardRow]] = None,
        parse_mode: Optional[str] = None,
    ) -> None:
        """Редактировать сообщение."""
        reply_markup = _rows_to_max_keyboard(keyboard_rows)
        await self._client.edit_message(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )

    async def answer_callback(
        self,
        callback_id_or_equivalent: str | int,
        text: Optional[str] = None,
    ) -> None:
        """Ответить на callback."""
        await self._client.answer_callback(
            callback_id=str(callback_id_or_equivalent),
            text=text,
        )

    async def send_typing(self, chat_id: int | str) -> None:
        """Показать индикатор набора."""
        await self._client.send_typing(str(chat_id))

    async def send_media(
        self,
        chat_id: int | str,
        attachments: List[Dict[str, Any]],
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int | str] = None,
    ) -> None:
        """Отправить медиа. В MAX — через upload и затем отправка по file_id/url."""
        if not attachments:
            return None
        # Упрощённо: если есть id_or_url — отправляем как вложение; иначе нужна загрузка
        for att in attachments:
            file_id = att.get("id_or_url") or att.get("file_id")
            if not file_id:
                continue
            await self._client._request(
                "POST",
                "/v1/messages/send",
                json={
                    "chat_id": str(chat_id),
                    "attachment": {"type": att.get("type", "document"), "file_id": str(file_id)},
                    "caption": caption,
                    "reply_to_message_id": str(reply_to_message_id) if reply_to_message_id else None,
                },
            )

    async def download_file(self, file_id_or_url: str) -> bytes:
        """Скачать файл по file_id или URL."""
        import aiohttp
        if file_id_or_url.startswith("http://") or file_id_or_url.startswith("https://"):
            async with aiohttp.ClientSession() as session:
                async with session.get(file_id_or_url) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        # Иначе запрос к API MAX на получение файла
        result = await self._client._request("GET", f"/v1/files/{file_id_or_url}")
        url = result.get("url") or result.get("file_url")
        if url:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        raise ValueError("MAX API did not return file content or URL")
