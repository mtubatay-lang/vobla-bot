"""Telegram-адаптер: маппинг внутренних типов ↔ aiogram, отправка через Bot."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from aiogram import Bot
from aiogram.enums import ChatAction, ParseMode
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)

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


def from_telegram_message(message: Message) -> IncomingMessage:
    """Собрать IncomingMessage из aiogram Message."""
    from_user = message.from_user
    user = InternalUser(
        id=from_user.id if from_user else 0,
        platform="telegram",
        username=from_user.username if from_user else None,
        name=from_user.first_name if from_user else None,
        full_name=from_user.full_name if from_user else None,
    )
    chat = InternalChat(
        id=message.chat.id,
        platform="telegram",
        is_group=message.chat.type in ("group", "supergroup"),
        title=getattr(message.chat, "title", None),
        username=getattr(message.chat, "username", None),
    )
    text = (message.text or message.caption or "").strip()
    attachments: List[Dict[str, Any]] = []
    if message.photo:
        attachments.append({"type": "photo", "file_id": message.photo[-1].file_id})
    if message.document:
        attachments.append({"type": "document", "file_id": message.document.file_id})
    if message.video:
        attachments.append({"type": "video", "file_id": message.video.file_id})
    if message.voice:
        attachments.append({"type": "voice", "file_id": message.voice.file_id})
    is_command = bool(text.startswith("/"))
    return IncomingMessage(
        user=user,
        chat=chat,
        text=text,
        attachments=attachments,
        is_command=is_command,
        reply_to_message_id=message.reply_to_message.message_id if message.reply_to_message else None,
        raw=message,
    )


def from_telegram_callback(callback: CallbackQuery) -> CallbackEvent:
    """Собрать CallbackEvent из aiogram CallbackQuery."""
    from_user = callback.from_user
    user = InternalUser(
        id=from_user.id if from_user else 0,
        platform="telegram",
        username=from_user.username if from_user else None,
        name=from_user.first_name if from_user else None,
        full_name=from_user.full_name if from_user else None,
    )
    chat = InternalChat(
        id=callback.message.chat.id if callback.message else 0,
        platform="telegram",
        is_group=callback.message.chat.type in ("group", "supergroup") if callback.message else False,
        title=getattr(callback.message.chat, "title", None) if callback.message else None,
        username=getattr(callback.message.chat, "username", None) if callback.message else None,
    )
    return CallbackEvent(
        user=user,
        chat=chat,
        data=callback.data or "",
        original_message_id=callback.message.message_id if callback.message else None,
        raw=callback,
        callback_id=str(callback.id) if callback.id is not None else None,
    )


def rows_to_inline_markup(rows: Optional[List[KeyboardRow]]) -> Optional[InlineKeyboardMarkup]:
    """Преобразовать платформо-независимые ряды кнопок в InlineKeyboardMarkup."""
    if not rows:
        return None
    inline_rows = []
    for row in rows:
        inline_row = []
        for btn in row:
            if btn.callback_data:
                inline_row.append(InlineKeyboardButton(text=btn.text, callback_data=btn.callback_data))
            elif btn.url:
                inline_row.append(InlineKeyboardButton(text=btn.text, url=btn.url))
        if inline_row:
            inline_rows.append(inline_row)
    if not inline_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=inline_rows)


class TelegramAdapter:
    """Адаптер мессенджера для Telegram (aiogram Bot)."""

    def __init__(self, bot: Bot) -> None:
        self._bot = bot

    @property
    def platform(self) -> Platform:
        return "telegram"

    @property
    def bot(self) -> Bot:
        """Доступ к aiogram Bot для кода, ещё не переведённого на адаптер."""
        return self._bot

    async def send_message(self, msg: OutgoingMessage) -> Optional[int]:
        """Отправить сообщение. Возвращает message_id."""
        reply_markup = rows_to_inline_markup(msg.keyboard_rows)
        parse_mode = msg.parse_mode or ParseMode.HTML
        sent = await self._bot.send_message(
            chat_id=msg.chat_id,
            text=msg.text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            reply_to_message_id=msg.reply_to_message_id,
        )
        return sent.message_id

    async def edit_message(
        self,
        chat_id: int | str,
        message_id: int | str,
        text: str,
        keyboard_rows: Optional[List[KeyboardRow]] = None,
        parse_mode: Optional[str] = None,
    ) -> None:
        """Редактировать существующее сообщение."""
        reply_markup = rows_to_inline_markup(keyboard_rows)
        parse_mode = parse_mode or ParseMode.HTML
        await self._bot.edit_message_text(
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
        """Ответить на callback (убрать «часики»)."""
        await self._bot.answer_callback_query(
            callback_query_id=callback_id_or_equivalent,
            text=text,
        )

    async def send_typing(self, chat_id: int | str, **kwargs: Any) -> None:
        """Показать индикатор набора текста."""
        await self._bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    async def send_media(
        self,
        chat_id: int | str,
        attachments: List[Dict[str, Any]],
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int | str] = None,
    ) -> None:
        """Отправить медиа (фото/видео/документы)."""
        photos = [a for a in attachments if a.get("type") == "photo"]
        videos = [a for a in attachments if a.get("type") == "video"]
        documents = [a for a in attachments if a.get("type") == "document"]
        for i in range(0, len(photos), 10):
            batch = photos[i : i + 10]
            media_group = [
                InputMediaPhoto(media=a["file_id"], caption=caption if idx == 0 else None)
                for idx, a in enumerate(batch)
            ]
            if media_group:
                await self._bot.send_media_group(chat_id=chat_id, media=media_group)
        for i in range(0, len(videos), 10):
            batch = videos[i : i + 10]
            media_group = [
                InputMediaVideo(media=a["file_id"], caption=caption if idx == 0 else None)
                for idx, a in enumerate(batch)
            ]
            if media_group:
                await self._bot.send_media_group(chat_id=chat_id, media=media_group)
        for att in documents:
            await self._bot.send_document(
                chat_id=chat_id,
                document=att["file_id"],
                caption=caption,
            )

    async def download_file(self, file_id_or_url: str) -> bytes:
        """Скачать файл по file_id. В Telegram file_id — это id, не URL."""
        file = await self._bot.get_file(file_id_or_url)
        data = await self._bot.download_file(file.file_path)
        if hasattr(data, "read"):
            return data.read()
        return bytes(data)
