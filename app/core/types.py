"""
Платформо-независимые типы для мультимессенджерного ядра (Telegram, MAX).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

Platform = Literal["telegram", "max"]


@dataclass(frozen=True)
class InternalUser:
    """Пользователь в едином представлении."""
    id: int | str
    platform: Platform
    username: Optional[str] = None
    name: Optional[str] = None
    full_name: Optional[str] = None


@dataclass(frozen=True)
class InternalChat:
    """Чат в едином представлении."""
    id: int | str
    platform: Platform
    is_group: bool = False
    title: Optional[str] = None
    username: Optional[str] = None


@dataclass
class KeyboardButton:
    """Одна кнопка клавиатуры (callback или link)."""
    text: str
    callback_data: Optional[str] = None
    url: Optional[str] = None


# Ряд кнопок — список KeyboardButton
KeyboardRow = list[KeyboardButton]


@dataclass
class IncomingMessage:
    """Входящее сообщение от пользователя (платформо-независимое)."""
    user: InternalUser
    chat: InternalChat
    text: str
    attachments: list[dict[str, Any]] = field(default_factory=list)
    is_command: bool = False
    reply_to_message_id: Optional[int | str] = None
    raw: Any = None


@dataclass
class CallbackEvent:
    """Событие нажатия inline-кнопки (платформо-независимое)."""
    user: InternalUser
    chat: InternalChat
    data: str
    original_message_id: Optional[int | str] = None
    raw: Any = None
    # MAX: POST /answers?callback_id=... (Telegram не использует)
    callback_id: Optional[str] = None


@dataclass
class OutgoingMessage:
    """Исходящее сообщение для отправки через адаптер."""
    chat_id: int | str
    platform: Platform
    text: str
    parse_mode: Optional[str] = None
    attachments: list[dict[str, Any]] = field(default_factory=list)
    keyboard_rows: Optional[list[KeyboardRow]] = None
    reply_to_message_id: Optional[int | str] = None
    # MAX: POST /messages — user_id (личка) или chat_id (группа)
    is_group_chat: bool = False


@dataclass
class MediaAttachment:
    """Вложение (фото, видео, документ) в платформо-независимом виде."""
    type: Literal["photo", "video", "document"]
    id_or_url: str  # file_id в Telegram или url/id после upload в MAX
    caption: Optional[str] = None
    platform: Optional[Platform] = None


@dataclass
class Recipient:
    """Получатель рассылки с указанием платформы."""
    platform: Platform
    chat_or_user_id: int | str
    is_chat: bool = False
