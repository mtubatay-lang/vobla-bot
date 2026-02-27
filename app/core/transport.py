"""
Интерфейс адаптера мессенджера (платформо-независимый API отправки).
"""

from __future__ import annotations

from typing import Protocol, Optional

from app.core.types import KeyboardRow, OutgoingMessage, Platform


class MessengerAdapter(Protocol):
    """Протокол адаптера мессенджера. Все параметры — во внутренних типах."""

    @property
    def platform(self) -> Platform:
        """Платформа этого адаптера."""
        ...

    def send_message(self, msg: OutgoingMessage) -> Optional[int | str]:
        """Отправить сообщение. Возвращает id сообщения или None."""
        ...

    def edit_message(
        self,
        chat_id: int | str,
        message_id: int | str,
        text: str,
        keyboard_rows: Optional[list[KeyboardRow]] = None,
        parse_mode: Optional[str] = None,
    ) -> None:
        """Редактировать существующее сообщение."""
        ...

    def answer_callback(
        self,
        callback_id_or_equivalent: str | int,
        text: Optional[str] = None,
    ) -> None:
        """Ответить на callback (убрать «часики», показать alert и т.п.)."""
        ...

    def send_typing(self, chat_id: int | str) -> None:
        """Показать индикатор набора текста."""
        ...

    def send_media(
        self,
        chat_id: int | str,
        attachments: list[dict],
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int | str] = None,
    ) -> None:
        """Отправить медиа (фото/видео/документы). attachments — список в общем формате."""
        ...

    def download_file(self, file_id_or_url: str) -> bytes:
        """Скачать файл по file_id (Telegram) или url (MAX). Возвращает байты."""
        ...
