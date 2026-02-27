"""Telegram-адаптер и маппинг событий."""

from app.platforms.telegram.adapter import (
    TelegramAdapter,
    from_telegram_message,
    from_telegram_callback,
    rows_to_inline_markup,
)

__all__ = [
    "TelegramAdapter",
    "from_telegram_message",
    "from_telegram_callback",
    "rows_to_inline_markup",
]
