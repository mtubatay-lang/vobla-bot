"""Хендлер для сбора получателей рассылок (не отвечает пользователю, только собирает данные)."""

import asyncio
import logging

from aiogram import Router
from aiogram.types import Message

from app.services.broadcast_recipients_service import upsert_user_recipient, upsert_chat_recipient

logger = logging.getLogger(__name__)

router = Router()


@router.message()
async def collect_recipient(message: Message) -> None:
    """
    Собирает данные о получателях для будущих рассылок.
    Не отвечает пользователю, только логирует в Google Sheets.
    Обработка продолжается для других хендлеров (не блокируем).
    """
    try:
        # Если это приватный чат - собираем данные пользователя
        if message.chat.type == "private":
            user = message.from_user
            if user:
                await asyncio.to_thread(
                    upsert_user_recipient,
                    user_id=user.id,
                    username=user.username,
                    full_name=user.full_name,
                )
        
        # Если это группа/супергруппа/канал - собираем данные чата
        elif message.chat.type in ("group", "supergroup", "channel"):
            await asyncio.to_thread(
                upsert_chat_recipient,
                chat_id=message.chat.id,
                chat_type=message.chat.type,
                title=message.chat.title,
                username=message.chat.username,
            )
    except Exception as e:
        # Тихий лог ошибок, чтобы не ломать основной функционал
        logger.debug(f"[RECIPIENTS_COLLECTOR] Error collecting recipient: {e}")
    
    # Не возвращаем ответ и не вызываем SkipHandler - обработка продолжается автоматически

