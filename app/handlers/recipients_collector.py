"""Хендлер для сбора получателей рассылок (не отвечает пользователю, только собирает данные)."""

import asyncio
import logging

from aiogram import Router
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.types import ChatMemberUpdated, Message

from app.services.broadcast_recipients_service import upsert_user_recipient, upsert_chat_recipient

logger = logging.getLogger(__name__)

router = Router()


@router.my_chat_member()
async def on_bot_added_to_chat(event: ChatMemberUpdated) -> None:
    """
    Когда бота добавляют в группу/супергруппу — сразу добавляем чат в recipients_chats,
    не дожидаясь первого сообщения.
    """
    if event.chat.type not in ("group", "supergroup"):
        return
    new = event.new_chat_member
    if new.status not in ("member", "administrator"):
        return
    try:
        await asyncio.to_thread(
            upsert_chat_recipient,
            chat_id=event.chat.id,
            chat_type=event.chat.type,
            title=event.chat.title or "",
            username=event.chat.username,
        )
    except Exception as e:
        logger.debug(f"[RECIPIENTS_COLLECTOR] Error adding chat on my_chat_member: {e}")


@router.message()
async def collect_recipient(message: Message) -> None:
    """
    Собирает данные о получателях для будущих рассылок.
    Не отвечает пользователю, только логирует в Google Sheets.
    Явно пропускает обработку дальше через SkipHandler.
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
    
    # Явно пропускаем обработку дальше другим хендлерам
    raise SkipHandler()

