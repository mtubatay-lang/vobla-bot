"""Хендлеры для работы с частыми вопросами франчайзи (/faq)."""

import asyncio
from typing import Set

from aiogram import Router, F
from aiogram.enums import ChatAction
from aiogram.filters import Command
from aiogram.types import Message

from app.config import MANAGER_CHAT_ID
from app.services.faq_service import find_similar_question
from app.services.openai_client import adapt_faq_answer
from app.services.auth_service import find_user_by_telegram_id
from app.services.metrics_service import log_event

router = Router()

# Пользователи, от которых мы ждём вопрос после /faq
PENDING_FAQ_USERS: Set[int] = set()


@router.message(Command("faq"))
async def cmd_faq(message: Message) -> None:
    """Команда /faq — включает режим ожидания вопроса для авторизованных пользователей."""
    user_id = message.from_user.id

    # 1. Проверяем авторизацию
    user = find_user_by_telegram_id(user_id)
    if not user:
        await message.answer(
            "🔐 Доступ к базе FAQ только для авторизованных пользователей.\n\n"
            "Если у вас есть код доступа, отправьте команду /login "
            "и введите выданный вам код."
        )
        return

    # 2. Включаем режим ожидания вопроса
    PENDING_FAQ_USERS.add(user_id)

    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="faq_mode_enter",
    )

    await message.answer(
        "✉️ Напишите, пожалуйста, ваш вопрос по работе Воблабир.\n"
        "Я попробую найти ответ в базе часто задаваемых вопросов."
    )


@router.message(F.text)
async def handle_faq_question(message: Message) -> None:
    """Обработка текста как потенциального вопроса франчайзи.

    Срабатывает только если пользователь до этого вызвал /faq.
    """
    user_id = message.from_user.id

    # Если пользователь не в режиме FAQ — даём обработать другим хендлерам
    if user_id not in PENDING_FAQ_USERS:
        return

    # Выходим из режима FAQ для этого пользователя
    PENDING_FAQ_USERS.discard(user_id)

    user_question = message.text.strip()
    if not user_question:
        await message.answer("Я не увидел вопроса. Напишите, пожалуйста, текстом 🙏")
        return

    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="faq_question_submitted",
        meta={"text": user_question},
    )

    await message.answer("🔎 Ищу ответ в базе часто задаваемых вопросов...")

    # Анимация печати перед поиском
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    # Ищем похожий вопрос
    match = await find_similar_question(user_question)

    if match is not None:
        log_event(
            user_id=user_id,
            username=message.from_user.username,
            event="faq_answer_found",
            meta={"matched_question": match.get("question", "")},
        )
        base_answer = match["answer"]

        # Анимация печати перед адаптацией ответа
        await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

        # Адаптация ответа через ChatGPT (сносим в поток, чтобы не блокировать event-loop)
        adapted_text = await asyncio.to_thread(
            adapt_faq_answer,
            user_question,
            base_answer,
        )
        await message.answer(adapted_text)
        return

    # Если похожего вопроса не нашли
    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="faq_answer_not_found",
        meta={"text": user_question},
    )

    await message.answer(
        "Пока у меня нет готового ответа на этот вопрос. "
        "Скоро менеджер свяжется с вами 🙏"
    )

    # Отправляем уведомление менеджерам, если указан MANAGER_CHAT_ID
    if MANAGER_CHAT_ID != 0:
        username = message.from_user.username
        full_name = message.from_user.full_name

        manager_text = (
            "❓ <b>Новый вопрос от франчайзи</b>\n\n"
            f"👤 Пользователь: {full_name}"
        )
        if username:
            manager_text += f" (@{username})"
        manager_text += f"\n🆔 User ID: <code>{user_id}</code>\n\n"
        manager_text += f"Вопрос:\n{user_question}"

        await message.bot.send_message(
            chat_id=MANAGER_CHAT_ID,
            text=manager_text,
        )