"""Хендлеры для вопросов по базе знаний kilbil (/kilbil)."""

from typing import Set

from aiogram import Router, F
from aiogram.enums import ChatAction
from aiogram.filters import Command, BaseFilter
from aiogram.types import Message

from app.services.kilbil_service import find_kilbil_answer
from app.services.auth_service import find_user_by_telegram_id
from app.services.metrics_service import log_event

router = Router()

PENDING_KILBIL_USERS: Set[int] = set()


class PendingKilbilFilter(BaseFilter):
    """Фильтр: сообщение обрабатывается только если пользователь в режиме /kilbil."""

    async def __call__(self, message: Message) -> bool:
        return message.from_user is not None and message.from_user.id in PENDING_KILBIL_USERS


@router.message(Command("kilbil"))
async def cmd_kilbil(message: Message) -> None:
    """Команда /kilbil — режим вопросов по платформе kilbil."""
    user_id = message.from_user.id

    user = find_user_by_telegram_id(user_id)
    if not user:
        await message.answer(
            "🔐 Доступ к базе знаний kilbil только для авторизованных пользователей.\n\n"
            "Нажмите /start → «Авторизация» или отправьте /login и введите код."
        )
        return

    PENDING_KILBIL_USERS.add(user_id)
    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="kilbil_mode_enter",
    )
    await message.answer(
        "✉️ Напишите ваш вопрос по работе с платформой kilbil.\n"
        "Я поищу ответ в базе знаний help.kilbil.ru"
    )


@router.message(F.text, PendingKilbilFilter())
async def handle_kilbil_question(message: Message) -> None:
    """Обработка текста как вопроса по kilbil (только в режиме /kilbil)."""
    user_id = message.from_user.id
    PENDING_KILBIL_USERS.discard(user_id)
    user_question = message.text.strip()
    if not user_question:
        await message.answer("Напишите вопрос текстом 🙏")
        return

    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="kilbil_question_submitted",
        meta={"text": user_question[:200]},
    )

    await message.answer("🔎 Ищу ответ в базе знаний kilbil...")
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    match = await find_kilbil_answer(user_question)

    if match:
        log_event(
            user_id=user_id,
            username=message.from_user.username,
            event="kilbil_answer_found",
            meta={"title": match.get("title", "")[:50]},
        )
        text = match["answer"]
        if match.get("url"):
            text += f"\n\n📎 Подробнее: {match['url']}"
        await message.answer(text)
    else:
        log_event(
            user_id=user_id,
            username=message.from_user.username,
            event="kilbil_answer_not_found",
            meta={"text": user_question[:200]},
        )
        await message.answer(
            "К сожалению, я не нашёл ответ на этот вопрос в базе знаний kilbil.\n"
            "Попробуйте переформулировать или посмотрите разделы на https://help.kilbil.ru"
        )
