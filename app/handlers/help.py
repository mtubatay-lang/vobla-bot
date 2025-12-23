"""Команда /help — справка по боту."""

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from app.services.auth_service import find_user_by_telegram_id
from app.ui.keyboards import main_menu_kb

router = Router()


def _help_text_authorized() -> str:
    return (
        "📌 <b>Как пользоваться ботом</b>\n\n"
        "❓ <b>Задать вопрос</b>\n"
        "• Нажми кнопку «❓ Задать вопрос» в меню\n"
        "• или введи команду /ask\n\n"
        "🧠 <b>Навык «Ответы на вопросы»</b>\n"
        "• Можно задавать вопросы подряд\n"
        "• Для выхода нажми «✅ Завершить навык»\n\n"
        "🔐 <b>Авторизация</b>\n"
        "• /login — если нужно войти заново\n"
    )


def _help_text_unauthorized() -> str:
    return (
        "🔒 Этот бот доступен только для партнёров Воблабир.\n\n"
        "Чтобы начать:\n"
        "1) Нажми /start\n"
        "2) Пройди авторизацию по коду доступа\n\n"
        "Если кода нет — обратись к менеджеру."
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    tg_id = message.from_user.id if message.from_user else 0
    user = find_user_by_telegram_id(tg_id)

    if user:
        await message.answer(_help_text_authorized(), reply_markup=main_menu_kb(), parse_mode="HTML")
    else:
        await message.answer(_help_text_unauthorized())

