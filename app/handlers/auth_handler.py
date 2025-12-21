"""Хендлеры авторизации пользователей (/login)."""

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from app.services.auth_service import (
    find_user_by_code,
    bind_telegram_id,
    find_user_by_telegram_id,
)
from app.services.metrics_service import log_event
from app.ui.keyboards import main_menu_kb

auth_router = Router()


# Состояния FSM
class AuthState(StatesGroup):
    waiting_for_code = State()


def _commands_menu_text() -> str:
    """Текст с меню команд после авторизации."""
    return (
        "📋 <b>Доступные команды:</b>\n"
        "• /help — подсказка по всем командам бота\n"
        "• /ask — задать вопрос (режим навыка)\n\n"
        "Или просто нажмите кнопку «❓ Задать вопрос» ниже 👇"
    )


# 🔹 Команда /login — начать авторизацию вручную
@auth_router.message(Command("login"))
async def login_start(message: Message, state: FSMContext) -> None:
    tg_id = message.from_user.id

    # Если пользователь уже авторизован — просто показываем меню
    user = find_user_by_telegram_id(tg_id)
    if user:
        await message.answer(
            f"✅ Вы уже авторизованы.\n"
            f"👤 Имя: <b>{user.name}</b>\n"
            f"🎯 Роль: <b>{user.role}</b>\n\n"
            + _commands_menu_text(),
            reply_markup=main_menu_kb(),
        )
        return

    # Иначе запускаем процесс ввода кода
    await state.set_state(AuthState.waiting_for_code)
    await message.answer(
        "🔐 Этот бот доступен только для партнёров Воблабир.\n\n"
        "Введите, пожалуйста, ваш одноразовый код доступа, "
        "который выдали вам менеджеры."
    )

    log_event(
        user_id=message.from_user.id,
        username=message.from_user.username,
        event="login_command",
    )


# 🔹 Обработка ввода кода
@auth_router.message(AuthState.waiting_for_code)
async def process_code(message: Message, state: FSMContext) -> None:
    code = message.text.strip()

    log_event(
        user_id=message.from_user.id,
        username=message.from_user.username,
        event="auth_code_submitted",
    )

    if not code:
        await message.answer("Я не увидел кода. Введите, пожалуйста, текстом 🙏")
        return

    user = find_user_by_code(code)

    if not user:
        log_event(
            user_id=message.from_user.id,
            username=message.from_user.username,
            event="auth_failed_code_not_found",
        )
        await message.answer("❌ Неверный код доступа. Попробуйте ещё раз.")
        return

    # Привязываем Telegram ID
    bind_telegram_id(user, message.from_user.id)

    log_event(
        user_id=message.from_user.id,
        username=message.from_user.username,
        event="auth_success",
        meta={"role": getattr(user, "role", ""), "name": getattr(user, "name", "")},
    )

    # Чистим состояние
    await state.clear()

    # Приветствие + меню команд
    await message.answer(
        f"✅ Доступ подтверждён!\n"
        f"Добро пожаловать, <b>{user.name}</b> 👋\n"
        f"Ваша роль: <b>{user.role}</b>.\n\n"
        + _commands_menu_text(),
        reply_markup=main_menu_kb(),
    )