"""Обработчики команды /start и авторизации через inline-кнопку."""

import asyncio
from typing import Dict

from aiogram import Router, F
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.enums import ChatAction

from app.services.auth_service import (
    find_user_by_telegram_id,
    find_user_by_code,
    bind_telegram_id,
)
from app.handlers.auth_handler import _commands_menu_text  # общее меню команд
from app.services.metrics_service import log_event

router = Router()

# Пользователи, от которых ждём ввод кода доступа
pending_auth: Dict[int, bool] = {}


def build_auth_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой авторизации."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔐 Авторизация",
                    callback_data="start_auth",
                )
            ]
        ]
    )


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Старт: проверяем авторизацию и показываем кнопку."""
    tg_id = message.from_user.id

    # 1. Уже авторизованный пользователь
    user = find_user_by_telegram_id(tg_id)
    if user:
        log_event(
            user_id=tg_id,
            username=message.from_user.username,
            event="start_authorized",
            meta={"role": getattr(user, "role", "")},
        )
        await message.answer(
            f"👋 Привет, {user.name}!\n"
            f"Вы авторизованы как <b>{user.role}</b>.\n\n"
            + _commands_menu_text()
        )
        return

    # 2. Новый / неавторизованный пользователь
    log_event(
        user_id=tg_id,
        username=message.from_user.username,
        event="start_unauthorized",
    )
    text = (
        "🔒 Этот бот доступен только для партнёров Воблабир.\n\n"
        "Если у вас есть код доступа, нажмите кнопку ниже, "
        "чтобы пройти авторизацию."
    )

    await message.answer(text, reply_markup=build_auth_keyboard())


@router.callback_query(F.data == "start_auth")
async def on_start_auth(callback: CallbackQuery) -> None:
    """Нажатие на кнопку «Авторизация» под /start."""
    tg_id = callback.from_user.id

    pending_auth[tg_id] = True

    log_event(
        user_id=tg_id,
        username=callback.from_user.username,
        event="auth_button_click",
    )

    await callback.message.answer(
        "🔐 Для входа в систему введите код доступа, выданный менеджером.\n"
        "Код можно использовать только один раз."
    )
    # закрываем «крутилку» на кнопке
    await callback.answer()


@router.message(F.text)
async def process_auth_code(message: Message) -> None:
    """Обработка текста как кода авторизации (если мы его ждём)."""
    tg_id = message.from_user.id
    text = message.text.strip()

    # Если бот НЕ ждёт код — передаём обработку дальше другим хендлерам
    if tg_id not in pending_auth:
        raise SkipHandler()

    # Анимация печати перед обработкой
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)
    await asyncio.sleep(1.2)

    log_event(
        user_id=tg_id,
        username=message.from_user.username,
        event="auth_code_submitted",
    )

    # Ищем пользователя по коду
    user = find_user_by_code(text)

    if not user:
        log_event(
            user_id=tg_id,
            username=message.from_user.username,
            event="auth_failed_code_not_found",
        )
        await message.answer(
            "❌ Код не найден. Проверьте правильность и попробуйте снова."
        )
        return

    # Проверяем статус
    if not user.is_active:
        log_event(
            user_id=tg_id,
            username=message.from_user.username,
            event="auth_failed_inactive",
        )
        await message.answer(
            "⛔ Ваш код не активирован. Обратитесь к менеджеру."
        )
        return

    # Привязываем Telegram ID + фиксируем дату
    bind_telegram_id(user, tg_id)

    log_event(
        user_id=tg_id,
        username=message.from_user.username,
        event="auth_success",
        meta={"role": getattr(user, "role", ""), "name": getattr(user, "name", "")},
    )

    # Удаляем из ожидания
    pending_auth.pop(tg_id, None)

    await message.answer(
        f"✅ Добро пожаловать, {user.name}!\n"
        f"Вы авторизованы как <b>{user.role}</b>.\n\n"
        + _commands_menu_text()
    )