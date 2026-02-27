"""Обработчики команды /start и авторизации через inline-кнопку."""

from aiogram import Router, F
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from app.core.handlers import (
    handle_start,
    handle_start_auth_callback,
    handle_auth_code,
    is_pending_auth,
)
from app.platforms.telegram import TelegramAdapter, from_telegram_message, from_telegram_callback

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Старт: проверяем авторизацию и показываем кнопку."""
    adapter = TelegramAdapter(message.bot)
    incoming = from_telegram_message(message)
    await handle_start(adapter, incoming)


@router.callback_query(F.data == "start_auth")
async def on_start_auth(callback: CallbackQuery) -> None:
    """Нажатие на кнопку «Авторизация» под /start."""
    adapter = TelegramAdapter(callback.bot)
    event = from_telegram_callback(callback)
    await handle_start_auth_callback(adapter, event)


@router.message(F.text)
async def process_auth_code(message: Message) -> None:
    """Обработка текста как кода авторизации (если мы его ждём)."""
    tg_id = message.from_user.id
    if not is_pending_auth("telegram", tg_id):
        raise SkipHandler()

    adapter = TelegramAdapter(message.bot)
    incoming = from_telegram_message(message)
    await handle_auth_code(adapter, incoming)