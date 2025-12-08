"""Обработчики команд /start, /help, /status."""

import time
import platform

import aiogram
from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command, CommandStart

from app.config import LOG_LEVEL

# Создаем роутер
router = Router()

# Фиксируем время запуска — для вычисления uptime
start_time = time.time()


@router.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start."""
    text = (
        f"Привет, {message.from_user.first_name}! 👋\n\n"
        "Я корпоративный бот Воблабир, работаю 24/7 на Railway.\n\n"
        "Доступные команды:\n"
        "/start — приветствие\n"
        "/help — список команд\n"
        "/status — информация о состоянии бота"
    )
    await message.answer(text)


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help."""
    text = (
        "Команды:\n"
        "/start — приветствие\n"
        "/help — список команд\n"
        "/status — состояние бота"
    )
    await message.answer(text)


@router.message(Command("status"))
async def cmd_status(message: Message):
    """Обработчик команды /status — показывает текущее состояние бота."""
    uptime = time.time() - start_time
    hours = int(uptime // 3600)
    minutes = int((uptime % 3600) // 60)

    text = (
        "📊 <b>Статус бота</b>\n\n"
        "🟢 Бот работает нормально\n"
        f"⏱ <b>Uptime:</b> {hours} ч {minutes} мин\n"
        f"🐍 <b>Python:</b> {platform.python_version()}\n"
        f"🤖 <b>Aiogram:</b> {aiogram.__version__}\n"
        f"⚙️ <b>LOG_LEVEL:</b> {LOG_LEVEL}"
    )

    await message.answer(text)