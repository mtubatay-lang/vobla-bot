"""Обработчик команды /start."""
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start."""
    await message.answer(
        f"Привет, {message.from_user.first_name}! 👋\n\n"
        "Я бот на aiogram 3. Отправь мне любое сообщение, и я отвечу!"
    )

