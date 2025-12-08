"""Обработчик эхо-сообщений."""
from aiogram import Router
from aiogram.types import Message

router = Router()


@router.message()
async def echo_handler(message: Message):
    """Обработчик всех сообщений (эхо)."""
    await message.answer(f"Вы написали: {message.text}")

