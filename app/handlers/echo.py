"""Простой echo-хендлер."""

from aiogram import Router, F
from aiogram.types import Message

router = Router()


@router.message(F.text)
async def echo(message: Message):
    """Отправляет пользователю его же текст."""
    await message.answer(message.text)