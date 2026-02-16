"""Обработчик голосовых сообщений: преобразование в текст через Whisper."""

import asyncio
import logging
from io import BytesIO

from aiogram import Router, F
from aiogram.types import Message

from app.config import VOICE_TO_TEXT_ENABLED, WHISPER_MAX_FILE_BYTES
from app.services.voice_transcription_service import transcribe_voice

logger = logging.getLogger(__name__)

router = Router(name="voice_to_text")

MSG_CONVERTING = "Преобразую в текст."
MSG_ERROR = "Не удалось преобразовать голос в текст. Попробуйте ещё раз."
MSG_TOO_LARGE = "Голосовое сообщение слишком длинное (лимит 25 МБ)."


@router.message(F.voice)
async def on_voice(message: Message) -> None:
    """Скачивает голосовое, конвертирует OGG→MP3, транскрибирует через Whisper, отправляет текст в чат."""
    if not VOICE_TO_TEXT_ENABLED:
        return

    chat_id = message.chat.id
    voice = message.voice
    file_size = getattr(voice, "file_size", None) or 0
    if file_size > WHISPER_MAX_FILE_BYTES:
        await message.answer(MSG_TOO_LARGE)
        return

    await message.answer(MSG_CONVERTING)

    try:
        buffer = BytesIO()
        await message.bot.download(voice.file_id, buffer)
        buffer.seek(0)
        voice_bytes = buffer.read()
    except Exception as e:
        logger.exception("[VOICE_TO_TEXT] Ошибка скачивания голосового: %s", e)
        await message.answer(MSG_ERROR)
        return

    try:
        text = await asyncio.to_thread(transcribe_voice, voice_bytes, "audio/ogg")
    except Exception as e:
        logger.exception("[VOICE_TO_TEXT] Ошибка транскрипции: %s", e)
        await message.answer(MSG_ERROR)
        return

    if not text:
        await message.answer("Текст не распознан.")
        return

    await message.answer(text)
