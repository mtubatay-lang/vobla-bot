"""Сервис транскрипции голосовых сообщений (OGG/Opus → текст через OpenAI Whisper)."""

import logging
from io import BytesIO

from pydub import AudioSegment

from app.config import WHISPER_MODEL
from app.services.openai_client import client

logger = logging.getLogger(__name__)


def _ogg_to_mp3_bytes(ogg_bytes: bytes) -> bytes:
    """Конвертирует OGG/Opus в MP3 в памяти. Требует ffmpeg."""
    audio = AudioSegment.from_ogg(BytesIO(ogg_bytes))
    buffer = BytesIO()
    audio.export(buffer, format="mp3")
    buffer.seek(0)
    return buffer.read()


def transcribe_voice(voice_bytes: bytes, mime_type: str = "audio/ogg") -> str:
    """
    Транскрибирует голосовое сообщение (Telegram OGG/Opus) в текст через OpenAI Whisper.

    Args:
        voice_bytes: сырые байты аудио (OGG от Telegram).
        mime_type: MIME-тип (по умолчанию audio/ogg).

    Returns:
        Распознанный текст.

    Raises:
        Exception: при ошибке конвертации или вызова API.
    """
    if not voice_bytes:
        raise ValueError("voice_bytes пустой")

    # Whisper API не поддерживает OGG — конвертируем в MP3
    mp3_bytes = _ogg_to_mp3_bytes(voice_bytes)
    file_like = BytesIO(mp3_bytes)
    file_like.name = "audio.mp3"

    resp = client.audio.transcriptions.create(
        model=WHISPER_MODEL,
        file=file_like,
        response_format="text",
        language="ru",
    )
    # response_format="text" returns str
    return (resp if isinstance(resp, str) else getattr(resp, "text", "") or "").strip()
