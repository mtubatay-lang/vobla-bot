"""Сервис транскрипции голосовых сообщений (OGG/Opus → текст через OpenAI Whisper)."""

import logging
from io import BytesIO

from pydub import AudioSegment

from app.config import WHISPER_MODEL
from app.services.openai_client import client, CHAT_MODEL

logger = logging.getLogger(__name__)

# Промпт для структурирования сырой расшифровки: очистка + структура без лишних блоков
STRUCTURE_TRANSCRIPT_SYSTEM = """Ты — редактор расшифровок голосовых сообщений. Твоя задача: почистить сырой текст и сделать его читаемым и структурированным. Смысл не меняй.

Вход: текст расшифровки (слова-паразиты, повторы, "ээ", "ну", "короче", оговорки, ошибки распознавания).

Сделай:
1) Очистка: убери слова-паразиты, междометия, лишние повторы и самопоправки без смысла. Исправь очевидные ошибки распознавания. Факты, цифры, имена, термины сохраняй.
2) Структура: разбей по смыслу на абзацы; если есть перечисления — списком; если короткий текст — можно одним абзацем.

Важно:
- Не придумывай факты. Не добавляй разделы "задачи", "вопросы", "цифры и даты", если в тексте их нет.
- Пиши только то, что есть в расшифровке. Нет задач — не пиши блок про задачи. Нет вопросов — не пиши про вопросы. Нет цифр/дат — не пиши этот блок.
- Сохраняй язык оригинала. Будь лаконичен: один короткий абзац остаётся одним коротким абзацем, не раздувай."""


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


def structure_transcript(raw_text: str) -> str:
    """
    Превращает сырую расшифровку голосового в структурированный документ
    (резюме, блоки, задачи, чеклист, цифры/даты). Не меняет смысл.
    """
    if not raw_text or not raw_text.strip():
        return raw_text
    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": STRUCTURE_TRANSCRIPT_SYSTEM},
            {"role": "user", "content": f"Расшифровка голосового сообщения:\n\n{raw_text}"},
        ],
        temperature=0.2,
    )
    out = (resp.choices[0].message.content or "").strip()
    return out if out else raw_text
