"""Сервис транскрипции голосовых сообщений (OGG/Opus → текст через OpenAI Whisper)."""

import logging
from io import BytesIO

from pydub import AudioSegment
from pydub.exceptions import CouldntDecodeError

from app.config import WHISPER_MODEL
from app.services.openai_client import client, CHAT_MODEL

logger = logging.getLogger(__name__)

# Промпт для структурирования сырой расшифровки: очистка + структура без лишних блоков
STRUCTURE_TRANSCRIPT_SYSTEM = """Ты — редактор расшифровок голосовых сообщений. Твоя задача: почистить сырой текст и сделать его читаемым и структурированным. Смысл не меняй.

Вход: текст расшифровки (слова-паразиты, повторы, "ээ", "ну", "короче", оговорки, ошибки распознавания).

Сделай:
1) Очистка: убери слова-паразиты, междометия, лишние повторы и самопоправки без смысла. Исправь очевидные ошибки распознавания. Факты, цифры, имена, термины сохраняй.
2) Структура: дели текст на смысловые абзацы (каждый абзац — одна мысль или тема); если есть перечисления — списком; если короткий текст — можно одним абзацем.

Важно:
- Не придумывай факты. Не добавляй разделы "задачи", "вопросы", "цифры и даты", если в тексте их нет.
- Пиши только то, что есть в расшифровке. Нет задач — не пиши блок про задачи. Нет вопросов — не пиши про вопросы. Нет цифр/дат — не пиши этот блок.
- Сохраняй язык оригинала. Будь лаконичен: один короткий абзац остаётся одним коротким абзацем, не раздувай."""

_OGG_MIMES = frozenset(
    {"audio/ogg", "application/ogg", "audio/opus", "audio/x-opus+ogg"}
)


def _sniff_container(data: bytes) -> str | None:
    """Сигнатура контейнера для pydub/ffmpeg (не доверять только MIME из вебхука MAX)."""
    if len(data) < 12:
        return None
    if data[:4] == b"OggS":
        return "ogg"
    if data[:3] == b"ID3" or (data[0] == 0xFF and (data[1] & 0xE0) == 0xE0):
        return "mp3"
    if data[:4] == b"fLaC":
        return "flac"
    if data[:4] == b"RIFF" and data[8:12] == b"WAVE":
        return "wav"
    if len(data) >= 8 and data[4:8] == b"ftyp":
        return "mp4"
    if data[:4] == b"\x1a\x45\xdf\xa3":
        return "webm"
    return None


def _load_audio_segment(data: bytes, mime_type: str) -> AudioSegment:
    """Декодирует байты в AudioSegment: сначала по сигнатуре, затем по MIME, с fallback ffmpeg."""
    mt = (mime_type or "").strip().lower()
    sniff = _sniff_container(data)

    if sniff == "ogg":
        try:
            return AudioSegment.from_ogg(BytesIO(data))
        except CouldntDecodeError as e:
            logger.warning("transcribe: сигнатура OggS, но from_ogg не удался, пробуем авто: %s", e)
            return AudioSegment.from_file(BytesIO(data))

    if sniff in ("mp3", "mp4", "wav", "flac", "webm"):
        return AudioSegment.from_file(BytesIO(data), format=sniff)

    pydub_format_by_mime: dict[str, str] = {
        "audio/mpeg": "mp3",
        "audio/mp3": "mp3",
        "audio/mp4": "mp4",
        "audio/x-m4a": "mp4",
        "audio/aac": "aac",
        "audio/wav": "wav",
        "audio/x-wav": "wav",
        "audio/webm": "webm",
        "audio/flac": "flac",
    }

    if mt in _OGG_MIMES:
        try:
            return AudioSegment.from_file(BytesIO(data))
        except Exception as e:
            logger.warning("transcribe: MIME ogg/opus без сигнатуры OggS, авто не вышло, mp3: %s", e)
            return AudioSegment.from_file(BytesIO(data), format="mp3")

    fmt = pydub_format_by_mime.get(mt)
    if fmt:
        return AudioSegment.from_file(BytesIO(data), format=fmt)

    try:
        return AudioSegment.from_file(BytesIO(data))
    except Exception as e:
        logger.warning("transcribe: неизвестный MIME %r, пробуем mp3: %s", mime_type, e)
        return AudioSegment.from_file(BytesIO(data), format="mp3")


def _audio_bytes_to_mp3(data: bytes, mime_type: str) -> bytes:
    """Любой поддерживаемый аудиоформат → MP3 для Whisper."""
    audio = _load_audio_segment(data, mime_type)
    buffer = BytesIO()
    audio.export(buffer, format="mp3")
    buffer.seek(0)
    return buffer.read()


def transcribe_audio_bytes(data: bytes, mime_type: str = "audio/ogg") -> str:
    """
    Транскрибирует аудио (Telegram OGG, MAX и др.) в текст через Whisper.

    Args:
        data: сырые байты.
        mime_type: MIME-тип входного файла.

    Raises:
        ValueError: пустые данные.
    """
    if not data:
        raise ValueError("data пустой")
    mp3_bytes = _audio_bytes_to_mp3(data, mime_type)
    file_like = BytesIO(mp3_bytes)
    file_like.name = "audio.mp3"
    resp = client.audio.transcriptions.create(
        model=WHISPER_MODEL,
        file=file_like,
        response_format="text",
        language="ru",
    )
    return (resp if isinstance(resp, str) else getattr(resp, "text", "") or "").strip()


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
    return transcribe_audio_bytes(voice_bytes, mime_type)


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
