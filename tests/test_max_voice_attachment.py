"""Выбор голосового вложения MAX и MIME для транскрипции."""

from app.core.types import IncomingMessage, InternalChat, InternalUser
from app.platforms.max.voice_attachment import (
    first_max_voice_attachment,
    max_voice_attachment_declared_size,
    max_voice_effective_mime,
    mime_from_max_voice_attachment,
)


def _msg(attachments: list) -> IncomingMessage:
    return IncomingMessage(
        user=InternalUser(id=1, platform="max"),
        chat=InternalChat(id=1, platform="max"),
        text="",
        attachments=attachments,
    )


def test_first_max_voice_attachment_prefers_voice():
    ev = _msg(
        [
            {"type": "file", "file_id": "f1", "id_or_url": "f1"},
            {"type": "voice", "file_id": "v1", "id_or_url": "v1"},
        ]
    )
    att = first_max_voice_attachment(ev)
    assert att is not None
    assert att.get("file_id") == "v1"


def test_first_max_voice_attachment_audio():
    ev = _msg([{"type": "audio", "file_id": "a1", "id_or_url": "a1"}])
    assert first_max_voice_attachment(ev) is not None


def test_first_max_voice_attachment_skips_file_only():
    ev = _msg([{"type": "file", "file_id": "f1", "id_or_url": "f1"}])
    assert first_max_voice_attachment(ev) is None


def test_mime_from_payload():
    att = {"type": "audio", "payload": {"mime_type": "audio/mpeg"}}
    assert mime_from_max_voice_attachment(att) == "audio/mpeg"


def test_declared_size_from_payload():
    att = {"type": "voice", "payload": {"file_size": 12345}}
    assert max_voice_attachment_declared_size(att) == 12345


def test_max_voice_effective_mime_prefers_download_content_type():
    att = {"type": "voice", "file_id": "1"}
    assert max_voice_effective_mime(att, "audio/mp4; charset=utf-8") == "audio/mp4"


def test_max_voice_effective_mime_falls_back_on_octet_stream():
    att = {"type": "voice", "payload": {"mime_type": "audio/ogg"}}
    assert max_voice_effective_mime(att, "application/octet-stream") == "audio/ogg"
