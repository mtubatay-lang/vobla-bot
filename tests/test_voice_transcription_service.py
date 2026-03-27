"""Транскрипция: устойчивость к неверному MIME (MAX отдаёт не OGG под audio/ogg)."""

import io
import wave
from unittest.mock import patch

from app.services.voice_transcription_service import (
    _sniff_container,
    transcribe_audio_bytes,
)


def _minimal_wav_bytes() -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(8000)
        wf.writeframes(b"\x00\x00" * 400)
    return buf.getvalue()


@patch("app.services.voice_transcription_service.client")
def test_transcribe_wav_sniff_overrides_ogg_mime(mock_client) -> None:
    """Вебхук говорит audio/ogg, тело — RIFF/WAVE: не вызываем from_ogg вслепую."""
    mock_client.audio.transcriptions.create.return_value = "ok"
    out = transcribe_audio_bytes(_minimal_wav_bytes(), "audio/ogg")
    assert out == "ok"
    mock_client.audio.transcriptions.create.assert_called_once()


def test_sniff_container_signatures() -> None:
    wav = _minimal_wav_bytes()
    assert _sniff_container(wav) == "wav"
    assert _sniff_container(b"OggS" + b"\x00" * 20) == "ogg"
    assert _sniff_container(b"\xff\xfb" + b"\x00" * 30) == "mp3"
    ftyp = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00" + b"\x00" * 40
    assert _sniff_container(ftyp) == "mp4"
