"""Тесты сборки media_json v2 из URL (app/services/broadcast_media.py)."""

import json

import pytest

from app.services import broadcast_media
from app.services.broadcast_media import build_media_json_from_urls
from app.services.broadcast_service import parse_broadcast_media_for_platform


@pytest.mark.asyncio
async def test_telegram_only_uses_url_as_file_id():
    raw = await build_media_json_from_urls(
        [{"type": "photo", "url": "https://cdn.example/p.jpg"}],
        ["telegram"],
    )
    data = json.loads(raw)
    assert data["version"] == 2
    assert data["max"] == []
    tg = parse_broadcast_media_for_platform(raw, "telegram")
    assert tg[0]["type"] == "photo"
    assert tg[0]["file_id"] == "https://cdn.example/p.jpg"


@pytest.mark.asyncio
async def test_type_normalization_image_and_file():
    raw = await build_media_json_from_urls(
        [
            {"type": "image", "url": "https://cdn.example/a.png"},
            {"type": "file", "url": "https://cdn.example/b.pdf"},
        ],
        ["telegram"],
    )
    tg = parse_broadcast_media_for_platform(raw, "telegram")
    assert [a["type"] for a in tg] == ["photo", "document"]


@pytest.mark.asyncio
async def test_max_path_downloads_and_uploads(monkeypatch):
    async def fake_download(url):
        return b"bytes", "image/png"

    class FakeMaxAdapter:
        async def upload_document_bytes(self, data, filename, mime_type):
            assert data == b"bytes"
            return "max-file-1"

    monkeypatch.setattr(broadcast_media, "_download", fake_download)

    raw = await build_media_json_from_urls(
        [{"type": "photo", "url": "https://cdn.example/p.png", "filename": "p.png"}],
        ["telegram", "max"],
        max_adapter=FakeMaxAdapter(),
    )
    tg = parse_broadcast_media_for_platform(raw, "telegram")
    mx = parse_broadcast_media_for_platform(raw, "max")
    assert tg[0]["file_id"] == "https://cdn.example/p.png"
    assert mx[0]["type"] == "image"
    assert mx[0]["id_or_url"] == "max-file-1"


@pytest.mark.asyncio
async def test_max_skips_when_no_adapter():
    raw = await build_media_json_from_urls(
        [{"type": "photo", "url": "https://cdn.example/p.png"}],
        ["max"],
        max_adapter=None,
    )
    data = json.loads(raw)
    assert data["max"] == []
