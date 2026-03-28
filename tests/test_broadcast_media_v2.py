"""Тесты media_json v2 и parse_broadcast_media_for_platform."""

import json

from app.services.broadcast_service import parse_broadcast_media_for_platform


def test_v1_list_is_telegram_only():
    raw = json.dumps([{"type": "photo", "file_id": "tg1"}])
    assert len(parse_broadcast_media_for_platform(raw, "telegram")) == 1
    assert parse_broadcast_media_for_platform(raw, "max") == []


def test_v2_split_platforms():
    raw = json.dumps(
        {
            "version": 2,
            "telegram": [{"type": "photo", "file_id": "a"}],
            "max": [
                {
                    "type": "image",
                    "file_id": "m1",
                    "id_or_url": "m1",
                    "max_payload": {"token": "m1"},
                }
            ],
        }
    )
    tg = parse_broadcast_media_for_platform(raw, "telegram")
    mx = parse_broadcast_media_for_platform(raw, "max")
    assert tg[0]["file_id"] == "a"
    assert mx[0]["id_or_url"] == "m1"
    assert mx[0]["type"] == "image"
    assert mx[0]["max_payload"] == {"token": "m1"}


def test_v2_max_video_with_legacy_file_id_payload_preserved_for_adapter():
    """Как в media_json из рассылки: type video + max_payload с file_id — send_media нормализует в token."""
    raw = json.dumps(
        {
            "version": 2,
            "telegram": [],
            "max": [
                {
                    "type": "video",
                    "file_id": "vid-fid",
                    "id_or_url": "vid-fid",
                    "max_payload": {"file_id": "vid-fid"},
                }
            ],
        }
    )
    mx = parse_broadcast_media_for_platform(raw, "max")
    assert len(mx) == 1
    assert mx[0]["type"] == "video"
    assert mx[0]["max_payload"] == {"file_id": "vid-fid"}


def test_v2_max_preserves_filename_for_spreadsheet_coerce():
    raw = json.dumps(
        {
            "version": 2,
            "telegram": [],
            "max": [
                {
                    "type": "video",
                    "file_id": "x1",
                    "id_or_url": "x1",
                    "filename": "Тестовая.xlsx",
                }
            ],
        }
    )
    mx = parse_broadcast_media_for_platform(raw, "max")
    assert mx[0]["filename"] == "Тестовая.xlsx"


def test_invalid_json_empty():
    assert parse_broadcast_media_for_platform("not json", "telegram") == []
    assert parse_broadcast_media_for_platform("", "max") == []
