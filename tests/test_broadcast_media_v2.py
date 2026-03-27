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


def test_invalid_json_empty():
    assert parse_broadcast_media_for_platform("not json", "telegram") == []
    assert parse_broadcast_media_for_platform("", "max") == []
