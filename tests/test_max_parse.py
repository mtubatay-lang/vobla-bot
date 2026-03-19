"""Тесты разбора webhook MAX (parse_max_update)."""

from app.platforms.max.adapter import parse_max_update


def test_parse_message_created_minimal():
    body = {
        "update_type": "message_created",
        "timestamp": 0,
        "message": {
            "sender": {"user_id": 42, "name": "U", "username": "u1"},
            "recipient": {"type": "user", "user_id": 42},
            "body": {"text": "/start"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.text == "/start"
    assert ev.user.id == 42
    assert ev.chat.is_group is False


def test_parse_bot_started():
    body = {
        "update_type": "bot_started",
        "user": {"user_id": 7, "name": "Bob"},
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.text == "/start"
    assert ev.is_command is True
    assert ev.user.id == 7


def test_parse_message_callback():
    body = {
        "update_type": "message_callback",
        "callback": {
            "callback_id": "cb-123",
            "payload": "start_auth",
            "user": {"user_id": 1, "name": "A"},
            "message": {
                "id": "mid-9",
                "sender": {"user_id": 1},
                "recipient": {"type": "user", "user_id": 1},
            },
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.data == "start_auth"
    assert ev.callback_id == "cb-123"
    assert ev.original_message_id == "mid-9"


def test_parse_legacy_type_field():
    body = {
        "type": "message_created",
        "message": {
            "sender": {"id": 99, "name": "Legacy"},
            "body": {"text": "hi"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.text == "hi"


def test_parse_missing_update_type():
    assert parse_max_update({}) is None
