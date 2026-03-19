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


def test_parse_dm_recipient_is_bot_uses_sender_as_peer():
    """В личке recipient — бот; отвечать нужно на user_id отправителя, иначе POST /messages → 403."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 999888, "name": "Human"},
            "recipient": {"type": "user", "user_id": 111},  # часто id бота
            "body": {"text": "K4mP7qR2"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.user.id == 999888
    assert ev.chat.id == 999888
    assert ev.chat.is_group is False


def test_parse_callback_peer_is_sender_in_dm():
    body = {
        "update_type": "message_callback",
        "callback": {
            "callback_id": "cb-1",
            "payload": "start_auth",
            "user": {"user_id": 42, "name": "U"},
            "message": {
                "id": "m1",
                "sender": {"user_id": 42},
                "recipient": {"type": "user", "user_id": 0},
            },
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.id == 42
    assert ev.chat.is_group is False
