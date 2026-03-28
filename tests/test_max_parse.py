"""Тесты разбора webhook MAX (parse_max_update)."""

from app.platforms.max.client import MaxApiClientError

from app.platforms.max.adapter import (
    _attachment_dicts_to_url_pairs,
    _finalize_max_token_payload,
    _find_attachment_download_url_in_api_message,
    _is_max_missing_video_token_error,
    _max_payload_photos_non_empty,
    _normalize_max_send_payload,
    coerce_spreadsheet_max_send_type,
    max_attachment_filename_from_raw,
    max_attachment_mime_from_raw,
    parse_max_update,
    spreadsheet_mime_indicates_table,
)


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


def test_parse_group_from_message_chat_root_only():
    """Иногда peer только в message.chat (без message.recipient)."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 42, "name": "U"},
            "chat": {
                "chat_id": 555444333,
                "type": "chat",
                "title": "Root chat only",
            },
            "body": {"text": "x"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.is_group is True
    assert ev.chat.id == 555444333


def test_parse_group_title_from_update_level_chat():
    """Название может быть только в update.chat, а в recipient — один chat_id."""
    body = {
        "update_type": "message_created",
        "chat": {
            "chat_id": 111222333,
            "type": "chat",
            "title": "Title only on update.chat",
        },
        "message": {
            "sender": {"user_id": 42, "name": "U"},
            "recipient": {"chat_id": 111222333},
            "body": {"text": "x"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.is_group is True
    assert ev.chat.title == "Title only on update.chat"


def test_parse_group_flat_recipient_chat_type_with_user_id():
    """Плоский recipient: chat_id + chat_type=chat + user_id (например бот) — всё равно группа."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 100, "name": "U"},
            "recipient": {
                "chat_id": -71933414354218,
                "chat_type": "chat",
                "user_id": 999001,
            },
            "body": {"text": "x"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.is_group is True
    assert ev.chat.id == -71933414354218


def test_parse_dialog_flat_recipient_chat_type_dialog():
    """Личка: chat_type dialog — не группа."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 173247757, "name": "Admin"},
            "recipient": {
                "chat_id": 173247757,
                "chat_type": "dialog",
                "user_id": 111,
            },
            "body": {"text": "hi"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.is_group is False
    assert ev.chat.id == 173247757


def test_parse_group_recipient_top_level_chat_id_only():
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 42, "name": "U"},
            "recipient": {"chat_id": 777888999},
            "body": {"text": "x"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.is_group is True
    assert ev.chat.id == 777888999


def test_parse_group_message_nested_chat_recipient():
    """MAX часто шлёт группу как recipient.chat { chat_id, type: chat }, без recipient.type."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 42, "name": "U"},
            "recipient": {
                "chat": {
                    "chat_id": 987654321,
                    "type": "chat",
                    "title": "Команда тест",
                }
            },
            "body": {"text": "hi all"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.is_group is True
    assert ev.chat.id == 987654321
    assert ev.chat.title == "Команда тест"


def test_parse_group_nested_chat_uses_name_when_no_title():
    """Webhook MAX: название группы в поле name, без title."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 42, "name": "U"},
            "recipient": {
                "chat": {
                    "chat_id": 72257432704463,
                    "type": "chat",
                    "name": "Тестовая группа MAX",
                }
            },
            "body": {"text": "hi"},
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.chat.is_group is True
    assert ev.chat.title == "Тестовая группа MAX"


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


def test_parse_message_attachment_payload_file_id():
    """Вложения MAX: file_id часто внутри payload."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 1, "name": "U"},
            "recipient": {"type": "user", "user_id": 1},
            "body": {
                "text": "caption",
                "attachments": [
                    {"type": "image", "payload": {"file_id": "abc-123"}},
                ],
            },
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert len(ev.attachments) == 1
    assert ev.attachments[0]["file_id"] == "abc-123"
    assert ev.attachments[0]["type"] == "image"
    assert ev.attachments[0]["max_payload"] == {"token": "abc-123"}


def test_parse_message_attachment_payload_token():
    """Для исходящего POST /messages картинка может прийти только с token в payload."""
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 1, "name": "U"},
            "recipient": {"type": "user", "user_id": 1},
            "body": {
                "text": "c",
                "attachments": [{"type": "image", "payload": {"token": "tok-max-9"}}],
            },
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.attachments[0]["max_payload"] == {"token": "tok-max-9"}


def test_parse_message_video_attachment_outgoing_token():
    body = {
        "update_type": "message_created",
        "message": {
            "sender": {"user_id": 1, "name": "U"},
            "recipient": {"type": "user", "user_id": 1},
            "body": {
                "attachments": [{"type": "video", "payload": {"file_id": "vid-1"}}],
            },
        },
    }
    ev = parse_max_update(body)
    assert ev is not None
    assert ev.attachments[0]["max_payload"] == {"token": "vid-1"}


def test_normalize_legacy_image_max_payload_file_id_to_token():
    assert _normalize_max_send_payload("image", {"file_id": "legacy-1"}) == {"token": "legacy-1"}
    assert _normalize_max_send_payload("image", {"token": "keep"}) == {"token": "keep"}
    assert _normalize_max_send_payload("file", {"file_id": "f"}) == {"file_id": "f"}


def test_normalize_legacy_video_audio_max_payload_file_id_to_token():
    assert _normalize_max_send_payload("video", {"file_id": "v-1"}) == {"token": "v-1"}
    assert _normalize_max_send_payload("audio", {"file_id": "a-1"}) == {"token": "a-1"}
    assert _normalize_max_send_payload("video", {"token": "keep-v"}) == {"token": "keep-v"}
    assert _normalize_max_send_payload("audio", {"token": "keep-a"}) == {"token": "keep-a"}
    assert _normalize_max_send_payload("video", {"url": "https://x"}) == {"url": "https://x"}


def test_normalize_video_empty_token_still_maps_file_id():
    """Пустой token не должен блокировать перенос file_id (как у xlsx, пришедшего как video)."""
    assert _normalize_max_send_payload("video", {"token": "", "file_id": "abc"}) == {"token": "abc"}
    assert _normalize_max_send_payload("video", {"token": "  ", "file_id": "abc"}) == {"token": "abc"}


def test_finalize_max_token_payload_fills_from_file_id():
    assert _finalize_max_token_payload("video", {}, "fid-9") == {"token": "fid-9"}
    assert _finalize_max_token_payload("video", {"token": ""}, "fid-9") == {"token": "fid-9"}
    assert _finalize_max_token_payload("file", {"file_id": "f"}, "x") == {"file_id": "f"}
    assert _finalize_max_token_payload("video", {"token": "ok"}, "ignored") == {"token": "ok"}


def test_max_payload_photos_non_empty_and_normalize_video():
    assert _max_payload_photos_non_empty({"photos": "[]"}) is False
    assert _max_payload_photos_non_empty({"photos": ""}) is False
    assert _max_payload_photos_non_empty({"photos": "{}"}) is False
    assert _max_payload_photos_non_empty({"photos": "null"}) is False
    assert _max_payload_photos_non_empty({"photos": "[1]"}) is True
    assert _normalize_max_send_payload("video", {"photos": "[]", "file_id": "z"}) == {"token": "z"}
    assert _finalize_max_token_payload("video", {"photos": "[]"}, "fid") == {"token": "fid"}


def test_coerce_spreadsheet_max_send_type():
    assert coerce_spreadsheet_max_send_type("video", "Тестовая.xlsx") == "file"
    assert coerce_spreadsheet_max_send_type("video", "t.CSV") == "file"
    assert coerce_spreadsheet_max_send_type("audio", "a.ods") == "file"
    assert coerce_spreadsheet_max_send_type("video", "clip.mp4") == "video"
    assert coerce_spreadsheet_max_send_type("video", "") == "video"
    assert coerce_spreadsheet_max_send_type("file", "x.xlsx") == "file"
    assert (
        coerce_spreadsheet_max_send_type(
            "video",
            "",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        == "file"
    )
    assert coerce_spreadsheet_max_send_type("video", "", "video/mp4") == "video"


def test_spreadsheet_mime_indicates_table():
    assert spreadsheet_mime_indicates_table("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    assert spreadsheet_mime_indicates_table("text/csv")
    assert not spreadsheet_mime_indicates_table("video/mp4")


def test_max_attachment_mime_from_raw_payload():
    assert (
        max_attachment_mime_from_raw({"payload": {"mime_type": "text/csv; charset=utf-8"}})
        == "text/csv"
    )


def test_is_max_missing_video_token_error_relaxed():
    e1 = MaxApiClientError(
        "x",
        400,
        '{"code":"proto.payload","message":"Missing `token` in video attachment"}',
    )
    assert _is_max_missing_video_token_error(e1)
    e2 = MaxApiClientError("x", 400, '{"message":"problem with video attachment token"}')
    assert _is_max_missing_video_token_error(e2)
    e3 = MaxApiClientError("x", 400, '{"message":"other"}')
    assert not _is_max_missing_video_token_error(e3)


def test_attachment_dicts_to_url_pairs_nested_video():
    att = {"type": "video", "video": {"file_id": "v1", "url": "https://cdn.example/f"}}
    pairs = _attachment_dicts_to_url_pairs(att)
    assert ("v1", "https://cdn.example/f") in pairs


def test_find_attachment_download_url_falls_back_to_msg_attachments():
    data = {
        "message": {
            "attachments": [{"video": {"id": "x", "url": "https://example.com/a"}}],
        }
    }
    assert _find_attachment_download_url_in_api_message(data, file_id="x") == "https://example.com/a"


def test_max_attachment_filename_from_raw_nested():
    assert max_attachment_filename_from_raw({"payload": {"filename": "a.xlsx"}}) == "a.xlsx"
    assert max_attachment_filename_from_raw({"file": {"name": "b.xls"}}) == "b.xls"
    assert max_attachment_filename_from_raw({"payload": {"title": "sheet.xlsx"}}) == "sheet.xlsx"
    assert max_attachment_filename_from_raw({"type": "video"}) == ""
