"""Тесты выбора получателей рассылки для platform=max."""

from unittest.mock import patch

from app.services.broadcast_service import (
    get_broadcast_recipients_list,
    read_chats_by_regions,
)


def test_read_chats_by_regions_filters_platform(monkeypatch):
    """Без мока листа — проверяем, что сигнатура принимает target_platform."""
    with patch("app.services.broadcast_service.STATS_SHEET_ID", ""):
        assert read_chats_by_regions(["X"], target_platform="max") == []


def test_get_broadcast_selected_regions_max(monkeypatch):
    def fake_read(regions, target_platform="telegram"):
        assert target_platform == "max"
        assert regions == ["Р1"]
        return [-101, -102]

    monkeypatch.setattr(
        "app.services.broadcast_service.read_chats_by_regions",
        fake_read,
    )
    lst = get_broadcast_recipients_list(
        "selected_regions",
        {"selected_regions": ["Р1"], "audience_platform": "max"},
        platform_filter="max",
    )
    assert len(lst) == 2
    assert all(r.platform == "max" and r.is_chat for r in lst)
    assert {r.chat_or_user_id for r in lst} == {-101, -102}


def test_get_broadcast_selected_chats_max(monkeypatch):
    lst = get_broadcast_recipients_list(
        "selected_chats",
        {"selected_chat_ids": [9001, 9002], "audience_platform": "max"},
        platform_filter="max",
    )
    assert len(lst) == 2
    assert all(r.platform == "max" for r in lst)
    ids = sorted(r.chat_or_user_id for r in lst)
    assert ids == [9001, 9002]


def test_get_broadcast_selected_regions_telegram_default(monkeypatch):
    def fake_read(regions, target_platform="telegram"):
        assert target_platform == "telegram"
        return [11]

    monkeypatch.setattr(
        "app.services.broadcast_service.read_chats_by_regions",
        fake_read,
    )
    lst = get_broadcast_recipients_list(
        "selected_regions",
        {"selected_regions": ["Москва"]},
    )
    assert len(lst) == 1
    assert lst[0].platform == "telegram"
    assert lst[0].chat_or_user_id == 11
