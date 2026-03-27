"""Текст шага выбора варианта рассылки MAX (превью улучшенного текста)."""

from app.platforms.max.admin_flows import _max_variant_choice_message


def test_variant_message_includes_improved_and_invite():
    body = _max_variant_choice_message("Hello improved", "")
    assert "Превью (улучшенный вариант)" in body
    assert "Hello improved" in body
    assert "Выберите вариант текста:" in body
    assert "Медиа прикреплено" not in body


def test_variant_message_media_flag():
    body = _max_variant_choice_message("T", '{"version":2,"max":[]}')
    assert "Медиа прикреплено" in body


def test_variant_message_truncates():
    long = "x" * 5000
    body = _max_variant_choice_message(long, "")
    assert len(body) <= 4000
    assert "…\n\nВыберите вариант текста:" in body
    assert "Превью (улучшенный вариант)" in body
