"""Ключ ожидания кода авторизации: MAX присылает user_id строкой — ключ должен совпадать с int."""

import pytest

from app.core import handlers as auth_handlers


@pytest.fixture
def clear_pending_auth():
    auth_handlers._pending_auth.clear()
    yield
    auth_handlers._pending_auth.clear()


def test_pending_auth_uid_int_and_str():
    assert auth_handlers._pending_auth_uid(42) == 42
    assert auth_handlers._pending_auth_uid("42") == 42
    assert auth_handlers._pending_auth_uid(" 38170575 ") == 38170575


def test_is_pending_auth_normalizes_str(clear_pending_auth):
    auth_handlers._pending_auth[("max", 38170575)] = True
    assert auth_handlers.is_pending_auth("max", "38170575") is True
    assert auth_handlers.is_pending_auth("max", 38170575) is True
