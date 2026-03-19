import pytest

from app.core.types import CallbackEvent, IncomingMessage, InternalChat, InternalUser
from app.platforms.max.router import MaxActionRouter


class DummyAdapter:
    platform = "max"

    def __init__(self):
        self.sent_messages = []
        self.answered_callbacks = []
        self.typing_calls = 0

    async def send_message(self, msg):
        self.sent_messages.append(msg)
        return "m1"

    async def answer_callback(self, callback_id):
        self.answered_callbacks.append(callback_id)

    async def send_typing(self, *args, **kwargs):
        self.typing_calls += 1


def _mk_msg(text: str, uid: int = 100) -> IncomingMessage:
    return IncomingMessage(
        user=InternalUser(id=uid, platform="max", name="U"),
        chat=InternalChat(id=uid, platform="max", is_group=False),
        text=text,
        raw={"update_type": "message_created"},
    )


def _mk_cb(data: str, uid: int = 100) -> CallbackEvent:
    return CallbackEvent(
        user=InternalUser(id=uid, platform="max", name="U"),
        chat=InternalChat(id=uid, platform="max", is_group=False),
        data=data,
        callback_id="cb-1",
        raw={"update_type": "message_callback"},
    )


@pytest.mark.asyncio
async def test_router_login_command_triggers_auth_flow(monkeypatch):
    import app.platforms.max.router as max_router_mod

    called = {"n": 0}

    async def fake_handle_start_auth_callback(adapter, event):
        called["n"] += 1

    monkeypatch.setattr(max_router_mod, "handle_start_auth_callback", fake_handle_start_auth_callback)
    monkeypatch.setattr(max_router_mod, "find_user_by_platform_id", lambda platform, user_id: None)

    router = MaxActionRouter()
    adapter = DummyAdapter()
    result = await router.route(adapter, _mk_msg("/login"))
    assert result.handled is True
    assert result.action == "cmd_login"
    assert called["n"] == 1


@pytest.mark.asyncio
async def test_router_admin_guard_rejects_non_admin(monkeypatch):
    import app.platforms.max.router as max_router_mod

    class User:
        role = "user"

    monkeypatch.setattr(max_router_mod, "find_user_by_platform_id", lambda platform, user_id: User())
    router = MaxActionRouter()
    adapter = DummyAdapter()
    result = await router.route(adapter, _mk_msg("/admin"))
    assert result.handled is False
    assert result.action == "cmd_admin"
    assert adapter.sent_messages
    assert "только администраторам" in adapter.sent_messages[-1].text


@pytest.mark.asyncio
async def test_router_ask_callback_requires_auth(monkeypatch):
    import app.platforms.max.router as max_router_mod

    monkeypatch.setattr(max_router_mod, "find_user_by_platform_id", lambda platform, user_id: None)
    router = MaxActionRouter()
    adapter = DummyAdapter()
    result = await router.route(adapter, _mk_cb("qa_start"))
    assert result.handled is False
    assert result.action == "qa_start"
    assert adapter.answered_callbacks == ["cb-1"]
    assert "Сначала пройдите авторизацию" in adapter.sent_messages[-1].text
