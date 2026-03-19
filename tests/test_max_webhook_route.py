"""
Регрессия: вложенный webhook(request: Request) + PEP 563 в том же модуле
ломали разбор аннотаций — FastAPI вешал тело как JSON Body(Request) → 422.
"""

import pytest
from fastapi.routing import APIRoute


@pytest.fixture
def max_webhook_minimal_env(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "1:fake")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")
    monkeypatch.setenv("SHEET_ID", "sheet-id")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    monkeypatch.setenv("ENABLE_MAX", "true")
    monkeypatch.setenv("MAX_BOT_TOKEN", "max-test-token")
    monkeypatch.setenv("MAX_WEBHOOK_SECRET", "")


def test_max_webhook_route_has_no_body_field(max_webhook_minimal_env):
    from app.max_entrypoint import _create_app

    app = _create_app()
    route = next(
        r
        for r in app.routes
        if isinstance(r, APIRoute) and r.path == "/webhook/max"
    )
    assert route.body_field is None, (
        "body_field должен быть None: иначе MAX JSON трактуется как модель Request → 422"
    )


def test_max_webhook_post_bot_started_not_422(max_webhook_minimal_env):
    from app.max_entrypoint import _create_app
    from fastapi.testclient import TestClient

    app = _create_app()
    client = TestClient(app)
    body = {
        "update_type": "bot_started",
        "user": {"user_id": 1, "name": "U"},
    }
    r = client.post("/webhook/max", json=body)
    assert r.status_code != 422, r.text
