import app.services.auth_service as auth_service


def test_extract_max_user_id_aliases():
    assert auth_service._extract_max_user_id({"max_user_id": "123"}) == 123
    assert auth_service._extract_max_user_id({"max_id": "456"}) == 456
    assert auth_service._extract_max_user_id({"maxid": "789"}) == 789
    assert auth_service._extract_max_user_id({"max_id": ""}) is None


def test_resolve_max_id_column_prefers_canonical():
    headers = {"name": 1, "max_id": 9, "max_user_id": 10}
    col, name = auth_service._resolve_max_id_column(headers)
    assert col == 10
    assert name == "max_user_id"


def test_find_user_by_platform_id_load_users_and_ttl_cache(monkeypatch):
    called = []
    user = auth_service.User(
        row=2,
        name="U",
        phone="",
        code="",
        role="admin",
        telegram_id=1,
        is_active=True,
        used_at="",
        legal_entity="",
        max_user_id=42,
    )

    def fake_load_users(force_reload=False):
        called.append(force_reload)
        return [user]

    monkeypatch.setattr(auth_service, "load_users", fake_load_users)
    auth_service._find_user_ttl_cache.clear()
    assert auth_service.find_user_by_platform_id("max", 42) is not None
    assert auth_service.find_user_by_platform_id("telegram", 1) is not None
    assert called == [False, False]
    assert auth_service.find_user_by_platform_id("max", 42) is not None
    assert len(called) == 2
