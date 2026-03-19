"""Проверка роли admin для Telegram и MAX (единая точка для хендлеров)."""

from __future__ import annotations

from app.core.types import Platform
from app.services.auth_service import find_user_by_platform_id, find_user_by_telegram_id


def is_admin_for_platform(platform: Platform, user_id: int | str) -> bool:
    """
    True, если пользователь найден в листе «Пользователи» и role == admin.
    platform: telegram → поиск по telegram_id; max → по max_user_id.
    """
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if platform == "telegram":
        user = find_user_by_telegram_id(uid)
    else:
        user = find_user_by_platform_id(platform, uid)
    if not user:
        return False
    return str(getattr(user, "role", "") or "").strip().lower() == "admin"
