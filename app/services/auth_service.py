"""
Сервис авторизации пользователей для корпоративного бота Воблабир.

Работает с листом "Пользователи" в Google Sheets:
name | phone | code | role | telegram_id | is_active | used_at | юр. лицо
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from cachetools import TTLCache

from app.config import USERS_SHEET_ID
from app.services.sheets_client import get_sheets_client  # уже есть в проекте

USERS_SHEET_NAME = "Пользователи"

# Кэш списка пользователей: TTL 5 минут, при bind_telegram_id инвалидируется
_users_cache: TTLCache = TTLCache(maxsize=1, ttl=300)
_USERS_CACHE_KEY = "users"


@dataclass
class User:
    """Модель пользователя из таблицы."""
    row: int                      # номер строки в листе (нужен для обновления)
    name: str
    phone: str
    code: str
    role: str
    telegram_id: Optional[int]
    is_active: bool
    used_at: str
    legal_entity: str  # юр. лицо


def _get_worksheet():
    """Возвращает объект листа 'Пользователи'."""
    if not USERS_SHEET_ID:
        raise RuntimeError("USERS_SHEET_ID не задан в конфиге")

    client = get_sheets_client()
    spreadsheet = client.open_by_key(USERS_SHEET_ID)
    return spreadsheet.worksheet(USERS_SHEET_NAME)


def _parse_bool(value) -> bool:
    """
    Нормализуем значение из ячейки is_active в bool.
    Поддерживаем варианты: TRUE/FALSE, 1/0, yes/no.
    """
    if value is None:
        return True  # по умолчанию считаем активным

    text = str(value).strip().upper()
    if text in ("TRUE", "1", "YES", "Y", "ДА"):
        return True
    if text in ("FALSE", "0", "NO", "N", "НЕТ"):
        return False
    # на всякий случай — всё непонятное считаем активным
    return True


def load_users() -> List[User]:
    """
    Загружает всех пользователей из листа 'Пользователи'
    и возвращает список User. Результат кэшируется на 5 минут.
    """
    try:
        return _users_cache[_USERS_CACHE_KEY]
    except KeyError:
        pass

    ws = _get_worksheet()

    # get_all_records читает всю таблицу, первая строка — заголовки
    records = ws.get_all_records()

    users: List[User] = []
    for idx, rec in enumerate(records):
        # индекс в листе = номер строки + 1 (заголовки) + 1 (смещение)
        # т.е. первая запись (idx=0) находится в строке 2
        row_number = idx + 2

        raw_telegram_id = str(rec.get("telegram_id", "")).strip()
        telegram_id: Optional[int]
        if raw_telegram_id and raw_telegram_id != "0":
            try:
                telegram_id = int(raw_telegram_id)
            except ValueError:
                telegram_id = None
        else:
            telegram_id = None

        user = User(
            row=row_number,
            name=str(rec.get("name", "")).strip(),
            phone=str(rec.get("phone", "")).strip(),
            code=str(rec.get("code", "")).strip(),
            role=str(rec.get("role", "")).strip() or "user",
            telegram_id=telegram_id,
            is_active=_parse_bool(rec.get("is_active")),
            used_at=str(rec.get("used_at", "")).strip(),
            legal_entity=str(rec.get("юр. лицо", "")).strip(),
        )
        users.append(user)

    _users_cache[_USERS_CACHE_KEY] = users
    return users


def find_user_by_telegram_id(telegram_id: int) -> Optional[User]:
    """Ищет пользователя по telegram_id. Возвращает User или None."""
    for user in load_users():
        if user.telegram_id == telegram_id:
            return user
    return None


def find_user_by_code(code: str) -> Optional[User]:
    """Ищет пользователя по коду доступа. Возвращает User или None."""
    normalized = str(code).strip()
    if not normalized:
        return None

    for user in load_users():
        if user.code == normalized:
            return user
    return None


def bind_telegram_id(user: User, telegram_id: int) -> None:
    """
    Привязывает telegram_id к пользователю и проставляет used_at (дата/время).
    Инвалидирует кэш пользователей, чтобы при следующем запросе подтянуть свежие данные.
    """
    ws = _get_worksheet()

    # колонка 5 — telegram_id
    ws.update_cell(user.row, 5, str(telegram_id))

    # колонка 7 — used_at
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.update_cell(user.row, 7, now_str)

    _users_cache.pop(_USERS_CACHE_KEY, None)