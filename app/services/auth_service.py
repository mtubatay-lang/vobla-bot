"""
Сервис авторизации пользователей для корпоративного бота Воблабир.

Работает с листом "Пользователи" в Google Sheets:
name | phone | code | role | telegram_id | is_active | used_at | юр. лицо | max_user_id (опционально)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Optional, List

from cachetools import TTLCache

from app.config import USERS_SHEET_ID
from app.core.types import Platform
from app.services.sheets_client import get_sheets_client, run_with_retry_on_429  # уже есть в проекте

USERS_SHEET_NAME = "Пользователи"
MAX_ID_HEADERS = ("max_user_id", "max_id", "maxid")
logger = logging.getLogger(__name__)

# Кэш списка пользователей: TTL 5 минут, при bind_* инвалидируется
_users_cache: TTLCache = TTLCache(maxsize=1, ttl=300)
_USERS_CACHE_KEY = "users"
# Быстрый кэш результата find_user_by_platform_id (секунды), снижает повторные проходы по листу в MAX webhook
_find_user_ttl_cache: TTLCache = TTLCache(maxsize=512, ttl=60)


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
    max_user_id: Optional[int] = None  # ID пользователя в MAX


def _get_worksheet():
    """Возвращает объект листа 'Пользователи'."""
    if not USERS_SHEET_ID:
        raise RuntimeError("USERS_SHEET_ID не задан в конфиге")

    def _open():
        client = get_sheets_client()
        spreadsheet = client.open_by_key(USERS_SHEET_ID)
        return spreadsheet.worksheet(USERS_SHEET_NAME)
    return run_with_retry_on_429(_open)


def _get_headers(ws) -> dict:
    """Возвращает маппинг имя колонки -> 1-based индекс."""
    row = ws.row_values(1)
    return {str(h).strip(): i + 1 for i, h in enumerate(row) if str(h).strip()}


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


def _extract_max_user_id(record: dict) -> Optional[int]:
    """Читает MAX ID из поддерживаемых алиасов колонок."""
    raw = ""
    used_key = None
    for key in MAX_ID_HEADERS:
        value = str(record.get(key, "")).strip()
        if value:
            raw = value
            used_key = key
            break
    if used_key and used_key != "max_user_id":
        logger.warning("auth_service: using legacy MAX column alias '%s'", used_key)
    if not raw or raw == "0":
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("auth_service: invalid max user id value=%r", raw)
        return None


def _resolve_max_id_column(headers: dict) -> tuple[Optional[int], Optional[str]]:
    """Возвращает индекс и имя колонки для MAX ID с учётом алиасов."""
    for name in MAX_ID_HEADERS:
        col = headers.get(name)
        if col is not None:
            return col, name
    return None, None


def load_users(force_reload: bool = False) -> List[User]:
    """
    Загружает всех пользователей из листа 'Пользователи'
    и возвращает список User. Результат кэшируется на 5 минут.
    """
    if not force_reload:
        try:
            return _users_cache[_USERS_CACHE_KEY]
        except KeyError:
            pass

    ws = _get_worksheet()
    headers = _get_headers(ws)

    # get_all_records читает всю таблицу, первая строка — заголовки
    records = ws.get_all_records()

    users: List[User] = []
    for idx, rec in enumerate(records):
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

        max_user_id = _extract_max_user_id(rec)

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
            max_user_id=max_user_id,
        )
        users.append(user)

    _users_cache[_USERS_CACHE_KEY] = users
    return users


def find_user_by_platform_id(platform: Platform, user_id: int | str) -> Optional[User]:
    """Ищет пользователя по ID на указанной платформе. Возвращает User или None."""
    uid = int(user_id) if isinstance(user_id, str) else user_id
    cache_key = (platform, uid)
    try:
        return _find_user_ttl_cache[cache_key]
    except KeyError:
        pass
    found: Optional[User] = None
    for user in load_users(force_reload=False):
        if platform == "telegram" and user.telegram_id == uid:
            found = user
            break
        if platform == "max" and user.max_user_id is not None and user.max_user_id == uid:
            found = user
            break
    _find_user_ttl_cache[cache_key] = found
    return found


def find_user_by_telegram_id(telegram_id: int) -> Optional[User]:
    """Ищет пользователя по telegram_id. Обратная совместимость: обёртка над find_user_by_platform_id."""
    return find_user_by_platform_id("telegram", telegram_id)


def find_user_by_code(code: str) -> Optional[User]:
    """Ищет пользователя по коду доступа. Возвращает User или None."""
    normalized = str(code).strip()
    if not normalized:
        return None

    for user in load_users():
        if user.code == normalized:
            return user
    return None


def bind_platform_id(user: User, platform: Platform, platform_user_id: int | str) -> None:
    """
    Привязывает ID пользователя на платформе (telegram или max) и проставляет used_at.
    Инвалидирует кэш пользователей.
    """
    ws = _get_worksheet()
    headers = _get_headers(ws)
    pid = int(platform_user_id) if isinstance(platform_user_id, str) else platform_user_id

    if platform == "telegram":
        col = headers.get("telegram_id", 5)
        ws.update_cell(user.row, col, str(pid))
    elif platform == "max":
        col, col_name = _resolve_max_id_column(headers)
        if col is None:
            # Колонка может отсутствовать — добавляем в конец (после последней известной)
            last_col = max(headers.values()) if headers else 8
            col = last_col + 1
            ws.update_cell(1, col, "max_user_id")
            logger.warning("auth_service: auto-created missing 'max_user_id' column at index=%s", col)
        elif col_name != "max_user_id":
            logger.warning("auth_service: writing MAX user id into legacy column '%s'", col_name)
        ws.update_cell(user.row, col, str(pid))

    used_at_col = headers.get("used_at", 7)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.update_cell(user.row, used_at_col, now_str)

    _users_cache.pop(_USERS_CACHE_KEY, None)
    _find_user_ttl_cache.clear()


def bind_telegram_id(user: User, telegram_id: int) -> None:
    """Привязывает telegram_id к пользователю. Обратная совместимость: обёртка над bind_platform_id."""
    bind_platform_id(user, "telegram", telegram_id)