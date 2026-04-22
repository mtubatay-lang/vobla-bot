"""Сервис для работы с получателями рассылок в Google Sheets."""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from app.config import STATS_SHEET_ID, RECIPIENTS_USERS_TAB, RECIPIENTS_CHATS_TAB
from app.services.sheets_client import get_sheets_client, run_with_retry_on_429

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    """Возвращает текущее время в формате ISO UTC."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_ws(tab_name: str):
    """Открывает STATS_SHEET_ID и возвращает worksheet(tab_name)."""
    if not STATS_SHEET_ID:
        raise RuntimeError("STATS_SHEET_ID не задан")
    def _open():
        client = get_sheets_client()
        sh = client.open_by_key(STATS_SHEET_ID)
        return sh.worksheet(tab_name)
    return run_with_retry_on_429(_open)


def _get_headers(ws) -> Dict[str, int]:
    """Возвращает маппинг: имя колонки -> индекс (1-based)."""
    headers = ws.row_values(1)
    return {h.strip(): i + 1 for i, h in enumerate(headers) if str(h).strip()}


def _is_429(e: Exception) -> bool:
    """Проверяет, является ли ошибка превышением квоты Google API (429)."""
    msg = str(e.args[0]) if e.args else ""
    return "429" in msg or "Quota exceeded" in msg


def _find_user_row_by_platform(ws, user_id: int, platform: str) -> Optional[int]:
    """Строка recipients_users: совпадение user_id и platform (нужна колонка platform)."""
    values = ws.get_all_values()
    if len(values) < 2:
        return None
    headers = [h.strip() for h in values[0]]
    if "user_id" not in headers or "platform" not in headers:
        return None
    uid_i = headers.index("user_id")
    plat_i = headers.index("platform")
    want = platform.strip().lower()
    for ri, row in enumerate(values[1:], start=2):
        if len(row) <= uid_i:
            continue
        if str(row[uid_i]).strip() != str(user_id):
            continue
        rp = (row[plat_i] if len(row) > plat_i else "").strip().lower() or "telegram"
        if rp == want:
            return ri
    return None


def _find_chat_row_by_platform(ws, chat_id: int, platform: str) -> Optional[int]:
    values = ws.get_all_values()
    if len(values) < 2:
        return None
    headers = [h.strip() for h in values[0]]
    if "chat_id" not in headers or "platform" not in headers:
        return None
    cid_i = headers.index("chat_id")
    plat_i = headers.index("platform")
    want = platform.strip().lower()
    for ri, row in enumerate(values[1:], start=2):
        if len(row) <= cid_i:
            continue
        if str(row[cid_i]).strip() != str(chat_id):
            continue
        rp = (row[plat_i] if len(row) > plat_i else "").strip().lower() or "telegram"
        if rp == want:
            return ri
    return None


def _find_row_by_value(ws, col: int, value: str) -> Optional[int]:
    """
    Ищет строку по точному совпадению в колонке.
    Возвращает номер строки (1-based) или None.
    """
    try:
        cell = ws.find(str(value), in_column=col)
        if cell:
            return cell.row
    except Exception as e:
        logger.warning("[BROADCAST_RECIPIENTS] _find_row_by_value: %s", e, exc_info=True)
    return None


def upsert_user_recipient(
    user_id: int,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
    *,
    platform: str = "telegram",
) -> None:
    """
    Добавляет или обновляет получателя-пользователя в таблице recipients_users.
    user_id — id в указанной платформе. Колонка platform: telegram | max (обязательна для MAX).
    """
    if not STATS_SHEET_ID:
        return  # Тихий выход, если не настроено

    plat_norm = (platform or "telegram").strip().lower()
    if plat_norm not in ("telegram", "max"):
        plat_norm = "telegram"

    for attempt in range(3):
        try:
            ws = _get_ws(RECIPIENTS_USERS_TAB)
            header_map = _get_headers(ws)

            # Ищем колонку user_id
            user_id_col = header_map.get("user_id")
            if not user_id_col:
                return  # Нет колонки user_id - пропускаем

            if plat_norm == "max" and "platform" not in header_map:
                logger.warning(
                    "[BROADCAST_RECIPIENTS] Добавьте колонку platform в recipients_users для MAX (user_id=%s)",
                    user_id,
                )
                return

            if "platform" in header_map:
                row_num = _find_user_row_by_platform(ws, user_id, plat_norm)
            else:
                row_num = _find_row_by_value(ws, user_id_col, str(user_id))
            now_iso = _utc_now_iso()

            if row_num:
                # Обновляем существующую строку
                updates = {}

                # Обязательные поля при обновлении
                if "updated_at" in header_map:
                    updates["updated_at"] = now_iso
                if "is_active" in header_map:
                    updates["is_active"] = "1"
                if "last_error" in header_map:
                    updates["last_error"] = ""

                # Опциональные поля
                if "username" in header_map:
                    updates["username"] = username or ""
                if "full_name" in header_map:
                    updates["full_name"] = full_name or ""
                if "platform" in header_map:
                    updates["platform"] = plat_norm

                # Если created_at пустой — заполняем (один раз)
                if "created_at" in header_map:
                    created_at_col = header_map["created_at"]
                    existing_created_at = ws.cell(row_num, created_at_col).value
                    if not existing_created_at or not existing_created_at.strip():
                        updates["created_at"] = now_iso

                # Обновляем все поля по header_map
                for key, value in updates.items():
                    col = header_map[key]
                    ws.update_cell(row_num, col, value)
            else:
                # Добавляем новую строку
                row = []
                # Собираем строку по порядку заголовков
                headers = ws.row_values(1)
                for header in headers:
                    header_clean = header.strip()
                    if header_clean == "user_id":
                        row.append(str(user_id))
                    elif header_clean == "username":
                        row.append(username or "")
                    elif header_clean == "full_name":
                        row.append(full_name or "")
                    elif header_clean == "created_at":
                        row.append(now_iso)
                    elif header_clean == "updated_at":
                        row.append(now_iso)
                    elif header_clean == "is_active":
                        row.append("1")
                    elif header_clean == "last_error":
                        row.append("")
                    elif header_clean == "platform":
                        row.append(plat_norm)
                    else:
                        row.append("")

                ws.append_row(row, value_input_option="RAW")
            return
        except Exception as e:
            if _is_429(e) and attempt < 2:
                delay = 2 + attempt * 3  # 2s, 5s
                logger.debug("[BROADCAST_RECIPIENTS] upsert_user_recipient 429, retry in %ss: %s", delay, e)
                time.sleep(delay)
                continue
            logger.warning("[BROADCAST_RECIPIENTS] upsert_user_recipient: %s", e, exc_info=True)
            return


def upsert_chat_recipient(
    chat_id: int,
    chat_type: str,
    title: Optional[str] = None,
    username: Optional[str] = None,
    *,
    platform: str = "telegram",
) -> None:
    """
    Добавляет или обновляет получателя-чат в таблице recipients_chats.
    chat_id - ключ (уникальный идентификатор).
    При 429 (Quota exceeded) повторяет запрос до 2 раз с задержкой 2 и 5 сек.
    """
    if not STATS_SHEET_ID:
        return  # Тихий выход, если не настроено

    plat_norm = (platform or "telegram").strip().lower()
    if plat_norm not in ("telegram", "max"):
        plat_norm = "telegram"

    for attempt in range(3):
        try:
            ws = _get_ws(RECIPIENTS_CHATS_TAB)
            header_map = _get_headers(ws)

            # Ищем колонку chat_id
            chat_id_col = header_map.get("chat_id")
            if not chat_id_col:
                return  # Нет колонки chat_id - пропускаем

            if plat_norm == "max" and "platform" not in header_map:
                logger.warning(
                    "[BROADCAST_RECIPIENTS] Добавьте колонку platform в recipients_chats для MAX (chat_id=%s)",
                    chat_id,
                )
                return

            if "platform" in header_map:
                row_num = _find_chat_row_by_platform(ws, chat_id, plat_norm)
            else:
                row_num = _find_row_by_value(ws, chat_id_col, str(chat_id))
            now_iso = _utc_now_iso()

            if row_num:
                # Обновляем существующую строку
                updates = {}

                # Обязательные поля при обновлении
                if "updated_at" in header_map:
                    updates["updated_at"] = now_iso
                if "is_active" in header_map:
                    updates["is_active"] = "1"
                if "last_error" in header_map:
                    updates["last_error"] = ""

                # Опциональные поля
                if "title" in header_map:
                    updates["title"] = title or ""
                if "username" in header_map:
                    updates["username"] = username or ""
                if "chat_type" in header_map:
                    updates["chat_type"] = chat_type
                if "platform" in header_map:
                    updates["platform"] = plat_norm

                # Если created_at пустой — заполняем (один раз)
                if "created_at" in header_map:
                    created_at_col = header_map["created_at"]
                    existing_created_at = ws.cell(row_num, created_at_col).value
                    if not existing_created_at or not existing_created_at.strip():
                        updates["created_at"] = now_iso

                # Обновляем все поля по header_map
                for key, value in updates.items():
                    col = header_map[key]
                    ws.update_cell(row_num, col, value)
            else:
                # Добавляем новую строку
                row = []
                # Собираем строку по порядку заголовков
                headers = ws.row_values(1)
                for header in headers:
                    header_clean = header.strip()
                    if header_clean == "chat_id":
                        row.append(str(chat_id))
                    elif header_clean == "chat_type":
                        row.append(chat_type)
                    elif header_clean == "title":
                        row.append(title or "")
                    elif header_clean == "username":
                        row.append(username or "")
                    elif header_clean == "created_at":
                        row.append(now_iso)
                    elif header_clean == "updated_at":
                        row.append(now_iso)
                    elif header_clean == "is_active":
                        row.append("1")
                    elif header_clean == "last_error":
                        row.append("")
                    elif header_clean == "platform":
                        row.append(plat_norm)
                    else:
                        row.append("")

                ws.append_row(row, value_input_option="RAW")
            return
        except Exception as e:
            if _is_429(e) and attempt < 2:
                delay = 2 + attempt * 3  # 2s, 5s
                logger.debug("[BROADCAST_RECIPIENTS] upsert_chat_recipient 429, retry in %ss: %s", delay, e)
                time.sleep(delay)
                continue
            logger.warning("[BROADCAST_RECIPIENTS] upsert_chat_recipient: %s", e, exc_info=True)
            return

