"""Сервис для работы с получателями рассылок в Google Sheets."""

from datetime import datetime, timezone
from typing import Dict, Optional

from app.config import STATS_SHEET_ID, RECIPIENTS_USERS_TAB, RECIPIENTS_CHATS_TAB
from app.services.sheets_client import get_sheets_client


def _utc_now_iso() -> str:
    """Возвращает текущее время в формате ISO UTC."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_ws(tab_name: str):
    """Открывает STATS_SHEET_ID и возвращает worksheet(tab_name)."""
    if not STATS_SHEET_ID:
        raise RuntimeError("STATS_SHEET_ID не задан")
    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    return sh.worksheet(tab_name)


def _get_headers(ws) -> Dict[str, int]:
    """Возвращает маппинг: имя колонки -> индекс (1-based)."""
    headers = ws.row_values(1)
    return {h.strip(): i + 1 for i, h in enumerate(headers) if str(h).strip()}


def _find_row_by_value(ws, col: int, value: str) -> Optional[int]:
    """
    Ищет строку по точному совпадению в колонке.
    Возвращает номер строки (1-based) или None.
    """
    try:
        cell = ws.find(str(value), in_column=col)
        if cell:
            return cell.row
    except Exception:
        pass
    return None


def upsert_user_recipient(user_id: int, username: Optional[str] = None, full_name: Optional[str] = None) -> None:
    """
    Добавляет или обновляет получателя-пользователя в таблице recipients_users.
    user_id - ключ (уникальный идентификатор).
    """
    if not STATS_SHEET_ID:
        return  # Тихий выход, если не настроено
    
    try:
        ws = _get_ws(RECIPIENTS_USERS_TAB)
        header_map = _get_headers(ws)
        
        # Ищем колонку user_id
        user_id_col = header_map.get("user_id")
        if not user_id_col:
            return  # Нет колонки user_id - пропускаем
        
        # Ищем существующую строку
        row_num = _find_row_by_value(ws, user_id_col, str(user_id))
        now_iso = _utc_now_iso()
        
        if row_num:
            # Обновляем существующую строку
            updates = {}
            if "username" in header_map:
                updates["username"] = username or ""
            if "full_name" in header_map:
                updates["full_name"] = full_name or ""
            if "updated_at" in header_map:
                updates["updated_at"] = now_iso
            
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
                else:
                    row.append("")
            
            ws.append_row(row, value_input_option="RAW")
    except Exception:
        # Тихий выход при ошибках (чтобы не ломать основной функционал)
        pass


def upsert_chat_recipient(chat_id: int, chat_type: str, title: Optional[str] = None, username: Optional[str] = None) -> None:
    """
    Добавляет или обновляет получателя-чат в таблице recipients_chats.
    chat_id - ключ (уникальный идентификатор).
    """
    if not STATS_SHEET_ID:
        return  # Тихий выход, если не настроено
    
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        header_map = _get_headers(ws)
        
        # Ищем колонку chat_id
        chat_id_col = header_map.get("chat_id")
        if not chat_id_col:
            return  # Нет колонки chat_id - пропускаем
        
        # Ищем существующую строку
        row_num = _find_row_by_value(ws, chat_id_col, str(chat_id))
        now_iso = _utc_now_iso()
        
        if row_num:
            # Обновляем существующую строку
            updates = {}
            if "title" in header_map:
                updates["title"] = title or ""
            if "username" in header_map:
                updates["username"] = username or ""
            if "chat_type" in header_map:
                updates["chat_type"] = chat_type
            if "updated_at" in header_map:
                updates["updated_at"] = now_iso
            
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
                else:
                    row.append("")
            
            ws.append_row(row, value_input_option="RAW")
    except Exception:
        # Тихий выход при ошибках (чтобы не ломать основной функционал)
        pass

