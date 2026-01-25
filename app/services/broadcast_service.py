"""Сервис для работы с рассылками в Google Sheets."""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.config import STATS_SHEET_ID, BROADCASTS_TAB, BROADCAST_LOGS_TAB, RECIPIENTS_USERS_TAB, RECIPIENTS_CHATS_TAB
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


def create_broadcast_draft(
    created_by_user_id: int,
    created_by_username: Optional[str],
    text_original: str,
    media_json: str,
    users_count: int = 0,
    chats_count: int = 0,
) -> str:
    """
    Создает черновик рассылки в таблице broadcasts.
    Возвращает broadcast_id (uuid12).
    """
    if not STATS_SHEET_ID:
        raise RuntimeError("STATS_SHEET_ID не задан")
    
    broadcast_id = uuid.uuid4().hex[:12]
    now_iso = _utc_now_iso()
    
    ws = _get_ws(BROADCASTS_TAB)
    header_map = _get_headers(ws)
    
    row = []
    headers = ws.row_values(1)
    for header in headers:
        header_clean = header.strip()
        if header_clean == "broadcast_id":
            row.append(broadcast_id)
        elif header_clean == "created_at":
            row.append(now_iso)
        elif header_clean == "created_by":
            # Объединяем user_id и username в одну колонку
            created_by_str = f"{created_by_user_id}"
            if created_by_username:
                created_by_str += f" (@{created_by_username})"
            row.append(created_by_str)
        elif header_clean == "text":
            # Записываем оригинальный текст (позже обновится финальным)
            row.append(text_original)
        elif header_clean == "media_json":
            row.append(media_json)
        elif header_clean == "targets":
            # Объединяем counts в одну колонку
            targets_str = f"users:{users_count},chats:{chats_count}"
            row.append(targets_str)
        elif header_clean == "status":
            row.append("draft")
        elif header_clean == "sent_ok":
            row.append("0")
        elif header_clean == "sent_fail":
            row.append("0")
        # Поддержка дополнительных колонок (если есть)
        elif header_clean in ("created_by_user_id", "created_by_username"):
            # Дополнительные колонки для совместимости
            if header_clean == "created_by_user_id":
                row.append(str(created_by_user_id))
            else:
                row.append(created_by_username or "")
        elif header_clean in ("text_original", "text_final"):
            if header_clean == "text_original":
                row.append(text_original)
            else:
                row.append("")
        elif header_clean in ("selected_variant", "mode", "total"):
            row.append("")
        elif header_clean in ("recipients_users_count", "recipients_chats_count"):
            if header_clean == "recipients_users_count":
                row.append(str(users_count))
            else:
                row.append(str(chats_count))
        else:
            row.append("")
    
    ws.append_row(row, value_input_option="RAW")
    return broadcast_id


def finalize_broadcast(
    broadcast_id: str,
    text_final: str,
    status: str,
    sent_ok: int,
    sent_fail: int,
    selected_variant: str = "",
    mode: str = "",
) -> None:
    """Обновляет рассылку: статус, финальный текст, количество отправленных."""
    if not STATS_SHEET_ID:
        return
    
    ws = _get_ws(BROADCASTS_TAB)
    header_map = _get_headers(ws)
    
    broadcast_id_col = header_map.get("broadcast_id")
    if not broadcast_id_col:
        return
    
    # Ищем строку
    try:
        cell = ws.find(broadcast_id, in_column=broadcast_id_col)
        if not cell:
            return
        row_num = cell.row
    except Exception:
        return
    
    updates = {}
    # Обновляем text (финальный текст)
    if "text" in header_map:
        updates["text"] = text_final
    # Также поддерживаем text_final для совместимости
    elif "text_final" in header_map:
        updates["text_final"] = text_final
    if "status" in header_map:
        updates["status"] = status
    if "sent_ok" in header_map:
        updates["sent_ok"] = str(sent_ok)
    if "sent_fail" in header_map:
        updates["sent_fail"] = str(sent_fail)
    # Поддержка дополнительных колонок (если есть)
    if "selected_variant" in header_map and selected_variant:
        updates["selected_variant"] = selected_variant
    if "mode" in header_map and mode:
        updates["mode"] = mode
    if "total" in header_map:
        updates["total"] = str(sent_ok + sent_fail)
    
    for key, value in updates.items():
        col = header_map[key]
        ws.update_cell(row_num, col, value)


def log_broadcast_recipient(
    broadcast_id: str,
    recipient_type: str,
    recipient_id: int,
    status: str,
    error_text: str = "",
) -> None:
    """Логирует результат отправки одному получателю в broadcast_logs."""
    if not STATS_SHEET_ID:
        return
    
    ws = _get_ws(BROADCAST_LOGS_TAB)
    header_map = _get_headers(ws)
    
    now_iso = _utc_now_iso()
    
    row = []
    headers = ws.row_values(1)
    for header in headers:
        header_clean = header.strip()
        if header_clean == "broadcast_id":
            row.append(broadcast_id)
        elif header_clean == "ts":
            row.append(now_iso)
        elif header_clean == "target_type":
            # В таблице target_type, а не recipient_type
            row.append(recipient_type)
        elif header_clean == "recipient_type":
            # Поддержка для совместимости
            row.append(recipient_type)
        elif header_clean == "chat_id":
            # В таблице chat_id, а не recipient_id
            row.append(str(recipient_id))
        elif header_clean == "recipient_id":
            # Поддержка для совместимости
            row.append(str(recipient_id))
        elif header_clean == "status":
            row.append(status)
        elif header_clean == "error":
            # В таблице error, а не error_text
            row.append(error_text)
        elif header_clean == "error_text":
            # Поддержка для совместимости
            row.append(error_text)
        elif header_clean == "targets":
            # Дополнительное поле (может быть пустым)
            row.append("")
        else:
            row.append("")
    
    ws.append_row(row, value_input_option="RAW")


def read_active_recipients_users() -> List[int]:
    """Читает активных получателей-пользователей из recipients_users."""
    if not STATS_SHEET_ID:
        return []
    
    try:
        ws = _get_ws(RECIPIENTS_USERS_TAB)
        header_map = _get_headers(ws)
        
        user_id_col = header_map.get("user_id")
        is_active_col = header_map.get("is_active")
        
        if not user_id_col:
            return []
        
        values = ws.get_all_values()
        if len(values) < 2:
            return []
        
        headers = [h.strip() for h in values[0]]
        user_id_idx = headers.index("user_id") if "user_id" in headers else -1
        is_active_idx = headers.index("is_active") if "is_active" in headers else -1
        
        result = []
        for row in values[1:]:
            if len(row) <= user_id_idx:
                continue
            
            user_id_str = row[user_id_idx].strip()
            if not user_id_str:
                continue
            
            # Проверяем is_active
            is_active = ""
            if is_active_idx >= 0 and len(row) > is_active_idx:
                is_active = row[is_active_idx].strip().lower()
            
            # Активен, если is_active пусто, "1", "true"
            if is_active and is_active not in ("1", "true"):
                continue
            
            try:
                user_id = int(user_id_str)
                result.append(user_id)
            except ValueError:
                continue
        
        return result
    except Exception:
        return []


def read_active_recipients_chats() -> List[int]:
    """Читает активных получателей-чатов из recipients_chats."""
    if not STATS_SHEET_ID:
        return []
    
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        header_map = _get_headers(ws)
        
        chat_id_col = header_map.get("chat_id")
        is_active_col = header_map.get("is_active")
        
        if not chat_id_col:
            return []
        
        values = ws.get_all_values()
        if len(values) < 2:
            return []
        
        headers = [h.strip() for h in values[0]]
        chat_id_idx = headers.index("chat_id") if "chat_id" in headers else -1
        is_active_idx = headers.index("is_active") if "is_active" in headers else -1
        
        result = []
        for row in values[1:]:
            if len(row) <= chat_id_idx:
                continue
            
            chat_id_str = row[chat_id_idx].strip()
            if not chat_id_str:
                continue
            
            # Проверяем is_active
            is_active = ""
            if is_active_idx >= 0 and len(row) > is_active_idx:
                is_active = row[is_active_idx].strip().lower()
            
            # Активен, если is_active пусто, "1", "true"
            if is_active and is_active not in ("1", "true"):
                continue
            
            try:
                chat_id = int(chat_id_str)
                result.append(chat_id)
            except ValueError:
                continue
        
        return result
    except Exception:
        return []


def read_active_recipients_chats_with_names() -> List[Dict[str, Any]]:
    """Читает активных получателей-чатов с названиями из recipients_chats.
    
    Возвращает список dict: [{"chat_id": int, "name": str}, ...]
    """
    if not STATS_SHEET_ID:
        return []
    
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        header_map = _get_headers(ws)
        
        chat_id_col = header_map.get("chat_id")
        is_active_col = header_map.get("is_active")
        title_col = header_map.get("title")
        
        if not chat_id_col:
            return []
        
        values = ws.get_all_values()
        if len(values) < 2:
            return []
        
        headers = [h.strip() for h in values[0]]
        chat_id_idx = headers.index("chat_id") if "chat_id" in headers else -1
        is_active_idx = headers.index("is_active") if "is_active" in headers else -1
        title_idx = headers.index("title") if "title" in headers else -1
        
        result = []
        for row in values[1:]:
            if len(row) <= chat_id_idx:
                continue
            
            chat_id_str = row[chat_id_idx].strip()
            if not chat_id_str:
                continue
            
            # Проверяем is_active
            is_active = ""
            if is_active_idx >= 0 and len(row) > is_active_idx:
                is_active = row[is_active_idx].strip().lower()
            
            # Активен, если is_active пусто, "1", "true"
            if is_active and is_active not in ("1", "true"):
                continue
            
            try:
                chat_id = int(chat_id_str)
                
                # Получаем название чата
                chat_name = ""
                if title_idx >= 0 and len(row) > title_idx:
                    chat_name = row[title_idx].strip()
                
                # Если название пустое, используем fallback
                if not chat_name:
                    chat_name = f"Чат {chat_id}"
                
                result.append({
                    "chat_id": chat_id,
                    "name": chat_name
                })
            except ValueError:
                continue
        
        return result
    except Exception:
        return []


def read_active_regions() -> List[str]:
    """Читает список уникальных активных регионов из recipients_chats.
    
    Возвращает отсортированный список уникальных регионов: ["Башкирия", "Москва", ...]
    """
    if not STATS_SHEET_ID:
        return []
    
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        header_map = _get_headers(ws)
        
        chat_id_col = header_map.get("chat_id")
        is_active_col = header_map.get("is_active")
        region_col = header_map.get("region")
        
        if not chat_id_col or not region_col:
            return []
        
        values = ws.get_all_values()
        if len(values) < 2:
            return []
        
        headers = [h.strip() for h in values[0]]
        chat_id_idx = headers.index("chat_id") if "chat_id" in headers else -1
        is_active_idx = headers.index("is_active") if "is_active" in headers else -1
        region_idx = headers.index("region") if "region" in headers else -1
        
        if chat_id_idx < 0 or region_idx < 0:
            return []
        
        regions_set = set()
        for row in values[1:]:
            if len(row) <= chat_id_idx or len(row) <= region_idx:
                continue
            
            chat_id_str = row[chat_id_idx].strip()
            if not chat_id_str:
                continue
            
            # Проверяем is_active
            is_active = ""
            if is_active_idx >= 0 and len(row) > is_active_idx:
                is_active = row[is_active_idx].strip().lower()
            
            # Активен, если is_active пусто, "1", "true"
            if is_active and is_active not in ("1", "true"):
                continue
            
            # Получаем регион
            region = row[region_idx].strip()
            if region:
                regions_set.add(region)
        
        return sorted(list(regions_set))
    except Exception:
        return []


def read_chats_by_regions(regions: List[str]) -> List[int]:
    """Читает chat_id активных чатов из указанных регионов.
    
    Args:
        regions: Список названий регионов
    
    Returns:
        Список chat_id чатов из указанных регионов
    """
    if not STATS_SHEET_ID:
        return []
    
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        header_map = _get_headers(ws)
        
        chat_id_col = header_map.get("chat_id")
        is_active_col = header_map.get("is_active")
        region_col = header_map.get("region")
        
        if not chat_id_col or not region_col:
            return []
        
        values = ws.get_all_values()
        if len(values) < 2:
            return []
        
        headers = [h.strip() for h in values[0]]
        chat_id_idx = headers.index("chat_id") if "chat_id" in headers else -1
        is_active_idx = headers.index("is_active") if "is_active" in headers else -1
        region_idx = headers.index("region") if "region" in headers else -1
        
        if chat_id_idx < 0 or region_idx < 0:
            return []
        
        regions_set = set(regions)  # Для быстрого поиска
        result = []
        
        for row in values[1:]:
            if len(row) <= chat_id_idx or len(row) <= region_idx:
                continue
            
            chat_id_str = row[chat_id_idx].strip()
            if not chat_id_str:
                continue
            
            # Проверяем is_active
            is_active = ""
            if is_active_idx >= 0 and len(row) > is_active_idx:
                is_active = row[is_active_idx].strip().lower()
            
            # Активен, если is_active пусто, "1", "true"
            if is_active and is_active not in ("1", "true"):
                continue
            
            # Проверяем регион
            region = row[region_idx].strip()
            if region in regions_set:
                try:
                    chat_id = int(chat_id_str)
                    result.append(chat_id)
                except ValueError:
                    continue
        
        return result
    except Exception:
        return []


def mark_user_failed(user_id: int, error_text: str) -> None:
    """Помечает пользователя как неактивного при ошибке отправки."""
    if not STATS_SHEET_ID:
        return
    
    try:
        ws = _get_ws(RECIPIENTS_USERS_TAB)
        header_map = _get_headers(ws)
        
        user_id_col = header_map.get("user_id")
        if not user_id_col:
            return
        
        try:
            cell = ws.find(str(user_id), in_column=user_id_col)
            if not cell:
                return
            row_num = cell.row
        except Exception:
            return
        
        updates = {}
        
        # Если ошибка 403/blocked → is_active=0
        if "blocked" in error_text.lower() or "forbidden" in error_text.lower():
            if "is_active" in header_map:
                updates["is_active"] = "0"
        
        # Всегда обновляем last_error
        if "last_error" in header_map:
            updates["last_error"] = error_text[:500]  # Ограничиваем длину
        
        for key, value in updates.items():
            col = header_map[key]
            ws.update_cell(row_num, col, value)
    except Exception:
        pass


def mark_chat_failed(chat_id: int, error_text: str) -> None:
    """Помечает чат как неактивный при ошибке отправки."""
    if not STATS_SHEET_ID:
        return
    
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        header_map = _get_headers(ws)
        
        chat_id_col = header_map.get("chat_id")
        if not chat_id_col:
            return
        
        try:
            cell = ws.find(str(chat_id), in_column=chat_id_col)
            if not cell:
                return
            row_num = cell.row
        except Exception:
            return
        
        updates = {}
        
        # Если ошибка 403/blocked → is_active=0
        if "blocked" in error_text.lower() or "forbidden" in error_text.lower():
            if "is_active" in header_map:
                updates["is_active"] = "0"
        
        # Всегда обновляем last_error
        if "last_error" in header_map:
            updates["last_error"] = error_text[:500]  # Ограничиваем длину
        
        for key, value in updates.items():
            col = header_map[key]
            ws.update_cell(row_num, col, value)
    except Exception:
        pass

