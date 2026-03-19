"""Сервис для работы с рассылками в Google Sheets."""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from aiogram.enums import ParseMode
from aiogram.types import InputMediaPhoto, InputMediaVideo

from app.config import STATS_SHEET_ID, BROADCASTS_TAB, BROADCAST_LOGS_TAB, RECIPIENTS_USERS_TAB, RECIPIENTS_CHATS_TAB
from app.services.sheets_client import get_sheets_client
from app.core.types import Recipient, Platform, OutgoingMessage

logger = logging.getLogger(__name__)


def parse_broadcast_media_for_platform(media_json: str, platform: Platform) -> List[Dict[str, Any]]:
    """
    Вложения для рассылки по платформе.
    v1: JSON-массив — только Telegram file_id.
    v2: {"version": 2, "telegram": [...], "max": [...]} — раздельные вложения.
    """
    if not (media_json or "").strip():
        return []
    try:
        data = json.loads(media_json)
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return data if platform == "telegram" else []
    if isinstance(data, dict) and data.get("version") == 2:
        key = "telegram" if platform == "telegram" else "max"
        raw = data.get(key)
        if not isinstance(raw, list):
            return []
        out: List[Dict[str, Any]] = []
        for a in raw:
            if not isinstance(a, dict):
                continue
            if platform == "max":
                fid = a.get("id_or_url") or a.get("file_id")
                if not fid:
                    continue
                typ = str(a.get("type", "file")).lower()
                out.append({"type": typ, "id_or_url": str(fid), "file_id": str(fid)})
            else:
                out.append(a)
        return out
    return []


def _recipient_id_for_log(chat_or_user_id: int | str) -> int:
    if isinstance(chat_or_user_id, int):
        return chat_or_user_id
    try:
        return int(str(chat_or_user_id).strip())
    except ValueError:
        return 0


def _row_platform_value(row: List[str], headers: List[str]) -> Platform:
    if "platform" not in headers:
        return "telegram"
    idx = headers.index("platform")
    if len(row) <= idx:
        return "telegram"
    p = (row[idx] or "").strip().lower()
    return "max" if p == "max" else "telegram"


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
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] finalize_broadcast: не удалось найти broadcast_id в таблице: %s", e, exc_info=True)
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


def read_active_user_recipients() -> List[Recipient]:
    """Активные строки recipients_users с учётом колонки platform (по умолчанию telegram)."""
    if not STATS_SHEET_ID:
        return []
    try:
        ws = _get_ws(RECIPIENTS_USERS_TAB)
        values = ws.get_all_values()
        if len(values) < 2:
            return []
        headers = [h.strip() for h in values[0]]
        if "user_id" not in headers:
            return []
        user_id_idx = headers.index("user_id")
        is_active_idx = headers.index("is_active") if "is_active" in headers else -1
        out: List[Recipient] = []
        for row in values[1:]:
            if len(row) <= user_id_idx:
                continue
            uid_str = row[user_id_idx].strip()
            if not uid_str:
                continue
            is_active = ""
            if is_active_idx >= 0 and len(row) > is_active_idx:
                is_active = row[is_active_idx].strip().lower()
            if is_active and is_active not in ("1", "true"):
                continue
            try:
                uid = int(uid_str)
            except ValueError:
                continue
            pl = _row_platform_value(row, headers)
            out.append(Recipient(platform=pl, chat_or_user_id=uid, is_chat=False))
        return out
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] read_active_user_recipients: %s", e, exc_info=True)
        return []


def read_active_chat_recipients() -> List[Recipient]:
    """Активные строки recipients_chats с учётом platform."""
    if not STATS_SHEET_ID:
        return []
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        values = ws.get_all_values()
        if len(values) < 2:
            return []
        headers = [h.strip() for h in values[0]]
        if "chat_id" not in headers:
            return []
        chat_id_idx = headers.index("chat_id")
        is_active_idx = headers.index("is_active") if "is_active" in headers else -1
        out: List[Recipient] = []
        for row in values[1:]:
            if len(row) <= chat_id_idx:
                continue
            cid_str = row[chat_id_idx].strip()
            if not cid_str:
                continue
            is_active = ""
            if is_active_idx >= 0 and len(row) > is_active_idx:
                is_active = row[is_active_idx].strip().lower()
            if is_active and is_active not in ("1", "true"):
                continue
            try:
                cid = int(cid_str)
            except ValueError:
                continue
            pl = _row_platform_value(row, headers)
            out.append(Recipient(platform=pl, chat_or_user_id=cid, is_chat=True))
        return out
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] read_active_chat_recipients: %s", e, exc_info=True)
        return []


def read_active_recipients_users() -> List[int]:
    """Telegram user_id из recipients_users (для обратной совместимости)."""
    return [r.chat_or_user_id for r in read_active_user_recipients() if r.platform == "telegram"]


def read_active_recipients_chats() -> List[int]:
    """Telegram chat_id из recipients_chats (для обратной совместимости)."""
    return [r.chat_or_user_id for r in read_active_chat_recipients() if r.platform == "telegram"]


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
            if _row_platform_value(row, headers) != "telegram":
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
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] read_active_recipients_chats_with_names: %s", e, exc_info=True)
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
            if _row_platform_value(row, headers) != "telegram":
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
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] read_active_regions: %s", e, exc_info=True)
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
                if _row_platform_value(row, headers) != "telegram":
                    continue
                try:
                    chat_id = int(chat_id_str)
                    result.append(chat_id)
                except ValueError:
                    continue
        
        return result
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] read_chats_by_regions: %s", e, exc_info=True)
        return []


def _find_user_recipient_row_num(ws, user_id: int, platform: Platform) -> Optional[int]:
    """Номер строки recipients_users по user_id и platform (если есть колонка platform)."""
    values = ws.get_all_values()
    if len(values) < 2:
        return None
    headers = [h.strip() for h in values[0]]
    if "user_id" not in headers:
        return None
    uid_i = headers.index("user_id")
    plat_i = headers.index("platform") if "platform" in headers else -1
    want = "max" if platform == "max" else "telegram"
    for ri, row in enumerate(values[1:], start=2):
        if len(row) <= uid_i:
            continue
        if str(row[uid_i]).strip() != str(user_id):
            continue
        if plat_i >= 0 and len(row) > plat_i:
            rp = (row[plat_i] or "").strip().lower() or "telegram"
            if rp != want:
                continue
        elif want == "max":
            continue
        return ri
    return None


def _find_chat_recipient_row_num(ws, chat_id: int, platform: Platform) -> Optional[int]:
    values = ws.get_all_values()
    if len(values) < 2:
        return None
    headers = [h.strip() for h in values[0]]
    if "chat_id" not in headers:
        return None
    cid_i = headers.index("chat_id")
    plat_i = headers.index("platform") if "platform" in headers else -1
    want = "max" if platform == "max" else "telegram"
    for ri, row in enumerate(values[1:], start=2):
        if len(row) <= cid_i:
            continue
        if str(row[cid_i]).strip() != str(chat_id):
            continue
        if plat_i >= 0 and len(row) > plat_i:
            rp = (row[plat_i] or "").strip().lower() or "telegram"
            if rp != want:
                continue
        elif want == "max":
            continue
        return ri
    return None


def mark_user_failed(user_id: int, error_text: str, *, platform: Platform = "telegram") -> None:
    """Помечает пользователя как неактивного при ошибке отправки."""
    if not STATS_SHEET_ID:
        return
    
    try:
        ws = _get_ws(RECIPIENTS_USERS_TAB)
        header_map = _get_headers(ws)
        
        user_id_col = header_map.get("user_id")
        if not user_id_col:
            return
        
        row_num = _find_user_recipient_row_num(ws, user_id, platform)
        if row_num is None:
            if platform != "telegram":
                return
            try:
                cell = ws.find(str(user_id), in_column=user_id_col)
                if not cell:
                    return
                row_num = cell.row
            except Exception as e:
                logger.warning("[BROADCAST_SERVICE] mark_user_failed: не удалось найти user_id в таблице: %s", e, exc_info=True)
                return

        updates = {}

        # Если ошибка 403/blocked → is_active=0
        if "blocked" in error_text.lower() or "forbidden" in error_text.lower():
            if "is_active" in header_map:
                updates["is_active"] = "0"

        # Обновляем last_error (для 429 не пишем полный текст)
        if "last_error" in header_map:
            if "429" in error_text or "Quota exceeded" in error_text:
                updates["last_error"] = "Временная ошибка API"
            else:
                updates["last_error"] = error_text[:500]  # Ограничиваем длину

        for key, value in updates.items():
            col = header_map[key]
            ws.update_cell(row_num, col, value)
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] mark_user_failed: %s", e, exc_info=True)


def mark_chat_failed(chat_id: int, error_text: str, *, platform: Platform = "telegram") -> None:
    """Помечает чат как неактивный при ошибке отправки."""
    if not STATS_SHEET_ID:
        return
    
    try:
        ws = _get_ws(RECIPIENTS_CHATS_TAB)
        header_map = _get_headers(ws)
        
        chat_id_col = header_map.get("chat_id")
        if not chat_id_col:
            return
        
        row_num = _find_chat_recipient_row_num(ws, chat_id, platform)
        if row_num is None:
            if platform != "telegram":
                return
            try:
                cell = ws.find(str(chat_id), in_column=chat_id_col)
                if not cell:
                    return
                row_num = cell.row
            except Exception as e:
                logger.warning("[BROADCAST_SERVICE] mark_chat_failed: не удалось найти chat_id в таблице: %s", e, exc_info=True)
                return

        updates = {}

        # Если ошибка 403/blocked → is_active=0
        if "blocked" in error_text.lower() or "forbidden" in error_text.lower():
            if "is_active" in header_map:
                updates["is_active"] = "0"

        # Обновляем last_error (для 429 не пишем полный текст, чтобы не помечать чат как проблемный)
        if "last_error" in header_map:
            if "429" in error_text or "Quota exceeded" in error_text:
                updates["last_error"] = "Временная ошибка API"
            else:
                updates["last_error"] = error_text[:500]  # Ограничиваем длину

        for key, value in updates.items():
            col = header_map[key]
            ws.update_cell(row_num, col, value)
    except Exception as e:
        logger.warning("[BROADCAST_SERVICE] mark_chat_failed: %s", e, exc_info=True)


def get_broadcast_recipients_list(
    mode: str,
    mode_extra: Optional[Dict[str, Any]] = None,
    platform_filter: Optional[Platform] = None,
) -> List[Recipient]:
    """
    Возвращает список получателей рассылки с указанием платформы.
    Колонка platform в Sheets: telegram | max; без колонки — все строки считаются telegram.
    """
    mode_extra = mode_extra or {}
    result: List[Recipient] = []
    if mode == "users":
        result = list(read_active_user_recipients())
    elif mode == "chats":
        result = list(read_active_chat_recipients())
    elif mode == "users_chats":
        result = read_active_user_recipients() + read_active_chat_recipients()
    elif mode == "selected_chats" and mode_extra:
        chat_ids = mode_extra.get("chat_ids") or mode_extra.get("selected_chat_ids") or []
        for x in chat_ids:
            try:
                result.append(Recipient(platform="telegram", chat_or_user_id=int(x), is_chat=True))
            except (TypeError, ValueError):
                continue
    elif mode == "selected_regions" and mode_extra:
        regions = mode_extra.get("regions") or mode_extra.get("selected_regions") or []
        for cid in read_chats_by_regions(regions):
            result.append(Recipient(platform="telegram", chat_or_user_id=cid, is_chat=True))
    if platform_filter:
        result = [r for r in result if r.platform == platform_filter]
    return result


def get_broadcast_recipients(mode: str, mode_extra: Optional[Dict[str, Any]] = None) -> Tuple[List[int], List[int]]:
    """
    Возвращает (users, chats) для режима рассылки — только platform=telegram (legacy API).
    """
    lst = get_broadcast_recipients_list(mode, mode_extra, platform_filter="telegram")
    users = [r.chat_or_user_id for r in lst if not r.is_chat]
    chats = [r.chat_or_user_id for r in lst if r.is_chat]
    return (users, chats)


def mark_recipient_failed(
    platform: Platform,
    recipient_id: int | str,
    error_text: str,
    is_chat: bool = False,
) -> None:
    """Помечает получателя как неактивного при ошибке отправки."""
    rid = int(recipient_id) if isinstance(recipient_id, str) else recipient_id
    if is_chat:
        mark_chat_failed(rid, error_text, platform=platform)
    else:
        mark_user_failed(rid, error_text, platform=platform)


async def send_media_to_recipient(
    bot, chat_id: int, attachments: List[Dict[str, Any]], text: str = ""
) -> None:
    """Отправляет текст и медиа получателю (user или chat)."""
    photos = [att for att in attachments if att.get("type") == "photo"]
    videos = [att for att in attachments if att.get("type") == "video"]
    documents = [att for att in attachments if att.get("type") == "document"]

    if text:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    for i in range(0, len(photos), 10):
        batch = photos[i : i + 10]
        media_group = []
        for idx, att in enumerate(batch):
            caption = att.get("caption", "") if idx == 0 and not text else None
            media_group.append(
                InputMediaPhoto(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None)
            )
        if media_group:
            await bot.send_media_group(chat_id=chat_id, media=media_group)
    for i in range(0, len(videos), 10):
        batch = videos[i : i + 10]
        media_group = []
        for idx, att in enumerate(batch):
            caption = att.get("caption", "") if idx == 0 and not text else None
            media_group.append(
                InputMediaVideo(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None)
            )
        if media_group:
            await bot.send_media_group(chat_id=chat_id, media=media_group)
    for att in documents:
        caption = att.get("caption", "") if not text else None
        await bot.send_document(
            chat_id=chat_id,
            document=att["file_id"],
            caption=caption,
            parse_mode=ParseMode.HTML if caption else None,
        )


async def execute_broadcast(
    bot,
    text_final: str,
    media_json: str,
    mode: str,
    mode_extra: Optional[Dict[str, Any]] = None,
    created_by_user_id: int = 0,
    created_by_username: Optional[str] = None,
    text_original: str = "",
    selected_variant: str = "original",
) -> Tuple[str, int, int]:
    """
    Выполняет рассылку (обратная совместимость: принимает aiogram Bot).
    Telegram + при ENABLE_MAX и MAX_BOT_TOKEN — также MAX-получатели из таблицы.
    """
    from app.config import ENABLE_MAX, MAX_API_BASE_URL, MAX_AUTH_BEARER_PREFIX, MAX_BOT_TOKEN
    from app.platforms.max import MaxAdapter, MaxApiClient
    from app.platforms.telegram import TelegramAdapter

    mode_extra = mode_extra or {}
    recipients = await asyncio.to_thread(get_broadcast_recipients_list, mode, mode_extra)
    adapters: Dict[Platform, Any] = {"telegram": TelegramAdapter(bot)}
    if ENABLE_MAX and (MAX_BOT_TOKEN or "").strip():
        mx_client = MaxApiClient(
            token=MAX_BOT_TOKEN,
            base_url=MAX_API_BASE_URL,
            use_bearer_prefix=MAX_AUTH_BEARER_PREFIX,
        )
        adapters["max"] = MaxAdapter(mx_client)
    return await execute_broadcast_multi(
        adapters,
        recipients,
        text_final,
        media_json,
        created_by_user_id=created_by_user_id,
        created_by_username=created_by_username,
        text_original=text_original,
        selected_variant=selected_variant,
        mode=mode,
    )


async def execute_broadcast_multi(
    adapters: Dict[Platform, Any],
    recipients: List[Recipient],
    text_final: str,
    media_json: str,
    created_by_user_id: int = 0,
    created_by_username: Optional[str] = None,
    text_original: str = "",
    selected_variant: str = "original",
    mode: str = "",
) -> Tuple[str, int, int]:
    """
    Выполняет рассылку по списку получателей с указанием платформы.
    adapters: словарь platform -> MessengerAdapter (TelegramAdapter, MaxAdapter).
    Возвращает (broadcast_id, sent_ok, sent_fail).
    """
    users_count = sum(1 for r in recipients if not r.is_chat)
    chats_count = sum(1 for r in recipients if r.is_chat)

    broadcast_id = await asyncio.to_thread(
        create_broadcast_draft,
        created_by_user_id=created_by_user_id,
        created_by_username=created_by_username,
        text_original=text_original or text_final,
        media_json=media_json,
        users_count=users_count,
        chats_count=chats_count,
    )

    sent_ok = 0
    sent_fail = 0
    semaphore = asyncio.Semaphore(10)

    async def send_to_recipient(r: Recipient) -> None:
        nonlocal sent_ok, sent_fail
        adapter = adapters.get(r.platform)
        if not adapter:
            logger.warning("[BROADCAST_SERVICE] No adapter for platform %s", r.platform)
            await asyncio.to_thread(
                log_broadcast_recipient,
                broadcast_id,
                "user" if not r.is_chat else "chat",
                _recipient_id_for_log(r.chat_or_user_id),
                "fail",
                "no adapter",
            )
            sent_fail += 1
            return
        atts = parse_broadcast_media_for_platform(media_json or "", r.platform)
        async with semaphore:
            try:
                if text_final or atts:
                    if atts:
                        if r.platform == "max":
                            await adapter.send_media(
                                r.chat_or_user_id,
                                atts,
                                caption=text_final,
                                is_group_chat=r.is_chat,
                            )
                        else:
                            await adapter.send_media(
                                r.chat_or_user_id,
                                atts,
                                caption=text_final,
                            )
                    else:
                        await adapter.send_message(
                            OutgoingMessage(
                                chat_id=r.chat_or_user_id,
                                platform=r.platform,
                                text=text_final,
                                is_group_chat=r.is_chat if r.platform == "max" else False,
                            )
                        )
                    await asyncio.to_thread(
                        log_broadcast_recipient,
                        broadcast_id,
                        "user" if not r.is_chat else "chat",
                        _recipient_id_for_log(r.chat_or_user_id),
                        "ok",
                    )
                    sent_ok += 1
                else:
                    await asyncio.to_thread(
                        log_broadcast_recipient,
                        broadcast_id,
                        "user" if not r.is_chat else "chat",
                        _recipient_id_for_log(r.chat_or_user_id),
                        "fail",
                        "empty message",
                    )
                    sent_fail += 1
            except Exception as e:
                err = str(e)[:500]
                await asyncio.to_thread(
                    mark_recipient_failed,
                    r.platform,
                    r.chat_or_user_id,
                    err,
                    r.is_chat,
                )
                await asyncio.to_thread(
                    log_broadcast_recipient,
                    broadcast_id,
                    "user" if not r.is_chat else "chat",
                    r.chat_or_user_id,
                    "fail",
                    err,
                )
                sent_fail += 1

    if recipients:
        await asyncio.gather(*[send_to_recipient(r) for r in recipients], return_exceptions=True)

    await asyncio.to_thread(
        finalize_broadcast,
        broadcast_id=broadcast_id,
        text_final=text_final,
        status="sent",
        sent_ok=sent_ok,
        sent_fail=sent_fail,
        selected_variant=selected_variant,
        mode=mode,
    )
    return (broadcast_id, sent_ok, sent_fail)

