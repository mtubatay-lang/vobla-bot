"""Сервис для плановых (автоматических) рассылок: CRUD в Google Sheets, расчёт next_run_iso."""

import json
import logging
import uuid
from calendar import monthrange
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo

from gspread.exceptions import WorksheetNotFound

from app.config import (
    STATS_SHEET_ID,
    SCHEDULED_BROADCASTS_TAB,
    BROADCAST_SCHEDULE_TIMEZONE,
    BROADCAST_SCHEDULE_HOUR,
    BROADCAST_SCHEDULE_MINUTE,
)
from app.services.sheets_client import get_sheets_client

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_ws(tab_name: str):
    if not STATS_SHEET_ID:
        raise RuntimeError("STATS_SHEET_ID не задан")
    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    try:
        return sh.worksheet(tab_name)
    except WorksheetNotFound:
        raise RuntimeError(
            f"Лист «{tab_name}» не найден в таблице (STATS_SHEET_ID). "
            f"Добавьте лист с именем «{tab_name}» в Google Таблицу или задайте SCHEDULED_BROADCASTS_TAB в настройках."
        ) from None


def _get_headers(ws) -> Dict[str, int]:
    headers = ws.row_values(1)
    return {h.strip(): i + 1 for i, h in enumerate(headers) if str(h).strip()}


def _schedule_tz() -> ZoneInfo:
    try:
        return ZoneInfo(BROADCAST_SCHEDULE_TIMEZONE)
    except Exception as e:
        logger.warning("[SCHEDULED_BROADCAST] Invalid BROADCAST_SCHEDULE_TIMEZONE %s, using UTC: %s", BROADCAST_SCHEDULE_TIMEZONE, e)
        return ZoneInfo("UTC")


def _next_run_weekly(weekday: int) -> datetime:
    """Следующая дата: выбранный день недели в 10:00 по таймзоне бота. weekday: 0=пн, 6=вс."""
    tz = _schedule_tz()
    now = datetime.now(tz)
    target = now.replace(hour=BROADCAST_SCHEDULE_HOUR, minute=BROADCAST_SCHEDULE_MINUTE, second=0, microsecond=0)
    if now.weekday() == weekday and now < target:
        return target
    days_ahead = (weekday - now.weekday() + 7) % 7
    if days_ahead == 0:
        days_ahead = 7
    next_date = (now + timedelta(days=days_ahead)).replace(
        hour=BROADCAST_SCHEDULE_HOUR, minute=BROADCAST_SCHEDULE_MINUTE, second=0, microsecond=0
    )
    return next_date


def _next_run_monthly(day: int) -> datetime:
    """Следующая дата: день месяца в 10:00 по таймзоне бота. day 1-31; если числа нет в месяце — последний день."""
    tz = _schedule_tz()
    now = datetime.now(tz)
    # Текущий месяц: можем ли мы ещё сегодня отправить?
    _, last_day = monthrange(now.year, now.month)
    actual_day = min(day, last_day)
    candidate = now.replace(day=actual_day, hour=BROADCAST_SCHEDULE_HOUR, minute=BROADCAST_SCHEDULE_MINUTE, second=0, microsecond=0)
    if now < candidate:
        return candidate
    # Следующий месяц
    if now.month == 12:
        next_year, next_month = now.year + 1, 1
    else:
        next_year, next_month = now.year, now.month + 1
    _, last_day = monthrange(next_year, next_month)
    actual_day = min(day, last_day)
    return now.replace(year=next_year, month=next_month, day=actual_day, hour=BROADCAST_SCHEDULE_HOUR, minute=BROADCAST_SCHEDULE_MINUTE, second=0, microsecond=0)


def compute_next_run_iso(schedule_type: str, schedule_config: Dict[str, Any]) -> str:
    """Вычисляет next_run в ISO UTC из schedule_type и schedule_config."""
    if schedule_type == "weekly":
        weekday = int(schedule_config.get("weekday", 0))
        next_dt = _next_run_weekly(weekday)
    elif schedule_type == "monthly":
        day = int(schedule_config.get("day", 1))
        day = max(1, min(31, day))
        next_dt = _next_run_monthly(day)
    else:
        next_dt = _next_run_weekly(0)
    return next_dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def create_scheduled_broadcast(
    created_by_user_id: int,
    created_by_username: Optional[str],
    text_final: str,
    media_json: str,
    mode: str,
    mode_extra: str,
    schedule_type: str,
    schedule_config: Dict[str, Any],
) -> str:
    """
    Создаёт запись плановой рассылки в SCHEDULED_BROADCASTS_TAB.
    Возвращает schedule_id (uuid12).
    """
    if not STATS_SHEET_ID:
        raise RuntimeError("STATS_SHEET_ID не задан")

    schedule_id = uuid.uuid4().hex[:12]
    now_iso = _utc_now_iso()
    next_run_iso = compute_next_run_iso(schedule_type, schedule_config)
    mode_extra_str = mode_extra if isinstance(mode_extra, str) else json.dumps(mode_extra, ensure_ascii=False)
    config_str = json.dumps(schedule_config, ensure_ascii=False)

    ws = _get_ws(SCHEDULED_BROADCASTS_TAB)
    headers = ws.row_values(1)
    header_map = _get_headers(ws)

    row = []
    for header in headers:
        header_clean = header.strip()
        if header_clean == "schedule_id":
            row.append(schedule_id)
        elif header_clean == "created_at":
            row.append(now_iso)
        elif header_clean == "text_final":
            row.append(text_final)
        elif header_clean == "media_json":
            row.append(media_json)
        elif header_clean == "mode":
            row.append(mode)
        elif header_clean == "mode_extra":
            row.append(mode_extra_str)
        elif header_clean == "schedule_type":
            row.append(schedule_type)
        elif header_clean == "schedule_config":
            row.append(config_str)
        elif header_clean == "next_run_iso":
            row.append(next_run_iso)
        elif header_clean == "is_active":
            row.append("1")
        elif header_clean == "last_run_iso":
            row.append("")
        else:
            row.append("")

    ws.append_row(row, value_input_option="RAW")
    return schedule_id


def read_active_due_scheduled_broadcasts() -> List[Dict[str, Any]]:
    """Читает активные плановые рассылки с next_run_iso <= now (UTC)."""
    if not STATS_SHEET_ID:
        return []

    try:
        ws = _get_ws(SCHEDULED_BROADCASTS_TAB)
        header_map = _get_headers(ws)
        values = ws.get_all_values()
        if len(values) < 2:
            return []

        now_utc = datetime.now(timezone.utc)
        next_run_col = header_map.get("next_run_iso")
        is_active_col = header_map.get("is_active")
        if not next_run_col or not is_active_col:
            return []

        headers = [h.strip() for h in values[0]]
        result = []
        for row in values[1:]:
            if len(row) < max(header_map.values()):
                continue
            is_active = ""
            if is_active_col - 1 < len(row):
                is_active = str(row[is_active_col - 1]).strip().lower()
            if is_active not in ("1", "true"):
                continue
            next_run_str = row[next_run_col - 1].strip() if next_run_col - 1 < len(row) else ""
            if not next_run_str:
                continue
            try:
                next_run = datetime.fromisoformat(next_run_str.replace("Z", "+00:00"))
                if next_run.tzinfo is None:
                    next_run = next_run.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if next_run > now_utc:
                continue

            record = {}
            for h, col in header_map.items():
                if col - 1 < len(row):
                    record[h] = row[col - 1]
            record["_row_index"] = len(result) + 2
            result.append(record)
        return result
    except Exception as e:
        logger.warning("[SCHEDULED_BROADCAST] read_active_due_scheduled_broadcasts: %s", e, exc_info=True)
        return []


def update_scheduled_broadcast_after_run(schedule_id: str, next_run_iso: str, last_run_iso: str) -> None:
    """Обновляет next_run_iso и last_run_iso после выполнения рассылки."""
    if not STATS_SHEET_ID:
        return
    try:
        ws = _get_ws(SCHEDULED_BROADCASTS_TAB)
        header_map = _get_headers(ws)
        col = header_map.get("schedule_id")
        if not col:
            return
        cell = ws.find(schedule_id, in_column=col)
        if not cell:
            return
        row_num = cell.row
        if "next_run_iso" in header_map:
            ws.update_cell(row_num, header_map["next_run_iso"], next_run_iso)
        if "last_run_iso" in header_map:
            ws.update_cell(row_num, header_map["last_run_iso"], last_run_iso)
    except Exception as e:
        logger.warning("[SCHEDULED_BROADCAST] update_scheduled_broadcast_after_run: %s", e, exc_info=True)


def compute_next_run_iso_after_execution(schedule_type: str, schedule_config: Dict[str, Any]) -> str:
    """Вычисляет следующий next_run после выполнения (weekly: +7 дней, monthly: +1 месяц)."""
    tz = _schedule_tz()
    now = datetime.now(tz)
    if schedule_type == "weekly":
        next_dt = (now + timedelta(days=7)).replace(
            hour=BROADCAST_SCHEDULE_HOUR, minute=BROADCAST_SCHEDULE_MINUTE, second=0, microsecond=0
        )
    elif schedule_type == "monthly":
        day = max(1, min(31, int(schedule_config.get("day", 1))))
        if now.month == 12:
            next_year, next_month = now.year + 1, 1
        else:
            next_year, next_month = now.year, now.month + 1
        _, last_day = monthrange(next_year, next_month)
        actual_day = min(day, last_day)
        next_dt = now.replace(year=next_year, month=next_month, day=actual_day, hour=BROADCAST_SCHEDULE_HOUR, minute=BROADCAST_SCHEDULE_MINUTE, second=0, microsecond=0)
    else:
        next_dt = now + timedelta(days=7)
    return next_dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def set_scheduled_broadcast_inactive(schedule_id: str) -> bool:
    """Устанавливает is_active=0 для плановой рассылки. Возвращает True если запись найдена."""
    if not STATS_SHEET_ID:
        return False
    try:
        ws = _get_ws(SCHEDULED_BROADCASTS_TAB)
        header_map = _get_headers(ws)
        col = header_map.get("schedule_id")
        if not col or "is_active" not in header_map:
            return False
        cell = ws.find(schedule_id, in_column=col)
        if not cell:
            return False
        ws.update_cell(cell.row, header_map["is_active"], "0")
        return True
    except Exception as e:
        logger.warning("[SCHEDULED_BROADCAST] set_scheduled_broadcast_inactive: %s", e, exc_info=True)
        return False


def read_active_scheduled_broadcasts_for_list() -> List[Dict[str, Any]]:
    """Читает все активные плановые рассылки (is_active=1) для списка в боте."""
    if not STATS_SHEET_ID:
        return []
    try:
        ws = _get_ws(SCHEDULED_BROADCASTS_TAB)
        header_map = _get_headers(ws)
        values = ws.get_all_values()
        if len(values) < 2:
            return []

        result = []
        for row in values[1:]:
            if "is_active" not in header_map:
                continue
            idx = header_map["is_active"] - 1
            if idx >= len(row):
                continue
            is_active = str(row[idx]).strip().lower()
            if is_active not in ("1", "true"):
                continue
            record = {}
            for h, col in header_map.items():
                if col - 1 < len(row):
                    record[h] = row[col - 1]
            result.append(record)
        return result
    except Exception as e:
        logger.warning("[SCHEDULED_BROADCAST] read_active_scheduled_broadcasts_for_list: %s", e, exc_info=True)
        return []
