"""Сервис для хранения 'не найденных' вопросов в Google Sheets (pending_questions)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.config import STATS_SHEET_ID, PENDING_SHEET_TAB
from app.services.sheets_client import get_sheets_client


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_ws():
    if not STATS_SHEET_ID:
        raise RuntimeError("STATS_SHEET_ID не задан (он же используется для pending_questions)")
    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    return sh.worksheet(PENDING_SHEET_TAB)


def _get_headers(ws) -> Dict[str, int]:
    """Возвращает маппинг: имя колонки -> индекс (1-based)."""
    headers = ws.row_values(1)
    return {h.strip(): i + 1 for i, h in enumerate(headers) if str(h).strip()}


def create_ticket(
    *,
    user_id: int,
    username: Optional[str],
    name: Optional[str],
    phone: Optional[str],
    legal_entity: Optional[str],
    question: str,
) -> str:
    """
    Создаёт тикет со статусом open и возвращает ticket_id.
    """
    ws = _get_ws()

    ticket_id = uuid.uuid4().hex[:12]
    created_at = _utc_now_iso()
    status = "open"

    row = [
        ticket_id,
        created_at,
        status,
        str(user_id),
        username or "",
        name or "",
        phone or "",
        legal_entity or "",
        question,
        "",  # manager_answer
        "",  # answered_by
        "",  # answered_at
    ]

    ws.append_row(row, value_input_option="RAW")
    return ticket_id


def find_ticket_row(ticket_id: str) -> Optional[int]:
    """
    Находит строку тикета по ticket_id (колонка A).
    Возвращает номер строки или None.
    """
    ws = _get_ws()
    try:
        cell = ws.find(ticket_id)
        # убедимся, что нашли в первой колонке (ticket_id)
        if cell.col != 1:
            return None
        return cell.row
    except Exception:
        return None


def get_ticket(ticket_id: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает тикет как dict по заголовкам таблицы.
    """
    ws = _get_ws()
    row_num = find_ticket_row(ticket_id)
    if not row_num:
        return None

    headers = ws.row_values(1)
    values = ws.row_values(row_num)

    # выравниваем длины
    if len(values) < len(headers):
        values += [""] * (len(headers) - len(values))

    return {headers[i]: values[i] for i in range(len(headers))}


def update_ticket_fields(ticket_id: str, updates: Dict[str, Any]) -> bool:
    """
    Обновляет поля тикета по названиям колонок.
    updates: {"status": "answered", "manager_answer": "...", ...}
    """
    ws = _get_ws()
    row_num = find_ticket_row(ticket_id)
    if not row_num:
        return False

    header_map = _get_headers(ws)

    for key, val in updates.items():
        col = header_map.get(key)
        if not col:
            continue
        ws.update_cell(row_num, col, "" if val is None else str(val))

    return True

