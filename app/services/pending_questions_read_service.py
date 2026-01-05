"""Сервис для чтения pending_questions (только чтение, для отчётов)."""

from typing import Optional

from app.config import STATS_SHEET_ID, PENDING_SHEET_TAB
from app.services.sheets_client import get_sheets_client


def read_pending_open_count() -> int:
    """
    Читает лист pending_questions и считает тикеты со статусом "open" (или пустой статус).
    Возвращает количество открытых тикетов.
    """
    if not STATS_SHEET_ID:
        return 0

    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    ws = sh.worksheet(PENDING_SHEET_TAB)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return 0

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers) if h}

    def col(name: str) -> Optional[int]:
        return idx.get(name)

    status_i = col("status")

    # Если колонка status не найдена, считаем что все тикеты открыты (консервативно)
    if status_i is None:
        return len(values) - 1  # минус заголовок

    count = 0
    for r in values[1:]:
        def get(i: int) -> str:
            return (r[i] if i < len(r) else "").strip()

        status = get(status_i).lower() if status_i is not None else ""
        # Считаем открытыми: "open" или пустой статус
        if not status or status == "open":
            count += 1

    return count

