"""Логирование событий бота в Google Sheets (лист bot_stats)."""

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.config import STATS_SHEET_ID, STATS_SHEET_TAB
from app.services.sheets_client import get_sheets_client


def _now_ts_iso() -> str:
    # ISO-формат, удобно читать и сортировать
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today_date() -> str:
    # Дата для группировки (UTC). Если хочешь МСК — скажи, поменяем.
    return datetime.now(timezone.utc).date().isoformat()


def log_event(
    *,
    user_id: Optional[int],
    username: Optional[str],
    event: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Пишем строку в bot_stats:
    ts | date | user_id | username | event | meta_json
    """
    if not STATS_SHEET_ID:
        return  # не настроено — молча пропускаем

    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    ws = sh.worksheet(STATS_SHEET_TAB)

    meta_json = ""
    if meta:
        meta_json = json.dumps(meta, ensure_ascii=False)

    row = [
        _now_ts_iso(),
        _today_date(),
        str(user_id) if user_id is not None else "",
        username or "",
        event,
        meta_json,
    ]

    ws.append_row(row, value_input_option="RAW")

