"""Логирование событий бота в Google Sheets (лист bot_stats) + чтение событий для отчётов."""

import json
import asyncio
from datetime import datetime, timezone, date
from typing import Any, Dict, Optional, List

from app.config import STATS_SHEET_ID, STATS_SHEET_TAB
from app.services.sheets_client import get_sheets_client


def _now_ts_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today_date() -> str:
    # Дата для группировки (UTC). Если захочешь МСК — поменяем на zoneinfo.
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
        return

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


async def alog_event(
    *,
    user_id: Optional[int],
    username: Optional[str],
    event: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Async-обёртка, чтобы не блокировать event loop (gspread синхронный)."""
    await asyncio.to_thread(
        log_event,
        user_id=user_id,
        username=username,
        event=event,
        meta=meta,
    )


def read_events_by_dates(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """
    Читает события из bot_stats за диапазон включительно.
    date_from/date_to: 'YYYY-MM-DD'

    Возвращает список dict:
      {ts, date, user_id, username, event, meta}
    """
    if not STATS_SHEET_ID:
        return []

    client = get_sheets_client()
    sh = client.open_by_key(STATS_SHEET_ID)
    ws = sh.worksheet(STATS_SHEET_TAB)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    # ожидаем заголовок в первой строке
    rows = values[1:]

    out: List[Dict[str, Any]] = []
    for r in rows:
        # ts | date | user_id | username | event | meta_json
        ts = r[0] if len(r) > 0 else ""
        d = r[1] if len(r) > 1 else ""
        uid = r[2] if len(r) > 2 else ""
        uname = r[3] if len(r) > 3 else ""
        ev = r[4] if len(r) > 4 else ""
        meta_json = r[5] if len(r) > 5 else ""

        if not d:
            continue

        # фильтр по диапазону дат строкой (ISO сортируемый)
        if d < date_from or d > date_to:
            continue

        meta: Dict[str, Any] = {}
        if meta_json:
            try:
                meta = json.loads(meta_json)
            except Exception:
                meta = {"_raw": meta_json}

        out.append(
            {
                "ts": ts,
                "date": d,
                "user_id": uid,
                "username": uname,
                "event": ev,
                "meta": meta,
            }
        )

    return out