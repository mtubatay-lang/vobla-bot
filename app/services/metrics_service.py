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

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers) if h}

    def col(name: str) -> Optional[int]:
        return idx.get(name)

    ts_i = col("ts")
    d_i = col("date")
    uid_i = col("user_id")
    uname_i = col("username")
    ev_i = col("event")
    meta_i = col("meta_json")

    # если вдруг таблица не по ожидаемым заголовкам — лучше явно не врать отчётами
    required = [ts_i, d_i, uid_i, uname_i, ev_i, meta_i]
    if any(i is None for i in required):
        return []

    out: List[Dict[str, Any]] = []
    for r in values[1:]:
        def get(i: int) -> str:
            return (r[i] if i < len(r) else "").strip()

        d_raw = get(d_i)
        if not d_raw:
            continue

        # нормализуем дату в ISO day: YYYY-MM-DD
        d = d_raw[:10].strip()

        if d < date_from or d > date_to:
            continue

        meta_json = get(meta_i)
        meta: Dict[str, Any] = {}
        if meta_json:
            try:
                meta = json.loads(meta_json)
            except Exception:
                meta = {"_raw": meta_json}

        out.append(
            {
                "ts": get(ts_i),
                "date": d,
                "user_id": get(uid_i),
                "username": get(uname_i),
                "event": get(ev_i),
                "meta": meta,
            }
        )

    return out