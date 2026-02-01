"""Сервис для хранения 'не найденных' вопросов в Google Sheets (pending_questions)."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

from aiogram.enums import ParseMode
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from app.config import STATS_SHEET_ID, PENDING_SHEET_TAB, MANAGER_CHAT_ID
from app.services.sheets_client import get_sheets_client


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _manager_chat_id_int() -> Optional[int]:
    if not MANAGER_CHAT_ID:
        return None
    try:
        return int(MANAGER_CHAT_ID)
    except Exception as e:
        logger.warning("[PENDING_QUESTIONS] _manager_chat_id_int: %s", e, exc_info=True)
        return None


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

    # Проверяем наличие колонки manager_media_json
    headers = ws.row_values(1)
    has_manager_media_json = "manager_media_json" in [h.strip() for h in headers]

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

    # Добавляем пустое значение для manager_media_json, если колонка есть
    if has_manager_media_json:
        row.append("")  # manager_media_json

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
        if cell.col != 1:
            return None
        return cell.row
    except Exception as e:
        logger.warning("[PENDING_QUESTIONS] find_ticket_row(%s): %s", ticket_id, e, exc_info=True)
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


def _manager_reply_keyboard(ticket_id: str) -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой 'Ответить' для менеджеров."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✍️ Ответить",
                    callback_data=f"mgr_reply:{ticket_id}",
                )
            ]
        ]
    )


def _format_manager_card(
    *,
    ticket_id: str,
    user_id: int,
    username: Optional[str],
    full_name: str,
    phone: str,
    legal_entity: str,
    question: str,
) -> str:
    """Форматирует карточку вопроса для менеджеров."""
    u = f"@{username}" if username else "—"
    phone_txt = phone or "—"
    le_txt = legal_entity or "—"
    return (
        "❓ <b>Новый вопрос от франчайзи</b>\n\n"
        f"🧾 Ticket: <code>{ticket_id}</code>\n"
        f"👤 Пользователь: {full_name} ({u})\n"
        f"🆔 User ID: <code>{user_id}</code>\n"
        f"📞 Телефон: <code>{phone_txt}</code>\n"
        f"🏢 Юр. лицо: <b>{le_txt}</b>\n\n"
        f"📝 Вопрос:\n{question}"
    )


async def create_ticket_and_notify_managers(message: Message, question: str) -> str:
    """
    Создаёт тикет и отправляет уведомление менеджерам.
    Используется в режиме навыка (qa_mode).
    Возвращает ticket_id.
    """
    import asyncio
    from app.services.auth_service import find_user_by_telegram_id
    from app.services.metrics_service import log_event

    user_id = message.from_user.id if message.from_user else 0
    username = message.from_user.username if message.from_user else None
    full_name = message.from_user.full_name if message.from_user else "Unknown"

    auth_user = find_user_by_telegram_id(user_id)

    ticket_id = create_ticket(
        user_id=user_id,
        username=username,
        name=(auth_user.name if auth_user else full_name),
        phone=(auth_user.phone if auth_user else ""),
        legal_entity=(auth_user.legal_entity if auth_user else ""),
        question=question,
    )

    # Логируем событие (в поток, чтобы не блокировать loop)
    await asyncio.to_thread(
        log_event,
        user_id=user_id,
        username=username,
        event="ticket_created",
        meta={"ticket_id": ticket_id},
    )

    # Уведомляем менеджеров
    mgr_chat = _manager_chat_id_int()
    if mgr_chat:
        manager_text = _format_manager_card(
            ticket_id=ticket_id,
            user_id=user_id,
            username=username,
            full_name=(auth_user.name if auth_user else full_name),
            phone=(auth_user.phone if auth_user else ""),
            legal_entity=(auth_user.legal_entity if auth_user else ""),
            question=question,
        )

        await message.bot.send_message(
            chat_id=mgr_chat,
            text=manager_text,
            reply_markup=_manager_reply_keyboard(ticket_id),
            parse_mode=ParseMode.HTML,
        )

    return ticket_id