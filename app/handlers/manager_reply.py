"""Хендлеры для менеджеров: кнопка 'Ответить' и отправка ответа пользователю."""

import asyncio
from datetime import datetime
from typing import Dict, Optional

from aiogram import Router, F
from aiogram.enums import ParseMode, ChatAction
from aiogram.types import CallbackQuery, Message

from app.config import MANAGER_CHAT_ID
from app.services.pending_questions_service import get_ticket, update_ticket_fields
from app.services.metrics_service import log_event
from app.services.sheets_client import get_sheets_client
from app.config import SHEET_ID  # это твоя FAQ-таблица

# Важно: ниже укажем лист, куда писать FAQ (см. комментарий)
FAQ_SHEET_NAME = "Sheet1"  # ← поменяй, если у тебя FAQ в другом листе

router = Router()

# ожидаем ответ менеджера: manager_user_id -> ticket_id
PENDING_MANAGER_REPLY: Dict[int, str] = {}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _append_faq_to_sheet(question: str, answer: str) -> None:
    """
    Добавляет новую пару Q/A в FAQ-таблицу.
    Предполагаем, что вопросы в колонке C, ответы в D.
    """
    if not SHEET_ID:
        return

    client = get_sheets_client()
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet(FAQ_SHEET_NAME)

    # Добавляем в конец строки: C=question, D=answer
    # Если в твоём листе есть колонки A/B — это ок, мы запишем только C/D.
    # Надёжнее писать через append_row полным рядом:
    ws.append_row(["", "", question, answer], value_input_option="RAW")


@router.callback_query(F.data.startswith("mgr_reply:"))
async def on_manager_reply_click(callback: CallbackQuery) -> None:
    """Менеджер нажал кнопку 'Ответить' под тикетом."""
    if MANAGER_CHAT_ID and callback.message and callback.message.chat.id != int(MANAGER_CHAT_ID):
        # кнопка нажата не в менеджерской группе — игнор
        await callback.answer("Эта кнопка доступна только менеджерам.", show_alert=True)
        return

    ticket_id = callback.data.split("mgr_reply:", 1)[1].strip()

    if not ticket_id:
        await callback.answer("Не вижу ticket_id", show_alert=True)
        return

    ticket = get_ticket(ticket_id)
    if not ticket:
        await callback.answer("Тикет не найден в таблице", show_alert=True)
        return

    # Запоминаем, что от этого менеджера ждём следующее сообщение как ответ
    PENDING_MANAGER_REPLY[callback.from_user.id] = ticket_id

    # Подсказываем менеджеру
    await callback.message.answer(
        "✍️ Напишите ответ одним сообщением (следующее ваше сообщение будет отправлено франчайзи).\n"
        f"Ticket: <code>{ticket_id}</code>\n\n"
        f"Вопрос:\n{ticket.get('question','')}"
    )

    await callback.answer()


@router.message(F.text)
async def on_manager_text(message: Message) -> None:
    """
    Ловим текст менеджера в группе.
    Если от этого менеджера ждём ответ — считаем это ответом.
    """
    if MANAGER_CHAT_ID and message.chat.id != int(MANAGER_CHAT_ID):
        return

    manager_id = message.from_user.id

    if manager_id not in PENDING_MANAGER_REPLY:
        return

    ticket_id = PENDING_MANAGER_REPLY.pop(manager_id)

    answer_text = (message.text or "").strip()

    if not answer_text:
        await message.answer("Ответ пустой — отправь текстом 🙏")
        PENDING_MANAGER_REPLY[manager_id] = ticket_id
        return

    ticket = get_ticket(ticket_id)
    if not ticket:
        await message.answer("Тикет не найден в таблице, попробуй ещё раз.")
        return

    user_id_raw = ticket.get("user_id", "")
    try:
        user_id = int(str(user_id_raw).strip())
    except Exception:
        await message.answer("Не могу прочитать user_id из тикета.")
        return

    # Пишем в тикет ответ/кто/когда
    update_ticket_fields(
        ticket_id,
        {
            "status": "answered",
            "manager_answer": answer_text,
            "answered_by": f"{message.from_user.full_name} (@{message.from_user.username})" if message.from_user.username else message.from_user.full_name,
            "answered_at": _now(),
        },
    )

    log_event(
        user_id=user_id,
        username=ticket.get("username"),
        event="pending_answer_written",
        meta={"ticket_id": ticket_id},
    )

    # Отправляем ответ пользователю
    await message.bot.send_chat_action(user_id, ChatAction.TYPING)
    await asyncio.sleep(0.8)

    user_message = (
        "✅ <b>Менеджер ответил на ваш вопрос</b>\n\n"
        f"📝 <b>Вопрос:</b>\n{ticket.get('question','')}\n\n"
        f"💬 <b>Ответ:</b>\n{answer_text}"
    )

    await message.bot.send_message(chat_id=user_id, text=user_message, parse_mode=ParseMode.HTML)

    # Пишем в FAQ-таблицу (чтобы в следующий раз находилось)
    try:
        _append_faq_to_sheet(ticket.get("question", ""), answer_text)
        update_ticket_fields(ticket_id, {"faq_written_at": _now()})
        log_event(
            user_id=user_id,
            username=ticket.get("username"),
            event="faq_written_from_ticket",
            meta={"ticket_id": ticket_id},
        )
    except Exception as e:
        # Не падаем, просто сообщаем менеджеру
        await message.answer(f"⚠️ Не смог записать в FAQ-таблицу: {e}")

    await message.answer(f"✅ Ответ отправлен пользователю и сохранён в FAQ. Ticket: <code>{ticket_id}</code>")

