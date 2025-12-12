"""Хендлеры для менеджеров: кнопка 'Ответить' и отправка ответа пользователю (через reply)."""

import asyncio
import re
from datetime import datetime
from typing import Optional

from aiogram import Router, F
from aiogram.enums import ParseMode, ChatAction
from aiogram.types import CallbackQuery, Message, ForceReply

from app.config import MANAGER_CHAT_ID, SHEET_ID  # SHEET_ID — FAQ-таблица
from app.services.pending_questions_service import get_ticket, update_ticket_fields
from app.services.metrics_service import log_event
from app.services.sheets_client import get_sheets_client

FAQ_SHEET_NAME = "Sheet1"  # ← поменяй, если у тебя FAQ в другом листе

router = Router()

TICKET_RE = re.compile(r"Ticket:\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _extract_ticket_id(text: str) -> Optional[str]:
    if not text:
        return None
    m = TICKET_RE.search(text)
    return m.group(1) if m else None


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

    # Пишем в C/D, оставляя A/B пустыми
    ws.append_row(["", "", question, answer], value_input_option="RAW")


@router.callback_query(F.data.startswith("mgr_reply:"))
async def on_manager_reply_click(callback: CallbackQuery) -> None:
    """Менеджер нажал кнопку 'Ответить' под тикетом."""
    if not callback.message:
        await callback.answer()
        return

    if MANAGER_CHAT_ID and callback.message.chat.id != int(MANAGER_CHAT_ID):
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

    question = str(ticket.get("question", "")).strip()

    # ВАЖНО: ForceReply — менеджер должен ответить reply на это сообщение
    await callback.message.answer(
        "✍️ Напишите ответ одним сообщением и обязательно ответьте на ЭТО сообщение.\n"
        f"Ticket: {ticket_id}\n\n"
        f"Вопрос:\n{question}",
        reply_markup=ForceReply(selective=True),
    )

    log_event(
        user_id=callback.from_user.id,
        username=callback.from_user.username,
        event="manager_reply_click",
        meta={"ticket_id": ticket_id},
    )

    await callback.answer()


@router.message(F.text)
async def on_manager_text(message: Message) -> None:
    """
    Ловим ответ менеджера в группе ТОЛЬКО если это reply на сообщение бота с Ticket: ...
    Так работает даже при включённой приватности бота.
    """
    if MANAGER_CHAT_ID and message.chat.id != int(MANAGER_CHAT_ID):
        return

    # Должен быть reply на сообщение
    if not message.reply_to_message or not message.reply_to_message.text:
        return

    ticket_id = _extract_ticket_id(message.reply_to_message.text)
    if not ticket_id:
        return

    answer_text = (message.text or "").strip()
    if not answer_text:
        await message.reply("Ответ пустой — отправь текстом 🙏")
        return

    ticket = get_ticket(ticket_id)
    if not ticket:
        await message.reply("Тикет не найден в таблице, попробуй ещё раз.")
        return

    user_id_raw = ticket.get("user_id", "")
    try:
        user_id = int(str(user_id_raw).strip())
    except Exception:
        await message.reply("Не могу прочитать user_id из тикета.")
        return

    # Обновляем тикет
    update_ticket_fields(
        ticket_id,
        {
            "status": "answered",
            "manager_answer": answer_text,
            "answered_by": (
                f"{message.from_user.full_name} (@{message.from_user.username})"
                if message.from_user.username
                else message.from_user.full_name
            ),
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

    # Пишем в FAQ-таблицу
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
        await message.reply(f"⚠️ Ответ отправлен, но не смог записать в FAQ: {e}")
        return

    await message.reply(f"✅ Ответ отправлен пользователю и сохранён в FAQ. Ticket: <code>{ticket_id}</code>")