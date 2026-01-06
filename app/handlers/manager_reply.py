"""Хендлеры для менеджеров: кнопка 'Ответить' и отправка ответа пользователю (через reply)."""

import asyncio
import inspect
import json
import logging
import re
from datetime import datetime
from typing import Optional, Any, List, Dict

from aiogram import Router, F
from aiogram.enums import ParseMode, ChatAction
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.types import CallbackQuery, Message, ForceReply

from app.config import MANAGER_CHAT_ID, SHEET_ID  # SHEET_ID — FAQ-таблица
from app.services.pending_questions_service import get_ticket, update_ticket_fields
from app.services.metrics_service import log_event
from app.services.sheets_client import get_sheets_client
from app.services.faq_service import add_faq_entry_to_cache

logger = logging.getLogger(__name__)

FAQ_SHEET_NAME = "Sheet1"  # ← поменяй, если у тебя FAQ в другом листе

router = Router()

TICKET_RE = re.compile(r"Ticket:\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _manager_chat_id_int() -> Optional[int]:
    """MANAGER_CHAT_ID может приходить строкой из env — приводим к int."""
    if not MANAGER_CHAT_ID:
        return None
    try:
        return int(MANAGER_CHAT_ID)
    except Exception:
        logger.exception("MANAGER_CHAT_ID is not int-like: %r", MANAGER_CHAT_ID)
        return None


def _extract_ticket_id(text: str) -> Optional[str]:
    if not text:
        return None
    m = TICKET_RE.search(text)
    return m.group(1) if m else None


async def _maybe_await(result: Any) -> Any:
    """Поддержка sync/async функций сервиса."""
    if inspect.isawaitable(result):
        return await result
    return result


def _append_faq_to_sheet_sync(question: str, answer: str, media_json: str = "") -> None:
    """
    Добавляет новую пару Q/A в FAQ-таблицу.
    Предполагаем, что вопросы в колонке C, ответы в D, media_json в E (если есть).
    """
    if not SHEET_ID:
        return

    client = get_sheets_client()
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet(FAQ_SHEET_NAME)

    # Проверяем наличие колонки media_json
    headers = ws.row_values(1)
    has_media_json = "media_json" in [h.strip() for h in headers]

    # Пишем в C/D, оставляя A/B пустыми
    row = ["", "", question, answer]
    if has_media_json:
        row.append(media_json or "")

    ws.append_row(row, value_input_option="RAW")


# --- ФИЛЬТРЫ НА УРОВНЕ РОУТЕРА: только менеджерский чат ---
_mgr_chat = _manager_chat_id_int()
if _mgr_chat:
    router.message.filter(F.chat.id == _mgr_chat)
    router.callback_query.filter(F.message.chat.id == _mgr_chat)
else:
    # Если вдруг не настроен MANAGER_CHAT_ID — логируем, чтобы сразу видно в Railway
    logger.warning("MANAGER_CHAT_ID is empty or invalid. Manager handlers will not be chat-restricted.")


@router.callback_query(F.data.startswith("mgr_reply:"))
async def on_manager_reply_click(callback: CallbackQuery) -> None:
    """Менеджер нажал кнопку 'Ответить' под тикетом."""
    if not callback.message:
        await callback.answer()
        return

    ticket_id = callback.data.split("mgr_reply:", 1)[1].strip()
    if not ticket_id:
        await callback.answer("Не вижу ticket_id", show_alert=True)
        return

    logger.info("[MANAGER_REPLY] click mgr_reply ticket_id=%s chat_id=%s", ticket_id, callback.message.chat.id)

    ticket = await _maybe_await(get_ticket(ticket_id))
    if not ticket:
        await callback.answer("Тикет не найден в таблице", show_alert=True)
        return

    question = str(ticket.get("question", "")).strip()

    await callback.message.answer(
        "✍️ Напишите ответ одним сообщением и обязательно ответьте на ЭТО сообщение.\n"
        f"Ticket: {ticket_id}\n\n"
        f"Вопрос:\n{question}",
        reply_markup=ForceReply(selective=True),
    )

    await _maybe_await(
        log_event(
            user_id=callback.from_user.id,
            username=callback.from_user.username,
            event="manager_reply_click",
            meta={"ticket_id": ticket_id},
        )
    )

    await callback.answer()


def _extract_media_attachments(message: Message) -> List[Dict[str, Any]]:
    """Извлекает медиа-вложения из сообщения."""
    attachments = []
    
    if message.photo:
        # Берем самое большое фото
        photo = message.photo[-1]
        attachments.append({
            "type": "photo",
            "file_id": photo.file_id,
            "caption": message.caption or "",
        })
    elif message.video:
        attachments.append({
            "type": "video",
            "file_id": message.video.file_id,
            "caption": message.caption or "",
        })
    elif message.document:
        attachments.append({
            "type": "document",
            "file_id": message.document.file_id,
            "caption": message.caption or "",
        })
    
    return attachments


@router.message(F.reply_to_message)
async def on_manager_text(message: Message) -> None:
    """
    Ловим ответ менеджера ТОЛЬКО если это reply на сообщение бота с Ticket: ...
    Поддерживает текстовые сообщения и медиа (photo/video/document).
    """
    logger.info(
        "[MANAGER_REPLY] HIT on_manager_text chat_id=%s from=%s content_type=%s",
        message.chat.id,
        message.from_user.id if message.from_user else None,
        message.content_type,
    )

    src_text = (message.reply_to_message.text or "") if message.reply_to_message else ""
    ticket_id = _extract_ticket_id(src_text)
    if not ticket_id:
        logger.info("[MANAGER_REPLY] reply_to_message has no Ticket: ... ; skip")
        return

    logger.info("[MANAGER_REPLY] ticket_id=%s", ticket_id)

    # Извлекаем текст и медиа
    answer_text = (message.text or message.caption or "").strip()
    attachments = _extract_media_attachments(message)
    
    # Проверяем, что есть хотя бы текст или медиа
    if not answer_text and not attachments:
        await message.reply("Ответ пустой — отправь текстом или медиа 🙏")
        return

    ticket = await _maybe_await(get_ticket(ticket_id))
    if not ticket:
        await message.reply("Тикет не найден в таблице, попробуй ещё раз.")
        return

    user_id_raw = ticket.get("user_id", "")
    try:
        user_id = int(str(user_id_raw).strip())
    except Exception:
        await message.reply("Не могу прочитать user_id из тикета.")
        return

    # Формируем JSON для медиа-вложений
    media_json_str = ""
    if attachments:
        media_json_str = json.dumps(attachments, ensure_ascii=False)

    # Пытаемся отправить пользователю (и ЛОВИМ ошибки!)
    try:
        await message.bot.send_chat_action(user_id, ChatAction.TYPING)
        await asyncio.sleep(0.2)

        # Отправляем текст (если есть)
        if answer_text:
            user_message = (
                "✅ <b>Менеджер ответил на ваш вопрос</b>\n\n"
                f"📝 <b>Вопрос:</b>\n{ticket.get('question','')}\n\n"
                f"💬 <b>Ответ:</b>\n{answer_text}"
            )
            await message.bot.send_message(chat_id=user_id, text=user_message, parse_mode=ParseMode.HTML)
        elif attachments:
            # Если только медиа без текста, отправляем заголовок
            user_message = (
                "✅ <b>Менеджер ответил на ваш вопрос</b>\n\n"
                f"📝 <b>Вопрос:</b>\n{ticket.get('question','')}\n\n"
                f"💬 <b>Ответ:</b>"
            )
            await message.bot.send_message(chat_id=user_id, text=user_message, parse_mode=ParseMode.HTML)

        # Отправляем медиа-вложения
        for att in attachments:
            file_id = att["file_id"]
            caption = att.get("caption", "")
            # Используем caption только если нет текста выше или если это единственное сообщение
            use_caption = caption if (not answer_text or len(attachments) == 1) else None

            if att["type"] == "photo":
                await message.bot.send_photo(chat_id=user_id, photo=file_id, caption=use_caption, parse_mode=ParseMode.HTML if use_caption else None)
            elif att["type"] == "video":
                await message.bot.send_video(chat_id=user_id, video=file_id, caption=use_caption, parse_mode=ParseMode.HTML if use_caption else None)
            elif att["type"] == "document":
                await message.bot.send_document(chat_id=user_id, document=file_id, caption=use_caption, parse_mode=ParseMode.HTML if use_caption else None)

    except TelegramForbiddenError:
        await message.reply("❌ Не смог отправить: пользователь не нажал Start или заблокировал бота.")
        updates = {
            "status": "answered_not_delivered",
            "manager_answer": answer_text or "",
            "answered_at": _now(),
        }
        if media_json_str:
            updates["manager_media_json"] = media_json_str
        await _maybe_await(update_ticket_fields(ticket_id, updates))
        return

    except TelegramBadRequest as e:
        await message.reply(f"❌ Ошибка отправки пользователю: {e}")
        return

    except Exception as e:
        logger.exception("[MANAGER_REPLY] Unexpected error sending to user")
        await message.reply(f"❌ Неожиданная ошибка при отправке пользователю: {e}")
        return

    # Обновляем тикет (успешная доставка)
    updates = {
        "status": "answered",
        "manager_answer": answer_text or "",
        "answered_by": (
            f"{message.from_user.full_name} (@{message.from_user.username})"
            if message.from_user and message.from_user.username
            else (message.from_user.full_name if message.from_user else "manager")
        ),
        "answered_at": _now(),
    }
    if media_json_str:
        updates["manager_media_json"] = media_json_str
        # Логируем событие сохранения медиа
        await _maybe_await(
            log_event(
                user_id=user_id,
                username=ticket.get("username"),
                event="manager_media_saved",
                meta={"ticket_id": ticket_id, "attachments_count": len(attachments)},
            )
        )

    await _maybe_await(update_ticket_fields(ticket_id, updates))

    await _maybe_await(
        log_event(
            user_id=user_id,
            username=ticket.get("username"),
            event="pending_answer_written",
            meta={"ticket_id": ticket_id},
        )
    )

    # Пишем в FAQ (в отдельном потоке, чтобы не блокировать бота)
    # Берём media_json из тикета (если есть)
    ticket_media_json = ticket.get("manager_media_json", "") or media_json_str
    try:
        await asyncio.to_thread(_append_faq_to_sheet_sync, ticket.get("question", ""), answer_text or "", ticket_media_json)
        await add_faq_entry_to_cache(ticket.get("question", ""), answer_text or "", ticket_media_json)
        await _maybe_await(update_ticket_fields(ticket_id, {"faq_written_at": _now()}))
        await _maybe_await(
            log_event(
                user_id=user_id,
                username=ticket.get("username"),
                event="faq_written_from_ticket",
                meta={"ticket_id": ticket_id},
            )
        )
    except Exception as e:
        logger.exception("[MANAGER_REPLY] FAQ write failed")
        await message.reply(f"⚠️ Ответ отправлен пользователю, но не смог записать в FAQ: {e}")
        return

    await message.reply(f"✅ Ответ отправлен пользователю и сохранён в FAQ. Ticket: <code>{ticket_id}</code>")