"""Хендлеры для менеджеров: кнопка 'Ответить' и отправка ответа пользователю (через reply)."""

import asyncio
import inspect
import json
import logging
import re
from datetime import datetime
from typing import Optional, Any, List, Dict, Set

from aiogram import Router, F
from aiogram.enums import ParseMode, ChatAction
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.types import CallbackQuery, Message, ForceReply, InputMediaPhoto, InputMediaVideo

from app.config import MANAGER_CHAT_ID, SHEET_ID  # SHEET_ID — FAQ-таблица
from app.services.pending_questions_service import get_ticket, update_ticket_fields
from app.services.metrics_service import log_event
from app.services.sheets_client import get_sheets_client
from app.services.faq_service import add_faq_entry_to_cache

logger = logging.getLogger(__name__)

FAQ_SHEET_NAME = "Sheet1"  # ← поменяй, если у тебя FAQ в другом листе

router = Router()

TICKET_RE = re.compile(r"Ticket:\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)

# Буфер для агрегации альбомов: (media_group_id, ticket_id) -> список сообщений
_media_group_buffer: Dict[tuple[str, str], List[Message]] = {}
# Флаги обработки для защиты от дублей
_processing_groups: Set[tuple[str, str]] = set()


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
    Добавляет новую пару Q/A в FAQ-таблицу и в Qdrant.
    Предполагаем, что вопросы в колонке C, ответы в D, media_json в E (если есть).
    """
    if not SHEET_ID:
        return

    # Сохраняем в Google Sheets (для обратной совместимости)
    try:
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
    except Exception as e:
        logger.exception(f"[MANAGER_REPLY] Ошибка записи в Google Sheets: {e}")
    
    # Также сохраняем в Qdrant через faq_service
    try:
        import asyncio
        from app.services.faq_service import add_faq_entry_to_cache
        
        # Запускаем асинхронно, чтобы не блокировать
        asyncio.create_task(add_faq_entry_to_cache(question, answer, media_json))
    except Exception as e:
        logger.exception(f"[MANAGER_REPLY] Ошибка сохранения в Qdrant: {e}")


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


def _collect_album_attachments(messages: List[Message]) -> tuple[str, List[Dict[str, Any]]]:
    """Собирает текст и все вложения из альбома сообщений."""
    answer_text = ""
    all_attachments = []
    
    for msg in messages:
        # Берем первый найденный caption/text
        if not answer_text:
            answer_text = (msg.text or msg.caption or "").strip()
        
        # Собираем все вложения
        attachments = _extract_media_attachments(msg)
        all_attachments.extend(attachments)
    
    return answer_text, all_attachments


async def _send_media_to_user(bot, user_id: int, attachments: List[Dict[str, Any]], header_text: str = "") -> None:
    """Отправляет медиа пользователю: send_media_group для фото/видео, send_document для документов."""
    photos = [att for att in attachments if att["type"] == "photo"]
    videos = [att for att in attachments if att["type"] == "video"]
    documents = [att for att in attachments if att["type"] == "document"]
    
    # Отправляем заголовок (если есть)
    if header_text:
        await bot.send_message(chat_id=user_id, text=header_text, parse_mode=ParseMode.HTML)
    
    # Отправляем фото батчами по 10
    for i in range(0, len(photos), 10):
        batch = photos[i:i+10]
        media_group = []
        for idx, att in enumerate(batch):
            # Caption только у первого фото, если нет заголовка
            caption = att.get("caption", "") if idx == 0 and not header_text else None
            media_group.append(InputMediaPhoto(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None))
        if media_group:
            await bot.send_media_group(chat_id=user_id, media=media_group)
    
    # Отправляем видео батчами по 10
    for i in range(0, len(videos), 10):
        batch = videos[i:i+10]
        media_group = []
        for idx, att in enumerate(batch):
            # Caption только у первого видео, если нет заголовка
            caption = att.get("caption", "") if idx == 0 and not header_text else None
            media_group.append(InputMediaVideo(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None))
        if media_group:
            await bot.send_media_group(chat_id=user_id, media=media_group)
    
    # Отправляем документы по одному
    for att in documents:
        # Caption только если нет заголовка
        caption = att.get("caption", "") if not header_text else None
        await bot.send_document(
            chat_id=user_id,
            document=att["file_id"],
            caption=caption,
            parse_mode=ParseMode.HTML if caption else None
        )


async def _process_album(group_key: tuple[str, str], messages: List[Message]) -> None:
    """Обрабатывает собранный альбом: обновляет тикет, отправляет пользователю, пишет в FAQ."""
    media_group_id, ticket_id = group_key
    
    # Убираем из флага обработки
    _processing_groups.discard(group_key)
    
    # Удаляем из буфера
    _media_group_buffer.pop(group_key, None)
    
    ticket = await _maybe_await(get_ticket(ticket_id))
    if not ticket:
        logger.warning(f"[MANAGER_REPLY] Ticket {ticket_id} not found for album {media_group_id}")
        return
    
    # Защита от дублей: если тикет уже answered — не обрабатываем
    if ticket.get("status", "").strip().lower() == "answered":
        logger.info(f"[MANAGER_REPLY] Ticket {ticket_id} already answered, skipping")
        return
    
    # Собираем текст и все вложения
    answer_text, all_attachments = _collect_album_attachments(messages)
    
    # Проверяем, что есть хотя бы текст или медиа
    if not answer_text and not all_attachments:
        logger.warning(f"[MANAGER_REPLY] Album {media_group_id} has no text or media")
        return
    
    user_id_raw = ticket.get("user_id", "")
    try:
        user_id = int(str(user_id_raw).strip())
    except Exception:
        logger.error(f"[MANAGER_REPLY] Cannot parse user_id from ticket {ticket_id}")
        return
    
    # Формируем JSON для медиа-вложений
    media_json_str = ""
    if all_attachments:
        media_json_str = json.dumps(all_attachments, ensure_ascii=False)
    
    # Отправляем пользователю
    try:
        await messages[0].bot.send_chat_action(user_id, ChatAction.TYPING)
        await asyncio.sleep(0.2)
        
        if answer_text:
            user_message = (
                "✅ <b>Менеджер ответил на ваш вопрос</b>\n\n"
                f"📝 <b>Вопрос:</b>\n{ticket.get('question','')}\n\n"
                f"💬 <b>Ответ:</b>\n{answer_text}"
            )
        else:
            user_message = (
                "✅ <b>Менеджер ответил на ваш вопрос</b>\n\n"
                f"📝 <b>Вопрос:</b>\n{ticket.get('question','')}\n\n"
                f"💬 <b>Ответ:</b>"
            )
        
        await _send_media_to_user(messages[0].bot, user_id, all_attachments, user_message)
        
    except TelegramForbiddenError:
        await messages[0].reply("❌ Не смог отправить: пользователь не нажал Start или заблокировал бота.")
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
        await messages[0].reply(f"❌ Ошибка отправки пользователю: {e}")
        return
    
    except Exception as e:
        logger.exception("[MANAGER_REPLY] Unexpected error sending to user")
        await messages[0].reply(f"❌ Неожиданная ошибка при отправке пользователю: {e}")
        return
    
    # Обновляем тикет (успешная доставка)
    updates = {
        "status": "answered",
        "manager_answer": answer_text or "",
        "answered_by": (
            f"{messages[0].from_user.full_name} (@{messages[0].from_user.username})"
            if messages[0].from_user and messages[0].from_user.username
            else (messages[0].from_user.full_name if messages[0].from_user else "manager")
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
                meta={"ticket_id": ticket_id, "attachments_count": len(all_attachments)},
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
    
    # Пишем в FAQ (с антидубликатом)
    ticket_media_json = ticket.get("manager_media_json", "") or media_json_str
    
    # Проверяем, не записан ли уже в FAQ
    faq_written = ticket.get("faq_written", "").strip()
    if faq_written and faq_written.lower() in ("1", "true", "yes", "да"):
        logger.info(f"[MANAGER_REPLY] Ticket {ticket_id} already written to FAQ, skipping")
    else:
        try:
            await asyncio.to_thread(_append_faq_to_sheet_sync, ticket.get("question", ""), answer_text or "", ticket_media_json)
            await add_faq_entry_to_cache(ticket.get("question", ""), answer_text or "", ticket_media_json)
            
            # Ставим флаг faq_written=1
            await _maybe_await(update_ticket_fields(ticket_id, {"faq_written": "1", "faq_written_at": _now()}))
            
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
            await messages[0].reply(f"⚠️ Ответ отправлен пользователю, но не смог записать в FAQ: {e}")
            return
    
    await messages[0].reply(f"✅ Ответ отправлен пользователю и сохранён в FAQ. Ticket: <code>{ticket_id}</code>")


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


@router.message(F.reply_to_message)
async def on_manager_text(message: Message) -> None:
    """
    Ловим ответ менеджера ТОЛЬКО если это reply на сообщение бота с Ticket: ...
    Поддерживает текстовые сообщения и медиа (photo/video/document).
    Обрабатывает альбомы через агрегацию по media_group_id.
    """
    logger.info(
        "[MANAGER_REPLY] HIT on_manager_text chat_id=%s from=%s content_type=%s media_group_id=%s",
        message.chat.id,
        message.from_user.id if message.from_user else None,
        message.content_type,
        message.media_group_id,
    )

    src_text = (message.reply_to_message.text or "") if message.reply_to_message else ""
    ticket_id = _extract_ticket_id(src_text)
    if not ticket_id:
        logger.info("[MANAGER_REPLY] reply_to_message has no Ticket: ... ; skip")
        return

    logger.info("[MANAGER_REPLY] ticket_id=%s media_group_id=%s", ticket_id, message.media_group_id)

    # Если это часть альбома (есть media_group_id)
    if message.media_group_id:
        group_key = (str(message.media_group_id), ticket_id)
        
        # Добавляем сообщение в буфер
        if group_key not in _media_group_buffer:
            _media_group_buffer[group_key] = []
        _media_group_buffer[group_key].append(message)
        
        # Если уже обрабатываем этот альбом — пропускаем
        if group_key in _processing_groups:
            logger.info(f"[MANAGER_REPLY] Album {message.media_group_id} already processing, skipping")
            return
        
        # Запускаем обработку с debounce
        _processing_groups.add(group_key)
        asyncio.create_task(_process_album_with_debounce(group_key, ticket_id))
        return
    
    # Обычное сообщение (не альбом) — обрабатываем сразу
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

    # Защита от дублей: если тикет уже answered — не обрабатываем
    if ticket.get("status", "").strip().lower() == "answered":
        await message.reply("Тикет уже обработан.")
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

        if answer_text:
            user_message = (
                "✅ <b>Менеджер ответил на ваш вопрос</b>\n\n"
                f"📝 <b>Вопрос:</b>\n{ticket.get('question','')}\n\n"
                f"💬 <b>Ответ:</b>\n{answer_text}"
            )
        else:
            user_message = (
                "✅ <b>Менеджер ответил на ваш вопрос</b>\n\n"
                f"📝 <b>Вопрос:</b>\n{ticket.get('question','')}\n\n"
                f"💬 <b>Ответ:</b>"
            )
        
        await _send_media_to_user(message.bot, user_id, attachments, user_message)

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

    # Пишем в FAQ (с антидубликатом)
    ticket_media_json = ticket.get("manager_media_json", "") or media_json_str
    
    # Проверяем, не записан ли уже в FAQ
    faq_written = ticket.get("faq_written", "").strip()
    if faq_written and faq_written.lower() in ("1", "true", "yes", "да"):
        logger.info(f"[MANAGER_REPLY] Ticket {ticket_id} already written to FAQ, skipping")
    else:
        try:
            await asyncio.to_thread(_append_faq_to_sheet_sync, ticket.get("question", ""), answer_text or "", ticket_media_json)
            await add_faq_entry_to_cache(ticket.get("question", ""), answer_text or "", ticket_media_json)
            
            # Ставим флаг faq_written=1
            await _maybe_await(update_ticket_fields(ticket_id, {"faq_written": "1", "faq_written_at": _now()}))
            
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


async def _process_album_with_debounce(group_key: tuple[str, str], ticket_id: str) -> None:
    """Обрабатывает альбом с debounce 1.2 сек."""
    await asyncio.sleep(1.2)
    
    if group_key not in _media_group_buffer:
        _processing_groups.discard(group_key)
        return
    
    messages = _media_group_buffer[group_key]
    if not messages:
        _processing_groups.discard(group_key)
        _media_group_buffer.pop(group_key, None)
        return
    
    await _process_album(group_key, messages)
