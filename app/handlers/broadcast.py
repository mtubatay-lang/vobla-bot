"""Хендлеры для рассылок (broadcast) админам."""

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from aiogram import Router, F
from aiogram.enums import ParseMode, ChatAction
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)

from app.services.auth_service import find_user_by_telegram_id
from app.services.broadcast_service import (
    create_broadcast_draft,
    finalize_broadcast,
    log_broadcast_recipient,
    mark_chat_failed,
    mark_user_failed,
    read_active_recipients_chats,
    read_active_recipients_users,
)
from app.services.metrics_service import log_event
from app.services.openai_client import improve_broadcast_text

logger = logging.getLogger(__name__)

router = Router()

# Буфер для агрегации альбомов: (media_group_id, user_id) -> список сообщений
_media_group_buffer: Dict[tuple[str, int], List[Message]] = {}
# Флаги обработки для защиты от дублей
_processing_groups: Set[tuple[str, int]] = set()


class BroadcastState(StatesGroup):
    waiting_text = State()
    waiting_media = State()
    waiting_choice = State()


def _check_admin(user) -> bool:
    """Проверяет, является ли пользователь админом."""
    if not user or not hasattr(user, "role"):
        return False
    # Нормализуем роль (убираем пробелы, приводим к нижнему регистру)
    role = str(user.role).strip().lower()
    return role == "admin"


async def _require_admin(obj) -> bool:
    """
    Проверяет, является ли пользователь админом. Возвращает True если админ.
    Принимает Message или CallbackQuery.
    """
    # Определяем откуда брать user_id: из Message или CallbackQuery
    if isinstance(obj, CallbackQuery):
        tg_id = obj.from_user.id if obj.from_user else 0
        reply_func = obj.message.answer if obj.message else None
    else:  # Message
        tg_id = obj.from_user.id if obj.from_user else 0
        reply_func = obj.answer
    
    if not tg_id:
        logger.warning("[BROADCAST] No user ID found")
        if reply_func:
            await reply_func("🔒 Доступно только администраторам. Нажмите /login")
        return False
    
    user = find_user_by_telegram_id(tg_id)
    
    if not user:
        logger.warning(f"[BROADCAST] User {tg_id} not found")
        if reply_func:
            await reply_func("🔒 Доступно только администраторам. Нажмите /login")
        return False
    
    role = getattr(user, "role", "")
    logger.info(f"[BROADCAST] User {tg_id} role: {role!r}, is_admin: {_check_admin(user)}")
    
    if not _check_admin(user):
        logger.warning(f"[BROADCAST] User {tg_id} is not admin (role: {role!r})")
        if reply_func:
            await reply_func("🔒 Доступно только администраторам. Нажмите /login")
        return False
    
    return True


def _extract_media_attachments(message: Message) -> List[Dict[str, Any]]:
    """Извлекает медиа-вложения из сообщения."""
    attachments = []
    
    if message.photo:
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


async def _send_media_to_recipient(
    bot, chat_id: int, attachments: List[Dict[str, Any]], text: str = ""
) -> None:
    """Отправляет медиа получателю: send_media_group для фото/видео, send_document для документов."""
    from aiogram.types import InputMediaPhoto, InputMediaVideo
    
    photos = [att for att in attachments if att["type"] == "photo"]
    videos = [att for att in attachments if att["type"] == "video"]
    documents = [att for att in attachments if att["type"] == "document"]
    
    # Отправляем текст (если есть) сначала
    if text:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    
    # Отправляем фото батчами по 10
    for i in range(0, len(photos), 10):
        batch = photos[i:i+10]
        media_group = []
        for idx, att in enumerate(batch):
            caption = att.get("caption", "") if idx == 0 and not text else None
            media_group.append(InputMediaPhoto(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None))
        if media_group:
            await bot.send_media_group(chat_id=chat_id, media=media_group)
    
    # Отправляем видео батчами по 10
    for i in range(0, len(videos), 10):
        batch = videos[i:i+10]
        media_group = []
        for idx, att in enumerate(batch):
            caption = att.get("caption", "") if idx == 0 and not text else None
            media_group.append(InputMediaVideo(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None))
        if media_group:
            await bot.send_media_group(chat_id=chat_id, media=media_group)
    
    # Отправляем документы по одному
    for att in documents:
        caption = att.get("caption", "") if not text else None
        await bot.send_document(
            chat_id=chat_id,
            document=att["file_id"],
            caption=caption,
            parse_mode=ParseMode.HTML if caption else None
        )


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext) -> None:
    """Команда /broadcast: начать процесс создания рассылки."""
    if not await _require_admin(message):
        return
    
    await state.set_state(BroadcastState.waiting_text)
    await message.answer(
        "📢 <b>Создание рассылки</b>\n\n"
        "Введите текст рассылки (можно написать \"-\" если без текста):",
        parse_mode=ParseMode.HTML
    )


@router.message(BroadcastState.waiting_text)
async def handle_broadcast_text(message: Message, state: FSMContext) -> None:
    """Обработка текста рассылки."""
    if not await _require_admin(message):
        return
    
    text_original = message.text.strip() if message.text else "-"
    if text_original == "-":
        text_original = ""
    
    await state.update_data(text_original=text_original)
    await state.set_state(BroadcastState.waiting_media)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить медиа", callback_data="broadcast_skip_media")
    ]])
    
    await message.answer(
        "📎 Прикрепите медиа (фото/видео/документ, можно альбом) или нажмите «Пропустить медиа»:",
        reply_markup=keyboard
    )


@router.callback_query(F.data == "broadcast_skip_media")
async def skip_media(callback: CallbackQuery, state: FSMContext) -> None:
    """Пропустить прикрепление медиа."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    await callback.answer()
    
    data = await state.get_data()
    text_original = data.get("text_original", "")
    
    await _process_broadcast_text(callback.message, state, text_original, "")


async def _process_broadcast_text(message: Message, state: FSMContext, text_original: str, media_json: str) -> None:
    """Обрабатывает текст рассылки: улучшает через OpenAI и показывает превью."""
    if text_original:
        # Улучшаем текст через OpenAI
        await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)
        
        try:
            improved = await asyncio.to_thread(improve_broadcast_text, text_original)
            fixed = improved.get("fixed", text_original)
            suggested = improved.get("suggested", text_original)
        except Exception as e:
            logger.exception(f"[BROADCAST] Error improving text: {e}")
            fixed = text_original
            suggested = text_original
    else:
        fixed = ""
        suggested = ""
    
    # Сохраняем в state
    await state.update_data(
        text_fixed=fixed,
        text_suggested=suggested,
        media_json=media_json
    )
    await state.set_state(BroadcastState.waiting_choice)
    
    # Показываем превью
    preview_text = "📋 <b>Превью рассылки</b>\n\n"
    
    if text_original:
        preview_text += "📝 <b>Оригинал:</b>\n"
        preview_text += f"{text_original}\n\n"
        
        if fixed and fixed != text_original:
            preview_text += "✏️ <b>Исправленный:</b>\n"
            preview_text += f"{fixed}\n\n"
        
        if suggested and suggested != text_original:
            preview_text += "✨ <b>Улучшенный:</b>\n"
            preview_text += f"{suggested}\n\n"
    else:
        preview_text += "📝 Текст отсутствует (только медиа)\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить оригинал", callback_data="broadcast_send_original")],
        [InlineKeyboardButton(text="✨ Отправить улучшенный", callback_data="broadcast_send_improved")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")],
    ])
    
    await message.answer(preview_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


@router.message(BroadcastState.waiting_media)
async def handle_broadcast_media(message: Message, state: FSMContext) -> None:
    """Обработка медиа рассылки."""
    if not await _require_admin(message):
        return
    
    # Если это альбом
    if message.media_group_id:
        group_key = (str(message.media_group_id), message.from_user.id if message.from_user else 0)
        
        if group_key not in _media_group_buffer:
            _media_group_buffer[group_key] = []
        _media_group_buffer[group_key].append(message)
        
        if group_key in _processing_groups:
            return
        
        _processing_groups.add(group_key)
        asyncio.create_task(_process_album_with_debounce(group_key, message, state))
        return
    
    # Обычное медиа (не альбом)
    attachments = _extract_media_attachments(message)
    if attachments:
        media_json = json.dumps(attachments, ensure_ascii=False)
        data = await state.get_data()
        text_original = data.get("text_original", "")
        await _process_broadcast_text(message, state, text_original, media_json)
    else:
        await message.answer("❌ Не удалось обработать медиа. Попробуйте ещё раз.")


async def _process_album_with_debounce(group_key: tuple[str, int], message: Message, state: FSMContext) -> None:
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
    
    # Собираем все вложения
    all_attachments = []
    for msg in messages:
        attachments = _extract_media_attachments(msg)
        all_attachments.extend(attachments)
    
    media_json = json.dumps(all_attachments, ensure_ascii=False) if all_attachments else ""
    
    data = await state.get_data()
    text_original = data.get("text_original", "")
    
    await _process_broadcast_text(message, state, text_original, media_json)


@router.callback_query(F.data.startswith("broadcast_"))
async def handle_broadcast_choice(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка выбора: отправка или отмена."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    data = await state.get_data()
    
    if callback.data == "broadcast_cancel":
        await callback.answer("❌ Рассылка отменена")
        await state.clear()
        return
    
    if callback.data == "broadcast_send_original":
        text_final = data.get("text_original", "")
    elif callback.data == "broadcast_send_improved":
        text_final = data.get("text_suggested", "") or data.get("text_fixed", "") or data.get("text_original", "")
    else:
        await callback.answer("❌ Неизвестная команда")
        return
    
    await callback.answer("📤 Рассылка начата...")
    
    # Получаем список получателей
    users = await asyncio.to_thread(read_active_recipients_users)
    chats = await asyncio.to_thread(read_active_recipients_chats)
    
    users_count = len(users)
    chats_count = len(chats)
    
    # Создаём черновик рассылки
    media_json = data.get("media_json", "")
    created_by_user_id = callback.from_user.id if callback.from_user else 0
    created_by_username = callback.from_user.username if callback.from_user else None
    
    broadcast_id = await asyncio.to_thread(
        create_broadcast_draft,
        created_by_user_id=created_by_user_id,
        created_by_username=created_by_username,
        text_original=data.get("text_original", ""),
        media_json=media_json,
        users_count=users_count,
        chats_count=chats_count,
    )
    
    # Логируем событие
    await log_event(
        user_id=created_by_user_id,
        username=created_by_username,
        event="broadcast_created",
        meta={"broadcast_id": broadcast_id, "users_count": users_count, "chats_count": chats_count},
    )
    
    # Отправляем всем получателям
    sent_ok = 0
    sent_fail = 0
    
    # Парсим медиа
    attachments = []
    if media_json:
        try:
            attachments = json.loads(media_json)
        except Exception:
            pass
    
    # Семафор на 10 одновременных отправок
    semaphore = asyncio.Semaphore(10)
    
    async def send_to_user(user_id: int) -> None:
        nonlocal sent_ok, sent_fail
        async with semaphore:
            try:
                if text_final or attachments:
                    if attachments:
                        await _send_media_to_recipient(callback.message.bot, user_id, attachments, text_final)
                    else:
                        await callback.message.bot.send_message(chat_id=user_id, text=text_final, parse_mode=ParseMode.HTML)
                    
                    await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "user", user_id, "ok")
                    sent_ok += 1
                else:
                    await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "user", user_id, "fail", "empty message")
                    sent_fail += 1
            except TelegramForbiddenError as e:
                error_text = "blocked"
                await asyncio.to_thread(mark_user_failed, user_id, error_text)
                await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "user", user_id, "fail", error_text)
                sent_fail += 1
            except Exception as e:
                error_text = str(e)[:500]
                await asyncio.to_thread(mark_user_failed, user_id, error_text)
                await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "user", user_id, "fail", error_text)
                sent_fail += 1
    
    async def send_to_chat(chat_id: int) -> None:
        nonlocal sent_ok, sent_fail
        async with semaphore:
            try:
                if text_final or attachments:
                    if attachments:
                        await _send_media_to_recipient(callback.message.bot, chat_id, attachments, text_final)
                    else:
                        await callback.message.bot.send_message(chat_id=chat_id, text=text_final, parse_mode=ParseMode.HTML)
                    
                    await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "chat", chat_id, "ok")
                    sent_ok += 1
                else:
                    await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "chat", chat_id, "fail", "empty message")
                    sent_fail += 1
            except TelegramForbiddenError as e:
                error_text = "blocked"
                await asyncio.to_thread(mark_chat_failed, chat_id, error_text)
                await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "chat", chat_id, "fail", error_text)
                sent_fail += 1
            except Exception as e:
                error_text = str(e)[:500]
                await asyncio.to_thread(mark_chat_failed, chat_id, error_text)
                await asyncio.to_thread(log_broadcast_recipient, broadcast_id, "chat", chat_id, "fail", error_text)
                sent_fail += 1
    
    # Создаём задачи для всех получателей
    tasks = []
    for user_id in users:
        tasks.append(send_to_user(user_id))
    for chat_id in chats:
        tasks.append(send_to_chat(chat_id))
    
    # Ждём завершения всех задач
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # Обновляем статус рассылки
    await asyncio.to_thread(
        finalize_broadcast,
        broadcast_id=broadcast_id,
        text_final=text_final,
        status="sent",
        sent_ok=sent_ok,
        sent_fail=sent_fail,
    )
    
    # Логируем событие
    await log_event(
        user_id=created_by_user_id,
        username=created_by_username,
        event="broadcast_sent",
        meta={"broadcast_id": broadcast_id, "ok": sent_ok, "fail": sent_fail},
    )
    
    # Отвечаем админу
    result_text = (
        f"✅ <b>Рассылка отправлена</b>\n\n"
        f"📊 Статистика:\n"
        f"• Успешно: {sent_ok}\n"
        f"• Ошибок: {sent_fail}\n"
        f"• Всего получателей: {users_count + chats_count}\n\n"
        f"ID рассылки: <code>{broadcast_id}</code>"
    )
    
    await callback.message.answer(result_text, parse_mode=ParseMode.HTML)
    await state.clear()

