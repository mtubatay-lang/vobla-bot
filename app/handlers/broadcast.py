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
    read_active_recipients_chats_with_names,
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
    choosing_variant = State()  # Выбор варианта текста (оригинал/улучшенный)
    choosing_audience = State()  # Первичный выбор аудитории (с "тест себе")
    choosing_audience_final = State()  # Финальный выбор аудитории (после теста)
    selecting_chats = State()  # Выбор конкретных чатов


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


async def _cancel_broadcast(callback: CallbackQuery, state: FSMContext, broadcast_id: Optional[str] = None) -> None:
    """Отменяет рассылку: обновляет статус, очищает FSM."""
    if broadcast_id:
        await asyncio.to_thread(
            finalize_broadcast,
            broadcast_id=broadcast_id,
            text_final="",
            status="cancelled",
            sent_ok=0,
            sent_fail=0,
        )
    
    await state.clear()
    if callback.message:
        await callback.message.answer("Рассылка отменена ✅")


async def _check_user_owns_broadcast(callback: CallbackQuery, state: FSMContext) -> bool:
    """Проверяет, что callback от инициатора рассылки."""
    data = await state.get_data()
    owner_id = data.get("owner_id")
    current_id = callback.from_user.id if callback.from_user else 0
    
    if owner_id and owner_id != current_id:
        await callback.answer("❌ Это не ваша рассылка", show_alert=True)
        return False
    return True


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext) -> None:
    """Команда /broadcast: начать процесс создания рассылки."""
    if not await _require_admin(message):
        return
    
    owner_id = message.from_user.id if message.from_user else 0
    await state.update_data(owner_id=owner_id)
    await state.set_state(BroadcastState.waiting_text)
    await message.answer(
        "📢 <b>Создание рассылки</b>\n\n"
        "Введите текст рассылки (можно написать \"-\" если без текста):",
        parse_mode=ParseMode.HTML
    )


@router.callback_query(F.data == "broadcast_start")
async def broadcast_start_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка нажатия на кнопку 'Запуск рассылки' в меню."""
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    await callback.answer()
    
    owner_id = callback.from_user.id if callback.from_user else 0
    await state.update_data(owner_id=owner_id)
    await state.set_state(BroadcastState.waiting_text)
    
    if callback.message:
        await callback.message.answer(
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
        InlineKeyboardButton(text="⏭ Пропустить медиа", callback_data="broadcast:skip_media")
    ]])
    
    await message.answer(
        "📎 Прикрепите медиа (фото/видео/документ, можно альбом) или нажмите «Пропустить медиа»:",
        reply_markup=keyboard
    )


@router.callback_query(F.data == "broadcast:edit_text")
async def handle_edit_text(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка кнопки 'Изменить текст'."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    # Переход в состояние ожидания текста
    await state.set_state(BroadcastState.waiting_text)
    
    if callback.message:
        await callback.message.answer(
            "✏️ <b>Изменение текста рассылки</b>\n\n"
            "Введите новый текст рассылки (можно написать \"-\" если без текста):",
            parse_mode=ParseMode.HTML
        )


@router.callback_query(F.data == "broadcast:edit_media")
async def handle_edit_media(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка кнопки 'Изменить медиа'."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    # Очищаем медиа из state
    await state.update_data(media_json="")
    
    # Переход в состояние ожидания медиа
    await state.set_state(BroadcastState.waiting_media)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить медиа", callback_data="broadcast:skip_media")
    ]])
    
    if callback.message:
        await callback.message.answer(
            "📎 <b>Изменение медиа рассылки</b>\n\n"
            "Прикрепите новое медиа (фото/видео/документ, можно альбом) или нажмите «Пропустить медиа»:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )


@router.callback_query(F.data == "broadcast:skip_media")
async def skip_media(callback: CallbackQuery, state: FSMContext) -> None:
    """Пропустить прикрепление медиа."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    data = await state.get_data()
    text_original = data.get("text_original", "")
    media_json = data.get("media_json", "")
    
    # Проверка: должен быть хотя бы текст или медиа
    if not text_original and not media_json:
        await callback.message.answer(
            "❌ Нужно ввести хотя бы текст или прикрепить медиа.\n\n"
            "Введите текст рассылки (можно \"-\" для пропуска):"
        )
        await state.set_state(BroadcastState.waiting_text)
        return
    
    await _process_broadcast_text(callback.message, state, text_original, media_json or "")


async def _process_broadcast_text(message: Message, state: FSMContext, text_original: str, media_json: str) -> None:
    """Обрабатывает текст рассылки: улучшает через OpenAI и показывает превью."""
    # Проверка: должен быть хотя бы текст или медиа
    if not text_original and not media_json:
        await message.answer(
            "❌ Нужно ввести хотя бы текст или прикрепить медиа.\n\n"
            "Введите текст рассылки (можно \"-\" для пропуска):"
        )
        await state.set_state(BroadcastState.waiting_text)
        return
    
    improved_text = ""
    if text_original:
        # Улучшаем текст через OpenAI
        await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)
        
        try:
            improved = await asyncio.to_thread(improve_broadcast_text, text_original)
            improved_text = improved.get("suggested", text_original) or improved.get("fixed", text_original) or text_original
        except Exception as e:
            logger.exception(f"[BROADCAST] Error improving text: {e}")
            improved_text = text_original
    else:
        improved_text = ""
    
    # АВТОМАТИЧЕСКИ выбираем улучшенную версию
    text_final = improved_text if improved_text else text_original
    
    # Сохраняем в state
    await state.update_data(
        improved_text=improved_text,
        media_json=media_json,
        text_final=text_final,  # Сохраняем сразу финальный текст
        selected_variant="improved"  # Автоматически выбираем улучшенный
    )
    await state.set_state(BroadcastState.choosing_audience)  # Переходим сразу к выбору аудитории
    
    # Если есть медиа, отправляем его вместе с текстом
    if media_json:
        try:
            attachments = json.loads(media_json)
            if attachments:
                # Отправляем медиа с текстом
                await _send_media_to_recipient(message.bot, message.chat.id, attachments, text_final)
        except Exception as e:
            logger.exception(f"[BROADCAST] Error sending media preview: {e}")
    
    # Формируем превью с улучшенным текстом
    preview_text = "📋 <b>Превью рассылки</b>\n\n"
    
    if text_final:
        preview_text += f"{text_final}\n\n"
    else:
        preview_text += "📝 Текст отсутствует (только медиа)\n\n"
    
    if media_json:
        preview_text += "📎 Медиа прикреплено\n\n"
    
    # Новые кнопки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧪 Отправить тестовую рассылку себе", callback_data="broadcast:aud:test_self")],
        [InlineKeyboardButton(text="✏️ Изменить текст", callback_data="broadcast:edit_text")],
        [InlineKeyboardButton(text="📎 Изменить медиа", callback_data="broadcast:edit_media")],
        [InlineKeyboardButton(text="❌ Отменить рассылку", callback_data="broadcast:cancel")],
    ])
    
    # Если медиа уже отправлено, отправляем только текст с кнопками или только кнопки
    if media_json and text_final:
        # Если медиа уже отправлено, отправляем только кнопки
        await message.answer("✅ Превью отправлено выше. Выберите действие:", reply_markup=keyboard)
    else:
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
    
    # Очищаем буфер
    _media_group_buffer.pop(group_key, None)
    _processing_groups.discard(group_key)
    
    data = await state.get_data()
    text_original = data.get("text_original", "")
    
    await _process_broadcast_text(message, state, text_original, media_json)




@router.callback_query(F.data == "broadcast:cancel")
async def handle_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка отмены рассылки."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    data = await state.get_data()
    broadcast_id = data.get("broadcast_id")
    
    await _cancel_broadcast(callback, state, broadcast_id)


@router.callback_query(F.data == "broadcast:aud:test_self")
async def handle_test_self(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка тестовой рассылки себе."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    data = await state.get_data()
    
    # Проверка наличия данных
    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    
    if not text_final and not media_json:
        await callback.answer("❌ Нет данных рассылки, начните заново /broadcast", show_alert=True)
        await state.clear()
        return
    
    await callback.answer("📤 Отправляю тест...")
    
    # Отправляем тест инициатору
    created_by_user_id = callback.from_user.id if callback.from_user else 0
    
    try:
        # Парсим медиа
        attachments = []
        if media_json:
            try:
                attachments = json.loads(media_json)
            except Exception:
                pass
        
        # Отправляем тест
        if attachments:
            await _send_media_to_recipient(callback.message.bot, created_by_user_id, attachments, text_final)
        else:
            await callback.message.bot.send_message(chat_id=created_by_user_id, text=text_final, parse_mode=ParseMode.HTML)
        
        # Показываем финальный выбор аудитории
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Пользователям бота", callback_data="broadcast:send:users")],
            [InlineKeyboardButton(text="💬 Во все чаты", callback_data="broadcast:send:chats")],
            [InlineKeyboardButton(text="👥💬 В бот и чаты", callback_data="broadcast:send:users_chats")],
            [InlineKeyboardButton(text="📋 Выбрать определенные чаты", callback_data="broadcast:select_chats")],
            [InlineKeyboardButton(text="❌ Отмена рассылки", callback_data="broadcast:cancel_send")],
        ])
        
        await callback.message.answer(
            "✅ Тест отправлен. Кому отправляем финально?",
            reply_markup=keyboard
        )
        
        await state.set_state(BroadcastState.choosing_audience_final)
        
    except Exception as e:
        logger.exception(f"[BROADCAST] Error sending test: {e}")
        await callback.message.answer(f"❌ Ошибка при отправке теста: {str(e)[:200]}")


@router.callback_query(F.data.startswith("broadcast:send:"))
async def handle_send_broadcast(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка финальной отправки рассылки по выбранной аудитории."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    # Проверка, что мы в финальном состоянии
    current_state = await state.get_state()
    if current_state != BroadcastState.choosing_audience_final:
        await callback.answer("❌ Сначала отправьте тестовую рассылку", show_alert=True)
        return
    
    data = await state.get_data()
    
    # Проверка наличия данных
    broadcast_id = data.get("broadcast_id")
    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    
    if not text_final and not media_json:
        await callback.answer("❌ Нет данных рассылки, начните заново /broadcast", show_alert=True)
        await state.clear()
        return
    
    # Проверка: должен быть хотя бы текст или медиа
    if not text_final and not media_json:
        await callback.answer("❌ Нужно ввести хотя бы текст или прикрепить медиа", show_alert=True)
        return
    
    # Определяем режим отправки (без self, так как он обрабатывается отдельно)
    if callback.data == "broadcast:send:users":
        mode = "users"
    elif callback.data == "broadcast:send:chats":
        mode = "chats"
    elif callback.data == "broadcast:send:users_chats":
        mode = "users_chats"
    else:
        await callback.answer("❌ Неизвестная команда")
        return
    
    await callback.answer("📤 Рассылка начата...")
    
    # Получаем данные
    created_by_user_id = callback.from_user.id if callback.from_user else 0
    created_by_username = callback.from_user.username if callback.from_user else None
    text_original = data.get("text_original", "")
    selected_variant = data.get("selected_variant", "original")
    
    # Получаем списки получателей в зависимости от режима (без self, так как он обрабатывается отдельно)
    users = []
    chats = []
    
    if mode == "users":
        users = await asyncio.to_thread(read_active_recipients_users)
        chats = []
    elif mode == "chats":
        users = []
        chats = await asyncio.to_thread(read_active_recipients_chats)
    elif mode == "users_chats":
        users_list = await asyncio.to_thread(read_active_recipients_users)
        chats_list = await asyncio.to_thread(read_active_recipients_chats)
        users = users_list
        chats = chats_list
    
    users_count = len(users)
    chats_count = len(chats)
    
    # Создаём черновик рассылки (если ещё не создан)
    if not broadcast_id:
        broadcast_id = await asyncio.to_thread(
            create_broadcast_draft,
            created_by_user_id=created_by_user_id,
            created_by_username=created_by_username,
            text_original=text_original,
            media_json=media_json,
            users_count=users_count,
            chats_count=chats_count,
        )
        await state.update_data(broadcast_id=broadcast_id)
        
        # Логируем событие создания
        await asyncio.to_thread(
            log_event,
            user_id=created_by_user_id,
            username=created_by_username,
            event="broadcast_created",
            meta={"broadcast_id": broadcast_id, "mode": mode},
        )
    
    # Парсим медиа
    attachments = []
    if media_json:
        try:
            attachments = json.loads(media_json)
        except Exception:
            pass
    
    # Отправляем всем получателям
    sent_ok = 0
    sent_fail = 0
    
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
    
    total = sent_ok + sent_fail
    
    # Обновляем статус рассылки
    await asyncio.to_thread(
        finalize_broadcast,
        broadcast_id=broadcast_id,
        text_final=text_final,
        status="sent",
        sent_ok=sent_ok,
        sent_fail=sent_fail,
        selected_variant=selected_variant,
        mode=mode,
    )
    
    # Логируем событие отправки
    await asyncio.to_thread(
        log_event,
        user_id=created_by_user_id,
        username=created_by_username,
        event="broadcast_sent",
        meta={
            "broadcast_id": broadcast_id,
            "mode": mode,
            "variant": selected_variant,
            "total": total,
            "ok": sent_ok,
            "fail": sent_fail,
        },
    )
    
    # Отвечаем админу
    result_text = (
        f"✅ <b>Рассылка отправлена</b>\n\n"
        f"📊 Статистика:\n"
        f"• Всего получателей: {total}\n"
        f"• Успешно: {sent_ok}\n"
        f"• Ошибок: {sent_fail}\n\n"
        f"ID рассылки: <code>{broadcast_id}</code>"
    )
    
    await callback.message.answer(result_text, parse_mode=ParseMode.HTML)
    await state.clear()


@router.callback_query(F.data == "broadcast:cancel_send")
async def handle_cancel_send(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка отмены на финальном этапе выбора аудитории."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    data = await state.get_data()
    broadcast_id = data.get("broadcast_id")
    
    # Помечаем рассылку как cancelled
    if broadcast_id:
        await asyncio.to_thread(
            finalize_broadcast,
            broadcast_id=broadcast_id,
            text_final="",
            status="cancelled",
            sent_ok=0,
            sent_fail=0,
        )
    
    await state.clear()
    await callback.message.answer("❌ Рассылка отменена")


@router.callback_query(F.data == "broadcast:select_chats")
async def handle_select_chats(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка нажатия на кнопку 'Выбрать определенные чаты'."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    # Переход в состояние выбора чатов
    await state.set_state(BroadcastState.selecting_chats)
    
    # Инициализируем список выбранных чатов, если его еще нет
    data = await state.get_data()
    if "selected_chat_ids" not in data:
        await state.update_data(selected_chat_ids=[])
    
    # Читаем список доступных чатов
    chats = await asyncio.to_thread(read_active_recipients_chats_with_names)
    
    if not chats:
        await callback.message.answer(
            "❌ Нет доступных чатов для выбора.\n\n"
            "Проверьте таблицу recipients_chats."
        )
        return
    
    # Сохраняем список чатов в state для последующего использования
    await state.update_data(available_chats=chats)
    
    # Формируем сообщение со списком чатов
    await _show_chats_selection(callback.message, state, chats, [])


async def _show_chats_selection(
    message: Message,
    state: FSMContext,
    chats: List[Dict[str, Any]],
    selected_chat_ids: List[int],
    page: int = 0,
    chats_per_page: int = 20
) -> None:
    """Показывает список чатов для выбора с пагинацией."""
    total_chats = len(chats)
    start_idx = page * chats_per_page
    end_idx = min(start_idx + chats_per_page, total_chats)
    page_chats = chats[start_idx:end_idx]
    
    # Получить текст рассылки из state
    data = await state.get_data()
    text_final = data.get("text_final", "")
    
    # Формируем текст сообщения
    text = f"📋 <b>Выберите чаты для рассылки</b>\n\n"
    
    # Добавить текст рассылки, если есть
    if text_final:
        # Обрезаем длинный текст для превью (максимум 200 символов)
        preview_text = text_final[:200] + "..." if len(text_final) > 200 else text_final
        text += f"<b>Текст рассылки:</b>\n{preview_text}\n\n"
    
    text += f"Выбрано: {len(selected_chat_ids)} из {total_chats}\n\n"
    
    if not page_chats:
        text += "Нет чатов для отображения."
    else:
        text += "Доступные чаты:\n\n"
    
    # Формируем кнопки для чатов
    buttons = []
    for chat in page_chats:
        chat_id = chat["chat_id"]
        chat_name = chat["name"]
        is_selected = chat_id in selected_chat_ids
        
        # Обрезаем длинное название
        display_name = chat_name[:40] + "..." if len(chat_name) > 40 else chat_name
        
        checkbox = "☑" if is_selected else "☐"
        buttons.append([
            InlineKeyboardButton(
                text=f"{checkbox} {display_name}",
                callback_data=f"broadcast:chat_toggle:{chat_id}"
            )
        ])
    
    # Кнопки навигации (если нужно)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(text="◀ Назад", callback_data=f"broadcast:chats_page:{page - 1}")
        )
    if end_idx < total_chats:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперед ▶", callback_data=f"broadcast:chats_page:{page + 1}")
        )
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Кнопки действий
    action_buttons = []
    if selected_chat_ids:
        action_buttons.append(
            InlineKeyboardButton(text="✅ Отправить в выбранные", callback_data="broadcast:send:selected_chats")
        )
    action_buttons.append(
        InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel_send")
    )
    buttons.append(action_buttons)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    # Отправляем или обновляем сообщение
    data = await state.get_data()
    selection_message_id = data.get("selection_message_id")
    
    if selection_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=selection_message_id,
                text=text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            # Если не удалось обновить, отправляем новое
            sent_msg = await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            await state.update_data(selection_message_id=sent_msg.message_id)
    else:
        sent_msg = await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        await state.update_data(selection_message_id=sent_msg.message_id)


@router.callback_query(F.data.startswith("broadcast:chat_toggle:"))
async def handle_chat_toggle(callback: CallbackQuery, state: FSMContext) -> None:
    """Переключение выбора чата (добавить/убрать из списка)."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    # Извлекаем chat_id из callback.data
    try:
        chat_id = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка обработки", show_alert=True)
        return
    
    data = await state.get_data()
    selected_chat_ids: List[int] = data.get("selected_chat_ids", [])
    available_chats: List[Dict[str, Any]] = data.get("available_chats", [])
    
    # Переключаем выбор
    if chat_id in selected_chat_ids:
        selected_chat_ids.remove(chat_id)
    else:
        selected_chat_ids.append(chat_id)
    
    # Обновляем state
    await state.update_data(selected_chat_ids=selected_chat_ids)
    
    # Определяем текущую страницу (по умолчанию 0)
    current_page = data.get("chats_page", 0)
    
    # Обновляем сообщение
    await _show_chats_selection(callback.message, state, available_chats, selected_chat_ids, current_page)
    await callback.answer()


@router.callback_query(F.data.startswith("broadcast:chats_page:"))
async def handle_chats_page(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка переключения страницы списка чатов."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    # Извлекаем номер страницы
    try:
        page = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка обработки", show_alert=True)
        return
    
    data = await state.get_data()
    selected_chat_ids: List[int] = data.get("selected_chat_ids", [])
    available_chats: List[Dict[str, Any]] = data.get("available_chats", [])
    
    # Сохраняем текущую страницу
    await state.update_data(chats_page=page)
    
    # Обновляем сообщение
    await _show_chats_selection(callback.message, state, available_chats, selected_chat_ids, page)
    await callback.answer()


@router.callback_query(F.data == "broadcast:send:selected_chats")
async def handle_send_selected_chats(callback: CallbackQuery, state: FSMContext) -> None:
    """Отправка рассылки в выбранные чаты."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    data = await state.get_data()
    
    # Получаем выбранные чаты
    selected_chat_ids: List[int] = data.get("selected_chat_ids", [])
    
    if not selected_chat_ids:
        await callback.answer("❌ Выберите хотя бы один чат", show_alert=True)
        return
    
    # Проверка наличия данных рассылки
    broadcast_id = data.get("broadcast_id")
    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    
    if not text_final and not media_json:
        await callback.answer("❌ Нет данных рассылки, начните заново /broadcast", show_alert=True)
        await state.clear()
        return
    
    await callback.answer("📤 Рассылка начата...")
    
    # Получаем данные
    created_by_user_id = callback.from_user.id if callback.from_user else 0
    created_by_username = callback.from_user.username if callback.from_user else None
    text_original = data.get("text_original", "")
    selected_variant = data.get("selected_variant", "original")
    
    # Используем только выбранные чаты
    users = []
    chats = selected_chat_ids
    
    users_count = len(users)
    chats_count = len(chats)
    
    # Создаём черновик рассылки (если ещё не создан)
    if not broadcast_id:
        broadcast_id = await asyncio.to_thread(
            create_broadcast_draft,
            created_by_user_id=created_by_user_id,
            created_by_username=created_by_username,
            text_original=text_original,
            media_json=media_json,
            users_count=users_count,
            chats_count=chats_count,
        )
        await state.update_data(broadcast_id=broadcast_id)
        
        # Логируем событие создания
        await asyncio.to_thread(
            log_event,
            user_id=created_by_user_id,
            username=created_by_username,
            event="broadcast_created",
            meta={"broadcast_id": broadcast_id, "mode": "selected_chats"},
        )
    
    # Парсим медиа
    attachments = []
    if media_json:
        try:
            attachments = json.loads(media_json)
        except Exception:
            pass
    
    # Отправляем всем получателям
    sent_ok = 0
    sent_fail = 0
    
    # Семафор на 10 одновременных отправок
    semaphore = asyncio.Semaphore(10)
    
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
    for chat_id in chats:
        tasks.append(send_to_chat(chat_id))
    
    # Ждём завершения всех задач
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    
    total = sent_ok + sent_fail
    
    # Обновляем статус рассылки
    await asyncio.to_thread(
        finalize_broadcast,
        broadcast_id=broadcast_id,
        text_final=text_final,
        status="sent",
        sent_ok=sent_ok,
        sent_fail=sent_fail,
        selected_variant=selected_variant,
        mode="selected_chats",
    )
    
    # Логируем событие отправки
    await asyncio.to_thread(
        log_event,
        user_id=created_by_user_id,
        username=created_by_username,
        event="broadcast_sent",
        meta={
            "broadcast_id": broadcast_id,
            "mode": "selected_chats",
            "variant": selected_variant,
            "total": total,
            "ok": sent_ok,
            "fail": sent_fail,
        },
    )
    
    # Отвечаем админу
    result_text = (
        f"✅ <b>Рассылка отправлена</b>\n\n"
        f"📊 Статистика:\n"
        f"• Всего получателей: {total}\n"
        f"• Успешно: {sent_ok}\n"
        f"• Ошибок: {sent_fail}\n\n"
        f"ID рассылки: <code>{broadcast_id}</code>"
    )
    
    await callback.message.answer(result_text, parse_mode=ParseMode.HTML)
    await state.clear()
