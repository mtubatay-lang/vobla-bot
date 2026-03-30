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

from app.services.admin_access import is_admin_for_platform
from app.services.auth_service import find_user_by_telegram_id
from app.services.broadcast_service import (
    create_broadcast_draft,
    execute_broadcast,
    finalize_broadcast,
    log_broadcast_recipient,
    mark_chat_failed,
    mark_user_failed,
    read_active_recipients_chats,
    read_active_recipients_chats_with_names,
    read_active_recipients_users,
    read_active_regions,
    read_chats_by_regions,
)
from app.services.metrics_service import log_event
from app.services.openai_client import improve_broadcast_text, generate_broadcast_title
from app.services.scheduled_broadcast_service import (
    compute_next_run_iso,
    create_scheduled_broadcast,
    read_active_scheduled_broadcasts_for_list,
    set_scheduled_broadcast_inactive,
)
from app.core.callbacks import ADMIN_SCHEDULED, DOC_GEN_START

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
    choosing_audience = State()  # Превью / тест себе (тест необязателен)
    choosing_audience_final = State()  # После «тест себе»; шаг сохраняется для совместимости
    choosing_send_type = State()  # Отправить сейчас / раз в неделю / раз в месяц
    choosing_weekday = State()  # День недели для еженедельной рассылки
    choosing_month_day = State()  # Число месяца для ежемесячной рассылки
    selecting_chats = State()  # Выбор конкретных чатов
    choosing_segmentation_type = State()  # Выбор типа сегментации (По Регионам / По ИП)
    selecting_regions = State()  # Выбор регионов


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
    
    if not is_admin_for_platform("telegram", tg_id):
        logger.warning(f"[BROADCAST] User {tg_id} is not admin or not found")
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


async def _build_scheduled_list() -> tuple[str, InlineKeyboardMarkup | None]:
    """Формирует текст и клавиатуру списка плановых рассылок. При отсутствии записей клавиатура None."""
    items = await asyncio.to_thread(read_active_scheduled_broadcasts_for_list)
    if not items:
        return "📋 Нет активных плановых рассылок.", None
    lines = ["📋 <b>Плановые рассылки</b>\n"]
    buttons = []
    for rec in items:
        schedule_id = (rec.get("schedule_id") or "")[:12]
        title = (rec.get("title") or "").strip()
        if not title:
            text_preview = (rec.get("text_final") or "").strip()
            title = (text_preview[:50] + "…") if len(text_preview) > 50 else text_preview if text_preview else "Без названия"
        next_run = (rec.get("next_run_iso") or "")[:16]
        lines.append(f"• <b>{title}</b> — след. запуск: {next_run}\n  ID: <code>{schedule_id}</code>")
        buttons.append([InlineKeyboardButton(text=f"🔴 Выключить {schedule_id}", callback_data=f"broadcast:scheduled_disable:{schedule_id}")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return "\n".join(lines), keyboard


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    """Команда /admin: раздел администраторов — выбор функции (рассылка, плановые рассылки, база знаний)."""
    if not await _require_admin(message):
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Запуск рассылки", callback_data="broadcast_start")],
        [InlineKeyboardButton(text="📅 Плановые рассылки", callback_data=ADMIN_SCHEDULED)],
        [InlineKeyboardButton(text="📚 Пополнение базы знаний", callback_data="kb_add")],
        [InlineKeyboardButton(text="📝 Создать документ", callback_data=DOC_GEN_START)],
    ])
    await message.answer(
        "Выберите необходимую функцию:",
        reply_markup=keyboard,
    )


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


@router.message(Command("broadcast_scheduled"))
async def cmd_broadcast_scheduled(message: Message) -> None:
    """Команда /broadcast_scheduled: список активных плановых рассылок и отключение."""
    if not await _require_admin(message):
        return

    text, keyboard = await _build_scheduled_list()
    if keyboard is None:
        await message.answer(text)
    else:
        await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


@router.callback_query(F.data == ADMIN_SCHEDULED)
async def admin_scheduled_callback(callback: CallbackQuery) -> None:
    """Кнопка «Плановые рассылки» в разделе администраторов."""
    if not await _require_admin(callback):
        await callback.answer()
        return
    await callback.answer()
    text, keyboard = await _build_scheduled_list()
    if callback.message:
        if keyboard is None:
            await callback.message.answer(text)
        else:
            await callback.message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


@router.callback_query(F.data.startswith("broadcast:scheduled_disable:"))
async def handle_scheduled_disable(callback: CallbackQuery) -> None:
    """Отключение плановой рассылки (is_active=0)."""
    if not await _require_admin(callback):
        await callback.answer()
        return
    schedule_id = callback.data.replace("broadcast:scheduled_disable:", "").strip()
    if not schedule_id:
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    ok = await asyncio.to_thread(set_scheduled_broadcast_inactive, schedule_id)
    if ok:
        await callback.answer("✅ Рассылка отключена")
        if callback.message:
            await callback.message.answer("✅ Плановая рассылка отключена.")
    else:
        await callback.answer("❌ Не удалось отключить", show_alert=True)


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


def _audience_preview_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура превью перед выбором аудитории (тест себе, изменить текст/медиа, отмена)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧪 Отправить тестовую рассылку себе", callback_data="broadcast:aud:test_self")],
        [InlineKeyboardButton(text="✏️ Изменить текст", callback_data="broadcast:edit_text")],
        [InlineKeyboardButton(text="📎 Изменить медиа", callback_data="broadcast:edit_media")],
        [InlineKeyboardButton(text="❌ Отменить рассылку", callback_data="broadcast:cancel")],
    ])


async def _send_audience_preview(
    message: Message, text_final: str, media_json: str
) -> None:
    """Отправляет превью рассылки и кнопки выбора аудитории (тест себе, изменить текст/медиа, отмена)."""
    if media_json:
        try:
            attachments = json.loads(media_json)
            if attachments:
                await _send_media_to_recipient(message.bot, message.chat.id, attachments, text_final)
        except Exception as e:
            logger.exception(f"[BROADCAST] Error sending media preview: {e}")
    keyboard = _audience_preview_keyboard()
    preview_text = "📋 <b>Превью рассылки</b>\n\n"
    if text_final:
        preview_text += f"{text_final}\n\n"
    else:
        preview_text += "📝 Текст отсутствует (только медиа)\n\n"
    if media_json:
        preview_text += "📎 Медиа прикреплено\n\n"
    if media_json and text_final:
        await message.answer("✅ Превью отправлено выше. Выберите действие:", reply_markup=keyboard)
    else:
        await message.answer(preview_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


async def _process_broadcast_text(message: Message, state: FSMContext, text_original: str, media_json: str) -> None:
    """Обрабатывает текст рассылки: улучшает через OpenAI и показывает превью (с выбором оригинала/улучшенного при необходимости)."""
    if not text_original and not media_json:
        await message.answer(
            "❌ Нужно ввести хотя бы текст или прикрепить медиа.\n\n"
            "Введите текст рассылки (можно \"-\" для пропуска):"
        )
        await state.set_state(BroadcastState.waiting_text)
        return

    improved_text = ""
    if text_original:
        await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)
        try:
            improved = await asyncio.to_thread(improve_broadcast_text, text_original)
            improved_text = improved.get("suggested", text_original) or improved.get("fixed", text_original) or text_original
        except Exception as e:
            logger.exception(f"[BROADCAST] Error improving text: {e}")
            improved_text = text_original
    else:
        improved_text = ""

    need_variant_choice = (
        bool(text_original)
        and bool(improved_text)
        and improved_text.strip() != text_original.strip()
    )

    if need_variant_choice:
        await state.update_data(
            text_original=text_original,
            improved_text=improved_text,
            media_json=media_json,
        )
        await state.set_state(BroadcastState.choosing_variant)
        if media_json:
            try:
                attachments = json.loads(media_json)
                if attachments:
                    await _send_media_to_recipient(message.bot, message.chat.id, attachments, improved_text)
            except Exception as e:
                logger.exception(f"[BROADCAST] Error sending media preview: {e}")
        variant_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оставить оригинал", callback_data="broadcast:variant:original")],
            [InlineKeyboardButton(text="Оставить улучшенный", callback_data="broadcast:variant:improved")],
        ])
        preview_msg = "📋 <b>Превью (улучшенный вариант)</b>\n\n" + improved_text
        if media_json:
            preview_msg += "\n\n📎 Медиа прикреплено"
        await message.answer(preview_msg, reply_markup=variant_keyboard, parse_mode=ParseMode.HTML)
        return

    text_final = improved_text if improved_text else text_original
    await state.update_data(
        improved_text=improved_text,
        media_json=media_json,
        text_final=text_final,
        selected_variant="improved" if improved_text else "original",
    )
    await state.set_state(BroadcastState.choosing_audience)
    await _send_audience_preview(message, text_final, media_json)


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




@router.callback_query(F.data == "broadcast:variant:original")
async def handle_variant_original(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор оригинала в превью рассылки."""
    if not callback.message:
        await callback.answer()
        return
    if not await _require_admin(callback):
        await callback.answer()
        return
    if not await _check_user_owns_broadcast(callback, state):
        return
    data = await state.get_data()
    text_original = data.get("text_original", "")
    media_json = data.get("media_json", "")
    await state.update_data(text_final=text_original, selected_variant="original")
    await state.set_state(BroadcastState.choosing_audience)
    await callback.answer()
    await _send_audience_preview(callback.message, text_original, media_json)


@router.callback_query(F.data == "broadcast:variant:improved")
async def handle_variant_improved(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор улучшенного текста в превью рассылки."""
    if not callback.message:
        await callback.answer()
        return
    if not await _require_admin(callback):
        await callback.answer()
        return
    if not await _check_user_owns_broadcast(callback, state):
        return
    data = await state.get_data()
    improved_text = data.get("improved_text", "")
    media_json = data.get("media_json", "")
    await state.update_data(text_final=improved_text, selected_variant="improved")
    await state.set_state(BroadcastState.choosing_audience)
    await callback.answer()
    await _send_audience_preview(callback.message, improved_text, media_json)


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
    prev_state = await state.get_state()
    # Состояние «после теста» до долгой отправки — уже в общем FSM (Redis); при MemoryStorage
    # по-прежнему нужен один воркер или REDIS_URL.
    await state.set_state(BroadcastState.choosing_audience_final)

    try:
        # Парсим медиа
        attachments = []
        if media_json:
            try:
                attachments = json.loads(media_json)
            except Exception as e:
                logger.warning("[BROADCAST] Парсинг media_json при тесте: %s", e, exc_info=True)
        
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
        
    except Exception as e:
        await state.set_state(prev_state)
        logger.exception(f"[BROADCAST] Error sending test: {e}")
        await callback.message.answer(f"❌ Ошибка при отправке теста: {str(e)[:200]}")


@router.callback_query(F.data == "broadcast:send:selected_chats")
async def handle_send_selected_chats(callback: CallbackQuery, state: FSMContext) -> None:
    """Отправка рассылки в выбранные чаты."""
    logger.info("[BROADCAST] handle_send_selected_chats called")
    
    if not callback.message:
        logger.warning("[BROADCAST] handle_send_selected_chats: no callback.message")
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        logger.warning("[BROADCAST] handle_send_selected_chats: admin check failed")
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        logger.warning("[BROADCAST] handle_send_selected_chats: user ownership check failed")
        return
    
    data = await state.get_data()
    logger.info(f"[BROADCAST] handle_send_selected_chats: state data keys: {list(data.keys())}")
    
    # Получаем выбранные чаты
    selected_chat_ids: List[int] = data.get("selected_chat_ids", [])
    logger.info(f"[BROADCAST] handle_send_selected_chats: selected_chat_ids={selected_chat_ids}")
    
    if not selected_chat_ids:
        logger.warning("[BROADCAST] handle_send_selected_chats: no selected chats")
        await callback.answer("❌ Выберите хотя бы один чат", show_alert=True)
        return

    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    if not text_final and not media_json:
        await callback.answer("❌ Нет данных рассылки, начните заново /broadcast", show_alert=True)
        await state.clear()
        return

    await callback.answer()
    await state.update_data(pending_send_mode="selected_chats")
    await state.set_state(BroadcastState.choosing_send_type)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить сейчас", callback_data="broadcast:send_now")],
        [InlineKeyboardButton(text="📅 Раз в неделю (в 10:00)", callback_data="broadcast:schedule:weekly")],
        [InlineKeyboardButton(text="📆 Раз в месяц (в 10:00)", callback_data="broadcast:schedule:monthly")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel_send")],
    ])
    await callback.message.answer(
        "⏰ <b>Как отправить?</b>\n\nВремя для повторяющихся рассылок всегда 10:00 по таймзоне бота.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


async def _show_regions_selection(
    message: Message,
    state: FSMContext,
    regions: List[str],
    selected_regions: List[str],
    page: int = 0,
    regions_per_page: int = 20
) -> None:
    """Показывает список регионов для выбора с пагинацией."""
    total_regions = len(regions)
    start_idx = page * regions_per_page
    end_idx = min(start_idx + regions_per_page, total_regions)
    page_regions = regions[start_idx:end_idx]
    
    # Получить текст рассылки из state
    data = await state.get_data()
    text_final = data.get("text_final", "")
    
    # Формируем текст сообщения
    text = f"🌍 <b>Выберите регионы для рассылки</b>\n\n"
    
    # Добавить текст рассылки, если есть
    if text_final:
        # Обрезаем длинный текст для превью (максимум 200 символов)
        preview_text = text_final[:200] + "..." if len(text_final) > 200 else text_final
        text += f"<b>Текст рассылки:</b>\n{preview_text}\n\n"
    
    text += f"Выбрано: {len(selected_regions)} из {total_regions}\n\n"
    
    if not page_regions:
        text += "Нет регионов для отображения."
    else:
        text += "Доступные регионы:\n\n"
    
    # Формируем кнопки для регионов
    # Используем индексы вместо полных названий для callback_data (лимит Telegram 64 байта)
    buttons = []
    for idx, region in enumerate(page_regions):
        # Вычисляем глобальный индекс региона в полном списке
        global_idx = start_idx + idx
        is_selected = region in selected_regions
        
        checkbox = "☑" if is_selected else "☐"
        buttons.append([
            InlineKeyboardButton(
                text=f"{checkbox} {region}",
                callback_data=f"broadcast:region_toggle:{global_idx}"
            )
        ])
    
    # Кнопки навигации (если нужно)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(text="◀ Назад", callback_data=f"broadcast:regions_page:{page - 1}")
        )
    if end_idx < total_regions:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперед ▶", callback_data=f"broadcast:regions_page:{page + 1}")
        )
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Кнопки действий
    action_buttons = []
    if selected_regions:
        action_buttons.append(
            InlineKeyboardButton(text="✅ Отправить в выбранные регионы", callback_data="broadcast:send:selected_regions")
        )
    action_buttons.append(
        InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel_send")
    )
    buttons.append(action_buttons)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    # Отправляем или обновляем сообщение
    data = await state.get_data()
    selection_message_id = data.get("regions_selection_message_id")
    
    if selection_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=selection_message_id,
                text=text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning("[BROADCAST] edit_message_text (regions) не удалось, отправляем новое: %s", e, exc_info=True)
            sent_msg = await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            await state.update_data(regions_selection_message_id=sent_msg.message_id)
    else:
        sent_msg = await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        await state.update_data(regions_selection_message_id=sent_msg.message_id)


@router.callback_query(F.data == "broadcast:send:selected_regions")
async def handle_send_selected_regions(callback: CallbackQuery, state: FSMContext) -> None:
    """Отправка рассылки в выбранные регионы."""
    logger.info("[BROADCAST] handle_send_selected_regions called")
    
    if not callback.message:
        logger.warning("[BROADCAST] handle_send_selected_regions: no callback.message")
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        logger.warning("[BROADCAST] handle_send_selected_regions: admin check failed")
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        logger.warning("[BROADCAST] handle_send_selected_regions: user ownership check failed")
        return
    
    data = await state.get_data()
    logger.info(f"[BROADCAST] handle_send_selected_regions: state data keys: {list(data.keys())}")
    
    # Получаем выбранные регионы
    selected_regions: List[str] = data.get("selected_regions", [])
    logger.info(f"[BROADCAST] handle_send_selected_regions: selected_regions={selected_regions}")
    
    if not selected_regions:
        logger.warning("[BROADCAST] handle_send_selected_regions: no selected regions")
        await callback.answer("❌ Выберите хотя бы один регион", show_alert=True)
        return
    
    # Получаем chat_id чатов из выбранных регионов
    chat_ids = await asyncio.to_thread(read_chats_by_regions, selected_regions)
    logger.info(f"[BROADCAST] handle_send_selected_regions: found {len(chat_ids)} chats in selected regions")
    
    if not chat_ids:
        await callback.answer("❌ В выбранных регионах нет активных чатов", show_alert=True)
        return

    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    if not text_final and not media_json:
        await callback.answer("❌ Нет данных рассылки, начните заново /broadcast", show_alert=True)
        await state.clear()
        return

    await callback.answer()
    await state.update_data(pending_send_mode="selected_regions")
    await state.set_state(BroadcastState.choosing_send_type)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить сейчас", callback_data="broadcast:send_now")],
        [InlineKeyboardButton(text="📅 Раз в неделю (в 10:00)", callback_data="broadcast:schedule:weekly")],
        [InlineKeyboardButton(text="📆 Раз в месяц (в 10:00)", callback_data="broadcast:schedule:monthly")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel_send")],
    ])
    await callback.message.answer(
        "⏰ <b>Как отправить?</b>\n\nВремя для повторяющихся рассылок всегда 10:00 по таймзоне бота.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data.startswith("broadcast:send:"))
async def handle_send_broadcast(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка финальной отправки рассылки по выбранной аудитории."""
    # Исключаем selected_chats и selected_regions - для них есть отдельные обработчики
    if callback.data == "broadcast:send:selected_chats" or callback.data == "broadcast:send:selected_regions":
        return
    
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
    
    if callback.data == "broadcast:send:users":
        mode = "users"
    elif callback.data == "broadcast:send:chats":
        mode = "chats"
    elif callback.data == "broadcast:send:users_chats":
        mode = "users_chats"
    else:
        await callback.answer("❌ Неизвестная команда")
        return

    await callback.answer()
    await state.update_data(pending_send_mode=mode)
    await state.set_state(BroadcastState.choosing_send_type)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить сейчас", callback_data="broadcast:send_now")],
        [InlineKeyboardButton(text="📅 Раз в неделю (в 10:00)", callback_data="broadcast:schedule:weekly")],
        [InlineKeyboardButton(text="📆 Раз в месяц (в 10:00)", callback_data="broadcast:schedule:monthly")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel_send")],
    ])
    await callback.message.answer(
        "⏰ <b>Как отправить?</b>\n\nВремя для повторяющихся рассылок всегда 10:00 по таймзоне бота.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


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


def _get_mode_extra_from_state(data: Dict[str, Any], mode: str) -> Optional[Dict[str, Any]]:
    if mode == "selected_chats":
        ids = data.get("selected_chat_ids", [])
        return {"chat_ids": ids} if ids else None
    if mode == "selected_regions":
        regions = data.get("selected_regions", [])
        return {"regions": regions} if regions else None
    return None


@router.callback_query(F.data == "broadcast:send_now")
async def handle_send_now(callback: CallbackQuery, state: FSMContext) -> None:
    """Отправить рассылку сейчас (без расписания)."""
    if not callback.message:
        await callback.answer()
        return
    if not await _require_admin(callback):
        await callback.answer()
        return
    if not await _check_user_owns_broadcast(callback, state):
        return

    data = await state.get_data()
    mode = data.get("pending_send_mode")
    if not mode:
        await callback.answer("❌ Нет выбранного режима, начните заново /broadcast", show_alert=True)
        await state.clear()
        return

    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    if not text_final and not media_json:
        await callback.answer("❌ Нет данных рассылки", show_alert=True)
        await state.clear()
        return

    mode_extra = _get_mode_extra_from_state(data, mode)
    created_by_user_id = callback.from_user.id if callback.from_user else 0
    created_by_username = callback.from_user.username if callback.from_user else None
    text_original = data.get("text_original", "")
    selected_variant = data.get("selected_variant", "original")

    await callback.answer("📤 Рассылка начата...")
    broadcast_id, sent_ok, sent_fail = await execute_broadcast(
        callback.message.bot,
        text_final=text_final,
        media_json=media_json,
        mode=mode,
        mode_extra=mode_extra,
        created_by_user_id=created_by_user_id,
        created_by_username=created_by_username,
        text_original=text_original,
        selected_variant=selected_variant,
    )
    total = sent_ok + sent_fail
    await asyncio.to_thread(
        log_event,
        user_id=created_by_user_id,
        username=created_by_username,
        event="broadcast_sent",
        meta={"broadcast_id": broadcast_id, "mode": mode, "total": total, "ok": sent_ok, "fail": sent_fail},
    )
    await callback.message.answer(
        f"✅ <b>Рассылка отправлена</b>\n\n📊 Всего: {total}, успешно: {sent_ok}, ошибок: {sent_fail}\n\nID: <code>{broadcast_id}</code>",
        parse_mode=ParseMode.HTML,
    )
    await state.clear()


@router.callback_query(F.data == "broadcast:schedule:weekly")
async def handle_schedule_weekly(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор дня недели для еженедельной рассылки."""
    if not callback.message:
        await callback.answer()
        return
    if not await _require_admin(callback):
        await callback.answer()
        return
    if not await _check_user_owns_broadcast(callback, state):
        return
    await callback.answer()
    await state.set_state(BroadcastState.choosing_weekday)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пн", callback_data="broadcast:weekday:0"), InlineKeyboardButton(text="Вт", callback_data="broadcast:weekday:1"), InlineKeyboardButton(text="Ср", callback_data="broadcast:weekday:2")],
        [InlineKeyboardButton(text="Чт", callback_data="broadcast:weekday:3"), InlineKeyboardButton(text="Пт", callback_data="broadcast:weekday:4"), InlineKeyboardButton(text="Сб", callback_data="broadcast:weekday:5"), InlineKeyboardButton(text="Вс", callback_data="broadcast:weekday:6")],
    ])
    await callback.message.answer("📅 Выберите день недели (рассылка в 10:00):", reply_markup=keyboard)


@router.callback_query(F.data == "broadcast:schedule:monthly")
async def handle_schedule_monthly(callback: CallbackQuery, state: FSMContext) -> None:
    """Запрос числа месяца для ежемесячной рассылки."""
    if not callback.message:
        await callback.answer()
        return
    if not await _require_admin(callback):
        await callback.answer()
        return
    if not await _check_user_owns_broadcast(callback, state):
        return
    await callback.answer()
    await state.set_state(BroadcastState.choosing_month_day)
    await callback.message.answer("📆 Введите число месяца (1–31), когда отправлять рассылку (в 10:00):")


@router.callback_query(F.data.startswith("broadcast:weekday:"))
async def handle_weekday(callback: CallbackQuery, state: FSMContext) -> None:
    """Сохранение еженедельной плановой рассылки."""
    if not callback.message:
        await callback.answer()
        return
    if not await _require_admin(callback):
        await callback.answer()
        return
    if not await _check_user_owns_broadcast(callback, state):
        return

    try:
        weekday = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    if weekday < 0 or weekday > 6:
        await callback.answer("❌ Неверный день", show_alert=True)
        return

    data = await state.get_data()
    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    mode = data.get("pending_send_mode", "chats")
    if not text_final and not media_json:
        await callback.answer("❌ Нет данных рассылки", show_alert=True)
        await state.clear()
        return

    mode_extra = _get_mode_extra_from_state(data, mode)
    if mode in ("selected_chats", "selected_regions") and not mode_extra:
        await callback.answer("❌ Нет выбранной аудитории", show_alert=True)
        return

    created_by_user_id = callback.from_user.id if callback.from_user else 0
    created_by_username = callback.from_user.username if callback.from_user else None
    mode_extra_str = json.dumps(mode_extra, ensure_ascii=False) if mode_extra else "{}"
    title = await asyncio.to_thread(generate_broadcast_title, text_final or "Рассылка без текста")

    schedule_id = await asyncio.to_thread(
        create_scheduled_broadcast,
        created_by_user_id=created_by_user_id,
        created_by_username=created_by_username,
        text_final=text_final,
        media_json=media_json,
        mode=mode,
        mode_extra=mode_extra_str,
        schedule_type="weekly",
        schedule_config={"weekday": weekday},
        title=title,
    )
    next_run = compute_next_run_iso("weekly", {"weekday": weekday})
    await callback.answer("✅ Плановая рассылка создана")
    await callback.message.answer(
        f"✅ <b>Плановая рассылка создана</b>\n\nСледующая отправка: {next_run[:16].replace('T', ' ')} UTC\n\nID: <code>{schedule_id}</code>",
        parse_mode=ParseMode.HTML,
    )
    await state.clear()


@router.message(BroadcastState.choosing_month_day, F.text)
async def handle_month_day_message(message: Message, state: FSMContext) -> None:
    """Ввод числа месяца для ежемесячной рассылки."""
    if not await _require_admin(message):
        return
    text = (message.text or "").strip()
    try:
        day = int(text)
    except ValueError:
        await message.answer("Введите число от 1 до 31.")
        return
    if day < 1 or day > 31:
        await message.answer("Введите число от 1 до 31.")
        return

    data = await state.get_data()
    text_final = data.get("text_final", "")
    media_json = data.get("media_json", "")
    mode = data.get("pending_send_mode", "chats")
    if not text_final and not media_json:
        await message.answer("❌ Нет данных рассылки. Начните заново /broadcast")
        await state.clear()
        return

    mode_extra = _get_mode_extra_from_state(data, mode)
    if mode in ("selected_chats", "selected_regions") and not mode_extra:
        await message.answer("❌ Нет выбранной аудитории")
        await state.clear()
        return

    created_by_user_id = message.from_user.id if message.from_user else 0
    created_by_username = message.from_user.username if message.from_user else None
    mode_extra_str = json.dumps(mode_extra, ensure_ascii=False) if mode_extra else "{}"
    title = await asyncio.to_thread(generate_broadcast_title, text_final or "Рассылка без текста")

    schedule_id = await asyncio.to_thread(
        create_scheduled_broadcast,
        created_by_user_id=created_by_user_id,
        created_by_username=created_by_username,
        text_final=text_final,
        media_json=media_json,
        mode=mode,
        mode_extra=mode_extra_str,
        schedule_type="monthly",
        schedule_config={"day": day},
        title=title,
    )
    next_run = compute_next_run_iso("monthly", {"day": day})
    await message.answer(
        f"✅ <b>Плановая рассылка создана</b>\n\nСледующая отправка: {next_run[:16].replace('T', ' ')} UTC\n\nID: <code>{schedule_id}</code>",
        parse_mode=ParseMode.HTML,
    )
    await state.clear()


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
    
    # Переход в состояние выбора типа сегментации
    await state.set_state(BroadcastState.choosing_segmentation_type)
    
    # Показываем выбор типа сегментации
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌍 По Регионам", callback_data="broadcast:segmentation:regions")],
        [InlineKeyboardButton(text="🏢 По ИП", callback_data="broadcast:segmentation:ip")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel_send")],
    ])
    
    await callback.message.answer(
        "📋 <b>Выберите тип сегментации</b>\n\n"
        "Как вы хотите выбрать получателей?",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )


@router.callback_query(F.data == "broadcast:segmentation:ip")
async def handle_segmentation_ip(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка выбора сегментации 'По ИП' - показывает список всех чатов."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    await state.set_state(BroadcastState.selecting_chats)
    
    # Инициализируем список выбранных чатов
    data = await state.get_data()
    if "selected_chat_ids" not in data:
        await state.update_data(selected_chat_ids=[])
    
    # Читаем список доступных чатов
    chats = await asyncio.to_thread(read_active_recipients_chats_with_names)
    
    if not chats:
        await callback.message.answer("❌ Нет доступных чатов для выбора.")
        return
    
    # Сохраняем список чатов в state
    await state.update_data(available_chats=chats)
    
    # Показываем список чатов
    await _show_chats_selection(callback.message, state, chats, [])


@router.callback_query(F.data == "broadcast:segmentation:regions")
async def handle_segmentation_regions(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка выбора сегментации 'По Регионам' - показывает список регионов."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    await callback.answer()
    
    await state.set_state(BroadcastState.selecting_regions)
    
    # Инициализируем список выбранных регионов
    data = await state.get_data()
    if "selected_regions" not in data:
        await state.update_data(selected_regions=[])
    
    # Читаем список доступных регионов
    regions = await asyncio.to_thread(read_active_regions)
    
    if not regions:
        await callback.message.answer("❌ Нет доступных регионов для выбора.")
        return
    
    # Сохраняем список регионов в state
    await state.update_data(available_regions=regions)
    
    # Показываем список регионов
    await _show_regions_selection(callback.message, state, regions, [])


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
        except Exception as e:
            logger.warning("[BROADCAST] edit_message_text (selection) не удалось, отправляем новое: %s", e, exc_info=True)
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


@router.callback_query(F.data.startswith("broadcast:region_toggle:"))
async def handle_region_toggle(callback: CallbackQuery, state: FSMContext) -> None:
    """Переключение выбора региона (добавить/убрать из списка)."""
    if not callback.message:
        await callback.answer()
        return
    
    if not await _require_admin(callback):
        await callback.answer()
        return
    
    if not await _check_user_owns_broadcast(callback, state):
        return
    
    # Извлекаем индекс региона из callback.data
    try:
        region_idx = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка обработки", show_alert=True)
        return
    
    data = await state.get_data()
    selected_regions: List[str] = data.get("selected_regions", [])
    available_regions: List[str] = data.get("available_regions", [])
    
    # Проверяем валидность индекса
    if region_idx < 0 or region_idx >= len(available_regions):
        await callback.answer("❌ Ошибка: неверный индекс региона", show_alert=True)
        return
    
    # Получаем название региона по индексу
    region = available_regions[region_idx]
    
    # Переключаем выбор
    if region in selected_regions:
        selected_regions.remove(region)
    else:
        selected_regions.append(region)
    
    # Обновляем state
    await state.update_data(selected_regions=selected_regions)
    
    # Определяем текущую страницу (по умолчанию 0)
    current_page = data.get("regions_page", 0)
    
    # Обновляем сообщение
    await _show_regions_selection(callback.message, state, available_regions, selected_regions, current_page)
    await callback.answer()


@router.callback_query(F.data.startswith("broadcast:regions_page:"))
async def handle_regions_page(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка переключения страницы списка регионов."""
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
    selected_regions: List[str] = data.get("selected_regions", [])
    available_regions: List[str] = data.get("available_regions", [])
    
    # Сохраняем текущую страницу
    await state.update_data(regions_page=page)
    
    # Обновляем сообщение
    await _show_regions_selection(callback.message, state, available_regions, selected_regions, page)
    await callback.answer()
