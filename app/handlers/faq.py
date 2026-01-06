"""Хендлеры для работы с частыми вопросами франчайзи (/faq)."""

import asyncio
import json
from typing import Set, Optional, List, Dict, Any

from aiogram import Router, F
from aiogram.enums import ChatAction, ParseMode
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from app.config import MANAGER_CHAT_ID
from app.services.faq_service import find_similar_question
from app.services.openai_client import adapt_faq_answer
from app.services.auth_service import find_user_by_telegram_id
from app.services.metrics_service import log_event
from app.services.pending_questions_service import create_ticket

router = Router()

# Пользователи, от которых мы ждём вопрос после /faq
PENDING_FAQ_USERS: Set[int] = set()


def _manager_reply_keyboard(ticket_id: str) -> InlineKeyboardMarkup:
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


@router.message(Command("faq"))
async def cmd_faq(message: Message) -> None:
    """Команда /faq — включает режим ожидания вопроса для авторизованных пользователей."""
    user_id = message.from_user.id

    # 1) Проверяем авторизацию
    user = find_user_by_telegram_id(user_id)
    if not user:
        await message.answer(
            "🔐 Доступ к базе FAQ только для авторизованных пользователей.\n\n"
            "Нажмите /start → «Авторизация» или отправьте /login и введите код."
        )
        return

    # 2) Включаем режим ожидания вопроса
    PENDING_FAQ_USERS.add(user_id)

    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="faq_mode_enter",
    )

    await message.answer(
        "✉️ Напишите, пожалуйста, ваш вопрос по работе Воблабир.\n"
        "Я попробую найти ответ в базе часто задаваемых вопросов."
    )


@router.message(F.text)
async def handle_faq_question(message: Message) -> None:
    """Обработка текста как потенциального вопроса франчайзи.

    Срабатывает только если пользователь до этого вызвал /faq.
    """
    user_id = message.from_user.id

    # Если пользователь не в режиме FAQ — даём обработать другим хендлерам
    if user_id not in PENDING_FAQ_USERS:
        return

    # Выходим из режима FAQ для этого пользователя
    PENDING_FAQ_USERS.discard(user_id)

    user_question = message.text.strip()
    if not user_question:
        await message.answer("Я не увидел вопроса. Напишите, пожалуйста, текстом 🙏")
        return

    # Данные пользователя из таблицы авторизации (для телефона/юр.лица)
    auth_user = find_user_by_telegram_id(user_id)

    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="faq_question_submitted",
        meta={"text": user_question},
    )

    await message.answer("🔎 Ищу ответ в базе часто задаваемых вопросов...")
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    # Ищем похожий вопрос
    match = await find_similar_question(user_question)

    if match is not None:
        log_event(
            user_id=user_id,
            username=message.from_user.username,
            event="faq_answer_found",
            meta={"matched_question": match.get("question", "")},
        )

        base_answer = match["answer"]

        # Печатает перед адаптацией
        await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

        adapted_text = await asyncio.to_thread(
            adapt_faq_answer,
            user_question,
            base_answer,
        )

        await message.answer(adapted_text)

        # Отправляем медиа-вложения, если есть (все вложения)
        media_json = match.get("media_json", "")
        if media_json:
            try:
                from aiogram.types import InputMediaPhoto, InputMediaVideo
                
                attachments: List[Dict[str, Any]] = json.loads(media_json)
                if attachments:
                    photos = [att for att in attachments if att.get("type") == "photo"]
                    videos = [att for att in attachments if att.get("type") == "video"]
                    documents = [att for att in attachments if att.get("type") == "document"]
                    
                    # Отправляем фото батчами по 10
                    for i in range(0, len(photos), 10):
                        batch = photos[i:i+10]
                        media_group = []
                        for idx, att in enumerate(batch):
                            caption = att.get("caption", "") if idx == 0 else None
                            media_group.append(InputMediaPhoto(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None))
                        if media_group:
                            await message.bot.send_media_group(chat_id=message.chat.id, media=media_group)
                    
                    # Отправляем видео батчами по 10
                    for i in range(0, len(videos), 10):
                        batch = videos[i:i+10]
                        media_group = []
                        for idx, att in enumerate(batch):
                            caption = att.get("caption", "") if idx == 0 else None
                            media_group.append(InputMediaVideo(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None))
                        if media_group:
                            await message.bot.send_media_group(chat_id=message.chat.id, media=media_group)
                    
                    # Отправляем документы по одному
                    for att in documents:
                        caption = att.get("caption", "")
                        await message.bot.send_document(
                            chat_id=message.chat.id,
                            document=att["file_id"],
                            caption=caption or None,
                            parse_mode=ParseMode.HTML if caption else None
                        )
                    
                    log_event(
                        user_id=user_id,
                        username=message.from_user.username,
                        event="faq_media_sent",
                        meta={"matched_question": match.get("question", "")},
                    )
            except Exception as e:
                # Логируем ошибку, но не прерываем выполнение
                logger.exception(f"[FAQ] Error sending media: {e}")

        return

    # --- НЕ НАШЛИ ОТВЕТ ---
    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="faq_answer_not_found",
        meta={"text": user_question},
    )

    await message.answer(
        "Пока у меня нет готового ответа на этот вопрос. "
        "Я передал его менеджерам — скоро вернёмся с ответом 🙏"
    )

    # Создаём тикет в pending_questions
    ticket_id = create_ticket(
        user_id=user_id,
        username=message.from_user.username,
        name=(auth_user.name if auth_user else (message.from_user.full_name or "")),
        phone=(auth_user.phone if auth_user else ""),
        legal_entity=(auth_user.legal_entity if auth_user else ""),
        question=user_question,
    )

    log_event(
        user_id=user_id,
        username=message.from_user.username,
        event="pending_ticket_created",
        meta={"ticket_id": ticket_id},
    )

    # Уведомляем менеджеров с кнопкой "Ответить"
    if MANAGER_CHAT_ID:
        manager_text = _format_manager_card(
            ticket_id=ticket_id,
            user_id=user_id,
            username=message.from_user.username,
            full_name=message.from_user.full_name,
            phone=(auth_user.phone if auth_user else ""),
            legal_entity=(auth_user.legal_entity if auth_user else ""),
            question=user_question,
        )

        await message.bot.send_message(
            chat_id=MANAGER_CHAT_ID,
            text=manager_text,
            reply_markup=_manager_reply_keyboard(ticket_id),
        )