import asyncio
import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.enums import ParseMode, ChatAction

from app.services.auth_service import find_user_by_telegram_id
from app.services.faq_service import find_similar_question
from app.services.metrics_service import alog_event  # async-логгер
from app.services.openai_client import polish_faq_answer, create_embedding, client, CHAT_MODEL
from app.services.openai_client import check_answer_grounding, generate_answer_from_full_document
from app.services.qdrant_service import get_qdrant_service
from app.services.pending_questions_service import create_ticket_and_notify_managers
from app.services.qa_feedback_service import save_qa_feedback
from app.services.reranking_service import rerank_chunks_with_llm, select_best_chunks
from app.services.kilbil_service import get_article_urls_from_chunks
from app.services.chunk_analyzer_service import (
    analyze_chunks_relevance,
    select_and_combine_chunks,
    extract_key_information,
)
from app.services.conversation_phrases import get_phrases_examples
from app.ui.keyboards import qa_kb, main_menu_kb
from app.config import (
    MAX_CLARIFICATION_ROUNDS,
    MIN_SCORE_AFTER_RERANK,
    USE_HYBRID_BM25,
    USE_HYDE,
    USE_FULL_FILE_CONTEXT,
    USE_CROSS_ENCODER_RERANK,
    COHERE_API_KEY,
    RERANK_CANDIDATES_LIMIT,
)

logger = logging.getLogger(__name__)

router = Router()


class QAMode(StatesGroup):
    active = State()


class FeedbackState(StatesGroup):
    waiting_helped = State()
    waiting_completeness = State()
    waiting_clarity = State()
    waiting_comment = State()


def _kb_helped() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Помог", callback_data="fb_helped:yes"),
            InlineKeyboardButton(text="🤏 Частично", callback_data="fb_helped:partial"),
            InlineKeyboardButton(text="❌ Не помог", callback_data="fb_helped:no"),
        ]
    ])


def _kb_stars(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⭐1", callback_data=f"{prefix}:1"),
        InlineKeyboardButton(text="⭐2", callback_data=f"{prefix}:2"),
        InlineKeyboardButton(text="⭐3", callback_data=f"{prefix}:3"),
        InlineKeyboardButton(text="⭐4", callback_data=f"{prefix}:4"),
        InlineKeyboardButton(text="⭐5", callback_data=f"{prefix}:5"),
    ]])


def _kb_skip_comment() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Пропустить", callback_data="fb_skip_comment"),
    ]])


async def detect_question_type(
    question: str,
    conversation_history: List[Dict[str, Any]],
) -> str:
    """Определяет тип вопроса: новый, уточнение, follow-up.
    
    Args:
        question: Текущий вопрос
        conversation_history: История диалога
    
    Returns:
        "new" | "clarification" | "follow_up"
    """
    if not conversation_history:
        return "new"
    
    # Проверяем, был ли последний ответ уточняющим вопросом
    last_assistant = None
    for msg in reversed(conversation_history):
        if msg.get("role") == "assistant":
            last_assistant = msg
            break
    
    if last_assistant and "уточнения" in last_assistant.get("text", "").lower():
        return "clarification"
    
    # Проверяем, является ли вопрос follow-up (ссылается на предыдущий ответ)
    if last_assistant:
        # Используем LLM для определения связи
        try:
            last_answer = last_assistant.get("text", "")
            prompt = (
                f"Предыдущий ответ бота: {last_answer[:300]}\n\n"
                f"Новый вопрос пользователя: {question}\n\n"
                "Определи, является ли новый вопрос уточнением или продолжением предыдущего ответа, "
                "или это совершенно новый вопрос.\n"
                "Ответь одним словом: 'follow_up' если вопрос связан с предыдущим ответом, "
                "'new' если это новый вопрос."
            )
            
            resp = await asyncio.to_thread(
                client.chat.completions.create,
                model=CHAT_MODEL,
                messages=[
                    {"role": "system", "content": "Ты помощник для определения типа вопроса."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                max_tokens=10,
            )
            
            answer = (resp.choices[0].message.content or "").strip().lower()
            if "follow_up" in answer:
                return "follow_up"
        except Exception as e:
            logger.exception(f"[QA_MODE] Ошибка определения типа вопроса: {e}")
    
    return "new"


async def is_follow_up_question(
    question: str,
    last_answer: str,
) -> bool:
    """Определяет, является ли вопрос уточнением предыдущего ответа.
    
    Args:
        question: Текущий вопрос
        last_answer: Последний ответ бота
    
    Returns:
        True если вопрос связан с предыдущим ответом
    """
    if not last_answer:
        return False
    
    try:
        prompt = (
            f"Предыдущий ответ: {last_answer[:400]}\n\n"
            f"Новый вопрос: {question}\n\n"
            "Является ли новый вопрос уточнением или продолжением предыдущего ответа? "
            "Ответь 'да' или 'нет'."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник для определения связи между вопросами."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=5,
        )
        
        answer = (resp.choices[0].message.content or "").strip().lower()
        return "да" in answer or "yes" in answer
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка определения follow-up вопроса: {e}")
        return False


async def detect_clarification_response_vs_new_question(
    user_message: str,
    last_bot_message: str,
    original_question: str,
) -> str:
    """Определяет: ответ на уточняющий вопрос или пользователь задаёт новый вопрос.

    Returns:
        "clarification_response" | "new_question"
    """
    if not user_message.strip() or not last_bot_message.strip():
        return "clarification_response"
    try:
        prompt = (
            f"Бот задал уточняющий вопрос к теме: {original_question[:300]}\n\n"
            f"Уточняющий вопрос бота: {last_bot_message[:400]}\n\n"
            f"Сообщение пользователя: {user_message}\n\n"
            "Это ответ на уточнение или пользователь перешёл к новому/другому вопросу? "
            "Ответь одним словом: clarification_response или new_question."
        )
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник. Определи тип сообщения: ответ на уточнение или новый вопрос. Ответь только clarification_response или new_question."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=20,
        )
        answer = (resp.choices[0].message.content or "").strip().lower()
        if "new_question" in answer or "new" in answer.split():
            logger.info("[QA_MODE] LLM: clarification_response vs new_question -> new_question")
            return "new_question"
        logger.info("[QA_MODE] LLM: clarification_response vs new_question -> clarification_response")
        return "clarification_response"
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка detect_clarification_response_vs_new_question, fallback clarification_response: {e}")
        return "clarification_response"


async def detect_new_question_vs_follow_up(user_message: str, last_bot_answer: str) -> str:
    """Определяет: новый самостоятельный вопрос или реакция/уточнение к предыдущему ответу.

    Returns:
        "new_question" | "follow_up"
    """
    if not user_message.strip() or not last_bot_answer.strip():
        return "new_question"
    try:
        prompt = (
            f"Последний ответ бота: {last_bot_answer[:400]}\n\n"
            f"Новое сообщение пользователя: {user_message}\n\n"
            "Это новый самостоятельный вопрос (другая тема, другая область — например было про маркировку товаров, стало про вывески/оформление) или реакция/уточнение к предыдущему ответу "
            "(спасибо, ок, «а как…», «подробнее»)? Если вопрос явно про другую область — это new_question. Ответь одним словом: new_question или follow_up."
        )
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник. Определи: новый вопрос (другая тема/область) или реакция/уточнение к ответу. Явная смена темы = new_question. Ответь только new_question или follow_up."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=10,
        )
        answer = (resp.choices[0].message.content or "").strip().lower()
        if "follow_up" in answer or "follow" in answer:
            logger.info("[QA_MODE] LLM: new_question vs follow_up -> follow_up")
            return "follow_up"
        logger.info("[QA_MODE] LLM: new_question vs follow_up -> new_question")
        return "new_question"
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка detect_new_question_vs_follow_up, fallback new_question: {e}")
        return "new_question"


async def detect_topic_shift(
    current_question: str,
    conversation_history: List[Dict[str, Any]],
) -> tuple[bool, Optional[str]]:
    """Определяет, сменилась ли тема разговора.
    
    Args:
        current_question: Текущий вопрос пользователя
        conversation_history: История диалога
    
    Returns:
        (is_topic_shift: bool, previous_topic: str | None)
        previous_topic - краткое описание предыдущей темы (если тема сменилась)
    """
    if not conversation_history or len(conversation_history) < 2:
        return (False, None)
    
    try:
        # Извлекаем последние сообщения для определения предыдущей темы
        recent_messages = []
        for msg in reversed(conversation_history[-5:]):
            if msg.get("role") == "user":
                recent_messages.insert(0, msg.get("text", ""))
            elif msg.get("role") == "assistant":
                recent_messages.insert(0, msg.get("text", "")[:300])
            if len(recent_messages) >= 3:
                break
        
        if not recent_messages:
            return (False, None)
        
        previous_context = "\n".join(recent_messages[:-1]) if len(recent_messages) > 1 else recent_messages[0]
        
        prompt = (
            f"Контекст предыдущего разговора:\n{previous_context}\n\n"
            f"Новый вопрос пользователя: {current_question}\n\n"
            "Определи, сменилась ли тема разговора. "
            "Тема сменилась, если новый вопрос относится к совершенно другой области, "
            "не связанной с предыдущим обсуждением.\n\n"
            "Ответь в формате:\n"
            "ТЕМА_СМЕНИЛАСЬ: да/нет\n"
            "ПРЕДЫДУЩАЯ_ТЕМА: краткое описание (1-3 слова, только если тема сменилась)"
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник для определения смены темы в диалоге."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=50,
        )
        
        answer = (resp.choices[0].message.content or "").strip()
        
        # Парсим ответ
        is_shift = "ТЕМА_СМЕНИЛАСЬ: да" in answer.lower() or "тема сменилась: да" in answer.lower()
        previous_topic = None
        
        if is_shift:
            # Извлекаем предыдущую тему
            if "ПРЕДЫДУЩАЯ_ТЕМА:" in answer:
                previous_topic = answer.split("ПРЕДЫДУЩАЯ_ТЕМА:")[1].strip().split("\n")[0].strip()
            elif "предыдущая тема:" in answer.lower():
                previous_topic = answer.split("предыдущая тема:")[1].strip().split("\n")[0].strip()
            
            if not previous_topic or len(previous_topic) > 50:
                # Если не удалось извлечь, определяем из контекста
                previous_topic = "предыдущая тема"
        
        logger.info(
            f"[QA_MODE] Определение смены темы: is_shift={is_shift}, "
            f"previous_topic={previous_topic}"
        )
        
        return (is_shift, previous_topic)
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка определения смены темы: {e}")
        return (False, None)


async def check_question_context_sufficiency(
    question: str,
    conversation_history: List[Dict[str, Any]],
    is_topic_shift: bool = False,
) -> tuple[bool, Optional[str]]:
    """Проверяет, достаточно ли контекста в вопросе для поиска.
    
    Args:
        question: Текущий вопрос пользователя
        conversation_history: История диалога
        is_topic_shift: Флаг смены темы
    
    Returns:
        (sufficient: bool, missing_context: str | None)
        missing_context - описание недостающего контекста (если sufficient=False)
    """
    if not question or len(question.strip()) < 3:
        return (False, "Вопрос слишком короткий")
    
    # Быстрая проверка: очень короткие вопросы (< 10 символов без пробелов)
    question_clean = question.strip()
    if len(question_clean) < 10:
        # Исключение: общие вопросы типа "что такое", "расскажи про"
        general_phrases = ["что такое", "расскажи про", "расскажи о", "что это", "про что"]
        if not any(phrase in question_clean.lower() for phrase in general_phrases):
            return (False, "Вопрос слишком короткий, нужны дополнительные детали")
    
    # Если тема сменилась, требуем больше контекста
    if is_topic_shift:
        # Для смены темы короткие вопросы недостаточны
        if len(question_clean) < 15:
            return (False, "При переходе на новую тему нужны более подробные детали")
    
    # Используем LLM для более точной оценки
    try:
        context_text = ""
        if conversation_history:
            recent_context = []
            for msg in reversed(conversation_history[-3:]):
                role = "Пользователь" if msg.get("role") == "user" else "Бот"
                text = msg.get("text", "")[:200]
                recent_context.insert(0, f"{role}: {text}")
            context_text = "\n".join(recent_context)
        
        topic_shift_note = ""
        if is_topic_shift:
            topic_shift_note = "\nВАЖНО: Пользователь перешел на новую тему. Для новой темы требуется больше контекста."
        
        prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"{'Контекст диалога:\n' + context_text + '\n\n' if context_text else ''}"
            f"{topic_shift_note}\n"
            "Оцени, достаточно ли контекста в этом вопросе для поиска информации в базе знаний.\n"
            "Вопрос должен содержать достаточно информации, чтобы можно было найти релевантные данные.\n\n"
            "Примеры недостаточного контекста:\n"
            "- 'по вывеске' (непонятно, о чем именно)\n"
            "- 'требования' (слишком общо)\n"
            "- 'как сделать' (не указано, что именно)\n\n"
            "Ответь в формате:\n"
            "ДОСТАТОЧНО: да/нет\n"
            "НЕДОСТАЕТ: краткое описание (только если нет)"
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник для оценки достаточности контекста в вопросах."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=100,
        )
        
        answer = (resp.choices[0].message.content or "").strip()
        
        # Парсим ответ
        is_sufficient = "ДОСТАТОЧНО: да" in answer.lower() or "достаточно: да" in answer.lower()
        missing_context = None
        
        if not is_sufficient:
            # Извлекаем описание недостающего контекста
            if "НЕДОСТАЕТ:" in answer:
                missing_context = answer.split("НЕДОСТАЕТ:")[1].strip().split("\n")[0].strip()
            elif "недостает:" in answer.lower():
                missing_context = answer.split("недостает:")[1].strip().split("\n")[0].strip()
            
            if not missing_context:
                missing_context = "Недостаточно контекста для поиска"
        
        logger.info(
            f"[QA_MODE] Проверка контекста: sufficient={is_sufficient}, "
            f"missing_context={missing_context}"
        )
        
        return (is_sufficient, missing_context)
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка проверки контекста: {e}")
        # При ошибке считаем контекст достаточным, чтобы не блокировать поиск
        return (True, None)


async def extract_topics_from_conversation(
    conversation_history: List[Dict[str, Any]],
) -> List[str]:
    """Извлекает основные темы из истории диалога.
    
    Args:
        conversation_history: История диалога
    
    Returns:
        Список тем (например, ["алкоголь", "договор", "магазин"])
    """
    if not conversation_history or len(conversation_history) < 2:
        return []
    
    try:
        # Собираем все сообщения пользователя
        user_messages = [msg.get("text", "") for msg in conversation_history if msg.get("role") == "user"]
        if not user_messages:
            return []
        
        conversation_text = "\n".join(user_messages[-5:])  # Последние 5 вопросов
        
        prompt = (
            f"История вопросов пользователя:\n{conversation_text}\n\n"
            "Определи основные темы, которые обсуждались в этих вопросах. "
            "Верни список тем через запятую (максимум 5 тем). "
            "Темы должны быть краткими (1-3 слова), например: 'алкоголь', 'договор', 'магазин'."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник для извлечения тем из диалога."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=100,
        )
        
        answer = (resp.choices[0].message.content or "").strip()
        # Парсим список тем
        topics = [t.strip() for t in answer.split(",") if t.strip()]
        return topics[:5]  # Максимум 5 тем
        
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка извлечения тем: {e}")
        return []


async def build_topic_summary(
    conversation_history: List[Dict[str, Any]],
) -> str:
    """Создает краткое резюме обсужденных тем для отсылок.
    
    Args:
        conversation_history: История диалога
    
    Returns:
        Краткое резюме тем (например, "Ранее обсуждались: алкоголь, договоры, магазины")
    """
    if not conversation_history:
        return ""
    
    topics = await extract_topics_from_conversation(conversation_history)
    if not topics:
        return ""
    
    # Создаем краткое резюме
    if len(topics) == 1:
        return f"Ранее обсуждалась тема: {topics[0]}"
    elif len(topics) == 2:
        return f"Ранее обсуждались темы: {topics[0]} и {topics[1]}"
    else:
        return f"Ранее обсуждались темы: {', '.join(topics[:-1])} и {topics[-1]}"


def build_conversation_context(
    conversation_history: List[Dict[str, Any]],
    max_messages: int = 5,
    include_topics: bool = True,
) -> str:
    """Строит структурированный контекст диалога.
    
    Args:
        conversation_history: История диалога
        max_messages: Максимальное количество сообщений для включения
        include_topics: Включать ли информацию о темах
    
    Returns:
        Структурированный текст контекста
    """
    if not conversation_history:
        return ""
    
    context_parts = []
    
    # Добавляем информацию о темах, если есть
    if include_topics:
        topics_list = []
        for msg in conversation_history:
            topics = msg.get("topics", [])
            if topics:
                topics_list.extend(topics)
        
        if topics_list:
            unique_topics = list(set(topics_list))[:5]
            if unique_topics:
                context_parts.append(f"Обсуждаемые темы: {', '.join(unique_topics)}")
    
    # Добавляем сообщения
    recent_messages = conversation_history[-max_messages:]
    
    for msg in recent_messages:
        role = msg.get("role", "")
        text = msg.get("text", "")
        question_type = msg.get("question_type", "")
        source = msg.get("source", "")
        key_points = msg.get("key_points", [])
        
        if role == "user":
            prefix = "Пользователь"
            if question_type:
                prefix += f" ({question_type})"
            context_line = f"{prefix}: {text[:200]}"
            if key_points:
                context_line += f" [Ключевые моменты: {', '.join(key_points[:2])}]"
            context_parts.append(context_line)
        elif role == "assistant":
            prefix = "Бот"
            if source:
                prefix += f" [{source}]"
            answer_summary = msg.get("answer_summary", text[:150])
            context_line = f"{prefix}: {answer_summary}"
            if key_points:
                context_line += f" [Ключевые моменты: {', '.join(key_points[:2])}]"
            context_parts.append(context_line)
    
    return "\n".join(context_parts)


async def _send_media_from_json(bot, chat_id: int, media_json: str) -> None:
    """Отправляет медиа-вложения из JSON строки. Использует send_media_group для фото/видео, send_document для документов."""
    if not media_json or not media_json.strip():
        return

    try:
        from aiogram.types import InputMediaPhoto, InputMediaVideo
        
        attachments: List[Dict[str, Any]] = json.loads(media_json)
        if not attachments:
            return

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
                await bot.send_media_group(chat_id=chat_id, media=media_group)
        
        # Отправляем видео батчами по 10
        for i in range(0, len(videos), 10):
            batch = videos[i:i+10]
            media_group = []
            for idx, att in enumerate(batch):
                caption = att.get("caption", "") if idx == 0 else None
                media_group.append(InputMediaVideo(media=att["file_id"], caption=caption, parse_mode=ParseMode.HTML if caption else None))
            if media_group:
                await bot.send_media_group(chat_id=chat_id, media=media_group)
        
        # Отправляем документы по одному
        for att in documents:
            caption = att.get("caption", "")
            await bot.send_document(
                chat_id=chat_id,
                document=att["file_id"],
                caption=caption or None,
                parse_mode=ParseMode.HTML if caption else None
            )
    except Exception as e:
        logger.exception(f"[QA_MODE] Error sending media: {e}")


async def _expand_query_for_search(original_query: str) -> str:
    """Расширяет запрос пользователя для улучшения поиска в RAG.
    
    Преобразует общие запросы в более конкретные, добавляя ключевые слова
    и контекст, которые помогут найти релевантные чанки.
    
    Примеры:
    - "Расскажи про Воблабир" -> "история компании Воблабир, философия бренда, 
      развитие сети магазинов, когда основана компания, количество магазинов"
    - "Как работает доставка?" -> "доставка товаров, условия доставки, 
      стоимость доставки, время доставки, зоны доставки"
    
    Args:
        original_query: Оригинальный запрос пользователя
        
    Returns:
        Расширенный запрос с дополнительными ключевыми словами
    """
    if not original_query or not original_query.strip():
        return original_query
    
    # Нормализация названий: Воблабир и Воблаbeer - это одно и то же
    NORMALIZATION_MAP = {
        "воблабир": ["Воблаbeer", "Воблабир"],
        "воблаbeer": ["Воблаbeer", "Воблабир"],
    }
    
    # Определяем, нужно ли расширять запрос (короткие темы вроде «Регламент по открытию магазина» тоже расширяем)
    query_lower = original_query.lower().strip()
    is_short = len(original_query.strip()) < 40
    is_general = any(phrase in query_lower for phrase in [
        "расскажи про", "расскажи о", "что такое", "что это",
        "как работает", "про что",
        "как выбрать", "как подобрать", "по каким критериям",
        "чек-лист", "чек лист", "критерии", "требования к",
    ])
    
    # Проверяем, нужно ли добавить нормализацию названий
    needs_normalization = any(keyword in query_lower for keyword in NORMALIZATION_MAP.keys())
    if needs_normalization:
        logger.info(f"[QA_MODE] Обнаружено название компании в запросе, применяем нормализацию")
    
    # Если запрос длинный и конкретный, не расширяем (но все равно применяем нормализацию)
    if not is_short and not is_general and not needs_normalization:
        logger.debug(f"[QA_MODE] Запрос достаточно конкретный, расширение не требуется: '{original_query[:50]}...'")
        return original_query
    
    try:
        system_prompt = (
            "Ты помощник для расширения поисковых запросов корпоративного бота сети магазинов Воблабир.\n"
            "ВАЖНО: Воблабир (также пишется как Воблаbeer) - это сеть магазинов, компания, бренд.\n"
            "НЕ путай с другими значениями слов (например, 'воблер' - это рыболовная приманка).\n\n"
            "Твоя задача — расширить запрос пользователя, добавив связанные ключевые слова и темы,\n"
            "которые помогут найти релевантную информацию в базе знаний о компании Воблабир.\n\n"
            "Правила:\n"
            "1. Сохрани оригинальный смысл запроса.\n"
            "2. Если в запросе упоминается 'Воблабир' или 'Воблаbeer', добавь оба варианта написания.\n"
            "3. Добавь связанные ключевые слова: компания, сеть магазинов, бренд, бизнес, история, философия.\n"
            "4. Используй синонимы и связанные понятия.\n"
            "5. Для запросов про выбор места, магазина, помещения или локации обязательно добавь формулировки из базы знаний: чек-лист выбора месторасположения, требования к помещению, критерии выбора помещения, площадь 50-70, этаж, отдельный вход, первая линия, вывеска, отделка, электрическая мощность, трафикообразующие соседи.\n"
            "6. Не добавляй лишних деталей, только ключевые слова для поиска.\n"
            "7. Ответ должен быть кратким (до 100 слов).\n"
            "8. Не используй форматирование, только текст.\n"
            "9. Для коротких запросов по теме (регламент, открытие магазина, документы, касса, ИП, ЕГАИС) добавь синонимы: ноу-хау, ссылка на регламент, документы для начала работы, кассовое оборудование, подрядчик, лицензионный договор."
        )
        
        # Добавляем информацию о нормализации в user_prompt
        normalization_hint = ""
        if needs_normalization:
            normalization_hint = (
                "\n\nВАЖНО: В запросе упоминается название компании. "
                "Добавь в расширенный запрос оба варианта написания: 'Воблабир' и 'Воблаbeer'. "
                "Это одно и то же - сеть магазинов."
            )
        
        user_prompt = (
            f"Оригинальный запрос: {original_query}\n"
            f"{normalization_hint}\n\n"
            "Расширь этот запрос, добавив связанные ключевые слова и темы, "
            "которые помогут найти релевантную информацию. "
            "Используй формат: ключевое слово 1, ключевое слово 2, тема 1, тема 2..."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=150,
        )
        
        expanded = (resp.choices[0].message.content or "").strip()
        
        # Если расширение не удалось или вернуло пустой результат, используем оригинал
        if not expanded or len(expanded) < len(original_query):
            logger.warning(f"[QA_MODE] Расширение запроса не удалось, используем оригинал")
            return original_query
        
        logger.info(f"[QA_MODE] Расширен запрос: '{original_query[:50]}...' -> '{expanded[:100]}...'")
        return expanded
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка расширения запроса: {e}")
        # При ошибке возвращаем оригинальный запрос
        return original_query


async def _require_auth(obj) -> bool:
    """
    Возвращает True если авторизован, иначе отправляет сообщение и False.
    obj может быть Message или CallbackQuery (у обоих есть from_user и bot/message).
    """
    user_id = obj.from_user.id if obj.from_user else 0
    user = find_user_by_telegram_id(user_id)

    if user:
        return True

    # универсально отправляем ответ
    text = (
        "🔒 Доступ к навыку «Задать вопрос» доступен только партнёрам Воблабир.\n\n"
        "Пожалуйста, пройдите авторизацию: /start → 🔐 Авторизация."
    )

    if hasattr(obj, "message") and obj.message:
        await obj.message.answer(text)
        await obj.answer()
    else:
        await obj.answer(text)

    return False


async def _check_sufficient_data_private(
    question: str,
    found_chunks: List[Dict[str, Any]],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    is_after_clarification: bool = False,
) -> tuple[bool, Optional[str]]:
    """Проверяет через AI, достаточно ли данных для ответа (для приватных чатов)."""
    if not found_chunks:
        return (False, "Не найдено релевантных фрагментов в базе знаний")
    
    # Проверяем максимальный score
    max_score = max((chunk.get("score", 0) for chunk in found_chunks), default=0)
    
    # Определяем, является ли вопрос общим (типа "расскажи про", "что такое")
    question_lower = question.lower()
    is_general_question = any(phrase in question_lower for phrase in [
        "расскажи про", "расскажи о", "что такое", "что это", "про что"
    ])
    
    # Если после уточнений и найдено достаточно чанков, используем более мягкие критерии
    if is_after_clarification and len(found_chunks) >= 3:
        if max_score >= 0.45:  # Снижен порог для после уточнений
            logger.info(
                f"[QA_MODE] После уточнений: найдено {len(found_chunks)} чанков с max_score={max_score:.3f}, "
                f"считаем данные достаточными"
            )
            return (True, None)
    
    # Для общих вопросов используем более мягкие критерии
    if is_general_question and len(found_chunks) >= 2 and max_score >= 0.45:
        logger.info(
            f"[QA_MODE] Общий вопрос: найдено {len(found_chunks)} чанков с max_score={max_score:.3f}, "
            f"считаем данные достаточными"
        )
        return (True, None)
    
    # Если score очень высокий, считаем данные достаточными
    if max_score >= 0.65:  # Снижен с 0.75 до 0.65
        logger.info(f"[QA_MODE] Высокий score ({max_score:.3f}), считаем данные достаточными")
        return (True, None)
    
    # Для средних scores используем AI проверку
    try:
        chunks_text = "\n\n".join([
            f"Фрагмент {i+1} (релевантность: {chunk.get('score', 0):.3f}):\n{chunk.get('text', '')[:500]}"
            for i, chunk in enumerate(found_chunks[:3])
        ])
        
        context_text = ""
        if conversation_history:
            context_lines = []
            for msg in conversation_history[-3:]:
                role = "Пользователь" if msg.get("role") == "user" else "Бот"
                text = msg.get("text", "")
                # Убираем вводную фразу из уточняющих вопросов для контекста
                if "уточнения" in text.lower():
                    text = text.replace("Чтобы ответить на ваш вопрос, мне нужны некоторые уточнения.\n\n", "")
                context_lines.append(f"{role}: {text[:200]}")
            context_text = "\n".join(context_lines)
        
        # Адаптируем промпт в зависимости от того, после уточнений или нет
        is_general = any(phrase in question.lower() for phrase in [
            "расскажи про", "расскажи о", "что такое", "что это", "про что"
        ])
        
        sufficiency_instruction = (
            "Если фрагменты релевантны вопросу и содержат полезную информацию, "
            "даже если не полностью покрывают все аспекты, считай данные достаточными. "
            "Для общих вопросов (типа 'расскажи про') достаточно, если фрагменты дают общее представление о теме."
        )
        if is_after_clarification:
            sufficiency_instruction = (
                "Пользователь уже дал уточнения. Если фрагменты содержат релевантную информацию, "
                "даже частичную, можно дать ответ на основе имеющихся данных. "
                "Считай данные достаточными, если они позволяют дать полезный ответ."
            )
        elif is_general:
            sufficiency_instruction = (
                "Это общий вопрос (типа 'расскажи про'). Если фрагменты содержат информацию о теме вопроса, "
                "даже если это не полная информация, считай данные достаточными для общего ответа. "
                "Не требуй полного покрытия всех аспектов - достаточно дать общее представление."
            )
        
        prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"{'Контекст диалога:\n' + context_text + '\n\n' if context_text else ''}"
            f"Найденные фрагменты из базы знаний:\n{chunks_text}\n\n"
            "Оцени, достаточно ли этих фрагментов для ответа на вопрос пользователя.\n"
            "Учитывай контекст диалога - если пользователь уточняет предыдущий вопрос, используй этот контекст.\n"
            f"{sufficiency_instruction}\n"
            "ВАЖНО: Будь склонен к ответу 'yes', если фрагменты содержат релевантную информацию.\n"
            "Ответь 'yes' или 'no'.\n"
            "Если 'no', укажи кратко, какая информация отсутствует."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник для оценки достаточности данных для ответа. Учитывай контекст диалога и релевантность найденных фрагментов."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        
        answer = (resp.choices[0].message.content or "").strip().lower()
        
        if answer.startswith("yes"):
            return (True, None)
        else:
            missing_info = answer.replace("no", "").strip()
            if not missing_info:
                missing_info = "Недостаточно информации для полного ответа"
            return (False, missing_info)
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка проверки достаточности данных: {e}")
        return (True, None)


async def _ask_clarification_question_private(
    message: Message,
    question: str,
    found_chunks: List[Dict[str, Any]],
    missing_info: str,
    state: FSMContext,
    insufficient_context: bool = False,
    is_topic_shift: bool = False,
    previous_topic: Optional[str] = None,
) -> None:
    """Задает уточняющий вопрос пользователю (для приватных чатов).
    
    Args:
        message: Сообщение пользователя
        question: Текущий вопрос
        found_chunks: Найденные чанки (может быть пустым для режима insufficient_context)
        missing_info: Описание недостающей информации
        state: FSM контекст
        insufficient_context: Флаг, что контекста недостаточно ДО поиска
        is_topic_shift: Флаг смены темы
        previous_topic: Описание предыдущей темы (если тема сменилась)
    """
    try:
        # Формируем промпт в зависимости от режима
        if insufficient_context:
            # Режим недостаточного контекста ДО поиска
            context_note = ""
            if is_topic_shift and previous_topic:
                context_note = (
                    f"\nВАЖНО: Пользователь перешел на новую тему. "
                    f"Ранее обсуждалась тема: {previous_topic}. "
                    f"Новый вопрос слишком короткий и не содержит достаточно контекста для поиска."
                )
            elif is_topic_shift:
                context_note = (
                    "\nВАЖНО: Пользователь перешел на новую тему. "
                    "Новый вопрос слишком короткий и не содержит достаточно контекста для поиска."
                )
            
            prompt = (
                f"Пользователь спросил: {question}\n\n"
                f"Проблема: {missing_info}\n"
                f"{context_note}\n\n"
                "Сформулируй один развернутый и понятный уточняющий вопрос, который поможет понять, "
                "что именно интересует пользователя.\n"
                "Вопрос должен быть максимально конкретным и понятным, как будто ты менеджер, "
                "который хочет помочь клиенту.\n"
                "Не используй технические термины, говори простым языком.\n"
                "Вопрос должен быть полным предложением, не используй сокращения.\n"
                "Если тема сменилась, уточни, о чем именно пользователь хочет узнать.\n"
                "Если перечисляешь варианты (1), 2), 3)) — каждый вариант с новой строки."
            )
            
            system_content = (
                "Ты дружелюбный менеджер, который помогает клиентам, задавая понятные уточняющие вопросы. "
                "Когда вопрос слишком короткий или неясный, ты задаешь уточняющий вопрос, чтобы понять, "
                "что именно нужно клиенту."
            )
        else:
            # Режим недостаточности данных ПОСЛЕ поиска
            if found_chunks:
                # Есть чанки: уточнение привязано к конкретным найденным фрагментам (выбор среди них)
                _max_chunks = 5
                _chunk_chars = 400
                chunks_list = "\n\n".join([
                    f"Фрагмент {i+1} (релевантность: {chunk.get('score', 0):.2f}):\n{chunk.get('text', '')[: _chunk_chars]}"
                    for i, chunk in enumerate(found_chunks[:_max_chunks])
                ])
                missing_note = f"\nСистема отметила: {missing_info[:200]}." if missing_info else ""
                system_content = (
                    "Ты дружелюбный менеджер, который формулирует уточняющие вопросы. "
                    "Твоя задача — по перечисленным ниже фрагментам базы знаний сформулировать один уточняющий вопрос, "
                    "который поможет пользователю выбрать среди этих конкретных вариантов (или сузить тему к одному из фрагментов). "
                    "Можно использовать формат «Вас интересует» с вариантами 1), 2), 3). "
                    "Важно: каждый вариант пиши с новой строки (1) на одной строке, 2) на следующей, 3) на следующей) — так удобнее читать. "
                    "Не придумывай варианты, которых нет в приведённых фрагментах. Вопрос должен быть конкретным и понятным."
                )
                prompt = (
                    f"Вопрос пользователя: {question}\n\n"
                    f"Найденные фрагменты из базы знаний:\n{chunks_list}\n\n"
                    f"{missing_note}\n\n"
                    "Сформулируй один уточняющий вопрос, помогающий выбрать среди этих вариантов."
                )
            else:
                # Нет чанков: уточнение по missing_info
                prompt = (
                    f"Пользователь спросил: {question}\n\n"
                    f"Недостающая информация: {missing_info}\n\n"
                    "Сформулируй один развернутый и понятный уточняющий вопрос, который поможет найти нужный ответ.\n"
                    "Вопрос должен быть максимально конкретным и понятным, как будто ты менеджер, который хочет помочь клиенту.\n"
                    "Не используй технические термины, говори простым языком.\n"
                    "Вопрос должен быть полным предложением, не используй сокращения.\n"
                    "Если перечисляешь варианты (1), 2), 3)) — каждый вариант с новой строки."
                )
                system_content = (
                    "Ты дружелюбный менеджер, который помогает клиентам, задавая понятные уточняющие вопросы."
                )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        
        clarification_text = resp.choices[0].message.content or "Можете уточнить ваш вопрос?"
        
        # Добавляем вводную фразу
        if insufficient_context:
            if is_topic_shift:
                intro = "Похоже, вы перешли на новую тему. Чтобы найти нужную информацию, мне нужны некоторые уточнения.\n\n"
            else:
                intro = "Чтобы найти нужную информацию, мне нужны некоторые уточнения.\n\n"
        else:
            intro = "Чтобы ответить на ваш вопрос, мне нужны некоторые уточнения.\n\n"
        
        clarification = intro + clarification_text
        
        await message.answer(clarification, reply_markup=qa_kb())
        
        # Сохраняем уточняющий вопрос в историю и устанавливаем флаг ожидания уточнения
        data = await state.get_data()
        history = data.get("qa_history", [])
        history.append({"role": "assistant", "text": clarification})
        await state.update_data(
            qa_history=history[-8:],
            qa_awaiting_clarification=True,
        )
        
        event_meta = {
            "original_question": question,
            "missing_info": missing_info,
            "insufficient_context": insufficient_context,
            "is_topic_shift": is_topic_shift,
        }
        if previous_topic:
            event_meta["previous_topic"] = previous_topic
        
        await alog_event(
            user_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            event="kb_clarification_asked_private",
            meta=event_meta,
        )
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка формулировки уточняющего вопроса: {e}")
        fallback_text = (
            "Чтобы найти нужную информацию, мне нужны некоторые уточнения.\n\n"
            "Можете уточнить ваш вопрос?"
        )
        await message.answer(fallback_text, reply_markup=qa_kb())


async def _generate_answer_from_chunks_private(
    question: str,
    chunks: List[Dict[str, Any]],
    conversation_history: List[Dict[str, Any]],
    user_name: str = "друг",
    is_first_question: bool = False,
    topics_summary: str = "",
) -> str:
    """Генерирует ответ на основе найденных чанков (для приватных чатов)."""
    try:
        # Строим структурированный контекст с темами
        history_text = build_conversation_context(conversation_history, max_messages=5, include_topics=True)
        
        # Используем переданное резюме тем или извлекаем, если не передано
        if not topics_summary:
            topics_summary = await build_topic_summary(conversation_history)
        
        # Определяем, является ли это follow-up вопросом
        is_follow_up = False
        last_answer = None
        for msg in reversed(conversation_history):
            if msg.get("role") == "assistant":
                last_answer = msg.get("text", "")
                break
        
        if last_answer:
            is_follow_up = await is_follow_up_question(question, last_answer)
        
        chunks_text = "\n\n---\n\n".join([
            f"Фрагмент {i+1}:\n{chunk.get('text', '')}"
            for i, chunk in enumerate(chunks)
        ])
        
        # Получаем примеры фраз для разнообразия
        phrases_examples = get_phrases_examples()
        
        # Формируем инструкции по приветствию
        greeting_instruction = ""
        if is_first_question:
            greeting_instruction = (
                f"ВАЖНО: Это первый вопрос пользователя в этой сессии. "
                f"Приветствуй его по имени ({user_name}) в начале ответа, например: 'Привет, {user_name}!' или 'Здравствуй, {user_name}!'"
            )
        else:
            greeting_instruction = (
                "ВАЖНО: Это НЕ первый вопрос в сессии. НЕ приветствуй пользователя, не используй 'Привет' или 'Здравствуй'. "
                "Используй имя только если это естественно для контекста, но не в начале каждого ответа."
            )
        
        system_prompt = (
            f"Ты помощник корпоративного бота сети магазинов Воблабир. "
            f"Ты общаешься с {user_name}.\n\n"
            "Твоя задача — ответить на вопрос пользователя на основе предоставленных фрагментов базы знаний.\n\n"
            "СТИЛЬ ОБЩЕНИЯ:\n"
            "1. Общайся как опытный менеджер, который хочет помочь клиенту\n"
            "2. Будь дружелюбным, но не навязчивым\n"
            "3. Используй простые слова, избегай технического жаргона\n"
            "4. Используй естественные переходы между мыслями\n"
            "5. Вариативность важна - не повторяй одни и те же фразы\n"
            "6. Используй разнообразные вводные и завершающие фразы\n"
            f"{greeting_instruction}\n\n"
            f"{phrases_examples}\n\n"
            "ИСПОЛЬЗОВАНИЕ КОНТЕКСТА:\n"
            "1. Используй информацию из прошлых сообщений для контекста\n"
            "2. Делай естественные отсылки к прошлым темам, если это уместно\n"
            "3. Можешь ссылаться на то, что обсуждалось ранее, но не обязательно явно\n"
            "4. Если пользователь задает вопрос, связанный с предыдущей темой, используй этот контекст\n\n"
            "КРИТИЧЕСКИ ВАЖНО:\n"
            "1. Фрагменты были найдены системой поиска как релевантные к вопросу пользователя.\n"
            "2. Твоя задача - найти и использовать релевантную информацию из этих фрагментов для ответа.\n"
            "3. НЕ говори 'фрагменты не содержат информации' - вместо этого найди и используй любую релевантную информацию.\n"
            "4. Если в фрагментах есть информация, связанная с вопросом (даже частично), используй её для ответа.\n"
            "5. Если фрагменты действительно не содержат релевантной информации, только тогда скажи об этом.\n\n"
            "ПРАВИЛА ОТВЕТА:\n"
            "1. Используй ТОЛЬКО информацию из предоставленных фрагментов\n"
            "2. Для каждого факта указывай номер фрагмента (1, 2, …), если уместно. Не используй информацию не из фрагментов.\n"
            "3. НЕ придумывай факты, которых нет в фрагментах\n"
            "4. Внимательно ищи релевантную информацию в каждом фрагменте\n"
            "5. Объединяй информацию из всех релевантных фрагментов для создания полного ответа\n"
            "6. Структурируй ответ: абзацы, списки, если уместно\n"
            "7. Будь дружелюбным и понятным\n"
            "8. Учитывай контекст предыдущих сообщений, но отвечай на текущий вопрос"
        )
        
        # Добавляем контекст предыдущего ответа для follow-up вопросов
        follow_up_context = ""
        if is_follow_up and last_answer:
            follow_up_context = f"\n\nВАЖНО: Это уточняющий вопрос к предыдущему ответу. Предыдущий ответ был:\n{last_answer[:300]}\n\n"
        
        # Добавляем информацию о темах в промпт
        topics_context = ""
        if topics_summary and not is_first_question:
            topics_context = f"\n\nКонтекст прошлых тем: {topics_summary}. Можешь делать естественные отсылки к этим темам, если это уместно.\n"
        
        user_prompt = (
            f"Текущий вопрос пользователя: {question}\n\n"
            f"{'Контекст диалога:\n' + history_text + '\n\n' if history_text else ''}"
            f"{topics_context}"
            f"{follow_up_context}"
            f"Фрагменты из базы знаний:\n{chunks_text}\n\n"
            "Сформулируй ответ на основе этих фрагментов.\n"
            "ВАЖНО: Отвечай ТОЛЬКО на текущий вопрос. Если фрагменты не относятся к текущему вопросу, скажи об этом."
        )
        
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        
        answer = resp.choices[0].message.content or "Извините, не могу сформировать ответ."
        return answer.strip()
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка генерации ответа: {e}")
        return "Извините, произошла ошибка при формировании ответа."


async def _should_escalate_to_manager_private(
    found_chunks: List[Dict[str, Any]],
    ai_decision: tuple[bool, Optional[str]],
) -> bool:
    """Определяет, нужно ли эскалировать вопрос менеджеру (для приватных чатов)."""
    sufficient, missing_info = ai_decision
    
    if not found_chunks:
        logger.info("[QA_MODE] Эскалация: чанки не найдены")
        return True
    
    max_score = max((chunk.get("score", 0) for chunk in found_chunks), default=0)
    
    # Если данные признаны достаточными, не эскалируем только из-за низкого score
    if sufficient:
        # Эскалируем только если score критически низкий (< 0.3)
        if max_score < 0.3:
            logger.info(f"[QA_MODE] Эскалация: данные достаточны, но score критически низкий ({max_score:.3f})")
            return True
        logger.info(f"[QA_MODE] Не эскалируем: данных достаточно, score приемлемый ({max_score:.3f})")
        return False
    
    # Если данных недостаточно
    if missing_info and any(word in missing_info.lower() for word in ["конкретн", "детал", "уточн"]):
        logger.info(f"[QA_MODE] Не эскалируем: данных недостаточно, но можно уточнить (max_score={max_score:.3f})")
        return False
    # Если данных недостаточно, но score хороший - все равно пытаемся ответить
    if max_score >= 0.6:
        logger.info(f"[QA_MODE] Не эскалируем: данных недостаточно, но score хороший ({max_score:.3f})")
        return False
    logger.info(f"[QA_MODE] Эскалация: данных недостаточно, score низкий ({max_score:.3f})")
    return True


@router.callback_query(F.data == "qa_start")
async def qa_start(cb: CallbackQuery, state: FSMContext):
    if not await _require_auth(cb):
        return

    # Сразу ответить на callback (лимит Telegram ~10s; иначе «часики» и ощущение, что кнопка мёртвая)
    await cb.answer()

    session_id = uuid.uuid4().hex[:12]
    await state.set_state(QAMode.active)
    await state.update_data(
        qa_history=[],
        qa_session_id=session_id,
        qa_questions_count=0,
        qa_last_question="",
        qa_last_answer_source="",
        qa_original_question="",
        qa_awaiting_clarification=False,
    )

    await cb.message.answer(
        "🧠 <b>Навык: Ответы на вопросы</b>\n\n"
        "Напиши вопрос — я попробую ответить по базе знаний.\n"
        "Можно задавать вопросы подряд.\n\n"
        "Чтобы выйти — нажми «Завершить навык».",
        reply_markup=qa_kb(),
        parse_mode="HTML",
    )


@router.message(F.text == "❓ Задать вопрос")
async def qa_start_text(message: Message, state: FSMContext):
    if not await _require_auth(message):
        return

    session_id = uuid.uuid4().hex[:12]
    await state.set_state(QAMode.active)
    await state.update_data(
        qa_history=[],
        qa_session_id=session_id,
        qa_questions_count=0,
        qa_last_question="",
        qa_last_answer_source="",
        qa_original_question="",
        qa_awaiting_clarification=False,
    )
    await message.answer(
        "🧠 <b>Навык: Ответы на вопросы</b>\n\n"
        "Напиши вопрос — я попробую ответить по базе знаний.\n"
        "Можно задавать вопросы подряд.\n\n"
        "Чтобы выйти — нажми «✅ Завершить навык».",
        reply_markup=qa_kb(),
        parse_mode="HTML",
    )


@router.message(Command("ask"))
async def qa_start_command(message: Message, state: FSMContext):
    if not await _require_auth(message):
        return

    session_id = uuid.uuid4().hex[:12]
    await state.set_state(QAMode.active)
    await state.update_data(
        qa_history=[],
        qa_session_id=session_id,
        qa_questions_count=0,
        qa_last_question="",
        qa_last_answer_source="",
        qa_original_question="",
        qa_awaiting_clarification=False,
    )
    await message.answer(
        "🧠 <b>Навык: Ответы на вопросы</b>\n\n"
        "Напиши вопрос — я попробую ответить по базе знаний.\n"
        "Можно задавать вопросы подряд.\n\n"
        "Чтобы выйти — нажми «✅ Завершить навык».",
        reply_markup=qa_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "qa_exit")
async def qa_exit(cb: CallbackQuery, state: FSMContext):
    await cb.answer()

    await state.set_state(FeedbackState.waiting_helped)
    await cb.message.answer(
        "Перед выходом оцените, пожалуйста, насколько я помог 😊\n\n"
        "1/4 — Помог ли бот решить вопрос?",
        reply_markup=_kb_helped(),
    )


@router.message(QAMode.active, F.text)
async def qa_handle_question(message: Message, state: FSMContext):
    if not await _require_auth(message):
        return

    q = (message.text or "").strip()
    if not q:
        await message.answer("Напиши вопрос текстом 🙂", reply_markup=qa_kb())
        return

    # Получаем имя пользователя
    user_id = message.from_user.id if message.from_user else 0
    user = find_user_by_telegram_id(user_id)
    user_name = user.name if user else (message.from_user.first_name if message.from_user else "друг")

    # Увеличиваем счётчик вопросов
    data = await state.get_data()
    cnt = int(data.get("qa_questions_count", 0)) + 1
    history = data.get("qa_history", [])
    original_question = data.get("qa_original_question", "")
    awaiting_clarification = data.get("qa_awaiting_clarification", False)
    previous_chunks = data.get("qa_found_chunks", [])  # Получаем предыдущие чанки
    
    # Определяем, является ли это первым вопросом или ответом на уточнение
    # Первый вопрос - когда история пустая или содержит только системные сообщения
    user_messages = [msg for msg in history if msg.get("role") == "user"]
    is_first_question = len(user_messages) == 0
    is_clarification_response = awaiting_clarification
    
    # ДОПОЛНИТЕЛЬНО: Проверяем, был ли последний ответ бота уточняющим вопросом
    last_assistant_msg = None
    for msg in reversed(history):
        if msg.get("role") == "assistant":
            last_assistant_msg = msg.get("text", "")
            break

    # LLM: при ожидании ответа на уточнение — ответ на уточнение или новый вопрос?
    if not is_first_question and awaiting_clarification and last_assistant_msg and original_question:
        clarification_vs_new = await detect_clarification_response_vs_new_question(q, last_assistant_msg, original_question)
        if clarification_vs_new == "new_question":
            is_clarification_response = False
            original_question = q
            awaiting_clarification = False
            logger.info(f"[QA_MODE] LLM: пользователь задал новый вопрос вместо ответа на уточнение: '{q[:50]}...'")

    is_new_question = False
    # LLM: после полного ответа бота — новый вопрос или реакция/уточнение к ответу?
    if last_assistant_msg and not is_first_question and "уточнения" not in last_assistant_msg.lower() and not is_clarification_response:
        new_vs_follow = await detect_new_question_vs_follow_up(q, last_assistant_msg)
        is_new_question = new_vs_follow == "new_question"
        if is_new_question:
            logger.info(f"[QA_MODE] Определен новый вопрос (LLM): '{q[:50]}...'")
        else:
            logger.info(f"[QA_MODE] Определена реакция/follow-up к предыдущему ответу (LLM): '{q[:50]}...'")

    # Если это новый вопрос (не первый и не уточнение), обновляем исходный вопрос
    if is_new_question and not is_first_question:
        original_question = q
        logger.info(f"[QA_MODE] Обновляем исходный вопрос на новый: '{q[:50]}...'")
        awaiting_clarification = False  # Сбрасываем флаг
    elif is_first_question:
        original_question = q
        logger.info(f"[QA_MODE] Сохраняем исходный вопрос: '{q[:50]}...'")
    # Если это ответ на уточнение, объединяем исходный вопрос с уточнением
    elif is_clarification_response and original_question:
        # Используем структурированный формат для объединения
        combined_question = f"Исходный вопрос: {original_question}\nУточнение пользователя: {q}"
        logger.info(f"[QA_MODE] Объединяем исходный вопрос с уточнением: '{combined_question[:100]}...'")
        q = combined_question  # Используем объединенный вопрос для поиска
    
    # Определяем тип вопроса
    question_type = await detect_question_type(q, history)
    
    # Проверяем, является ли это follow-up вопросом
    last_answer_text = None
    for msg in reversed(history):
        if msg.get("role") == "assistant":
            last_answer_text = msg.get("text", "")
            break
    
    is_follow_up = False
    if last_answer_text and question_type == "new":
        is_follow_up = await is_follow_up_question(q, last_answer_text)
        if is_follow_up:
            question_type = "follow_up"
            logger.info(f"[QA_MODE] Определен follow-up вопрос: '{q[:50]}...'")
    
    # Извлекаем темы из текущего вопроса (простая версия - можно улучшить через LLM)
    # Пока используем ключевые слова из вопроса
    import re
    question_words = re.findall(r'\b[а-яё]{4,}\b', q.lower())
    stop_words = {"это", "как", "что", "для", "когда", "где", "который", "можно", "нужно"}
    question_topics = [w for w in question_words if w not in stop_words][:3]
    
    # Добавляем вопрос в историю с расширенными метаданными
    history.append({
        "role": "user",
        "text": message.text.strip(),
        "timestamp": datetime.now().isoformat(),
        "question_type": question_type,
        "topics": question_topics,  # НОВОЕ: темы из вопроса
        "key_points": [],  # Будет заполнено после ответа
    })
    
    # Получаем предыдущие чанки из state (если есть)
    previous_chunks = data.get("qa_found_chunks", [])
    
    update_payload = {
        "qa_questions_count": cnt,
        "qa_last_question": q,
        "qa_original_question": original_question,
        "qa_awaiting_clarification": False,  # Сбрасываем флаг после обработки
        "qa_history": history[-8:],  # Ограничиваем историю
    }
    if is_new_question or is_first_question:
        update_payload["qa_clarification_rounds"] = 0
    await state.update_data(**update_payload)

    # Показываем индикатор обработки
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)
    
    # ПРОВЕРКИ ДО ПОИСКА В RAG
    # Определяем смену темы (если не первый вопрос и не ответ на уточнение)
    is_topic_shift = False
    previous_topic = None
    if not is_first_question and not is_clarification_response:
        is_topic_shift, previous_topic = await detect_topic_shift(q, history)
        logger.info(
            f"[QA_MODE] Определение смены темы: is_shift={is_topic_shift}, "
            f"previous_topic={previous_topic}"
        )
    
    # В режиме полного документа пропускаем проверку контекста — отвечаем сразу по документу без уточнений
    from app.services.full_file_context import get_full_file_context
    _full_doc = get_full_file_context() if USE_FULL_FILE_CONTEXT else None
    # Уточняющие вопросы отключены (MAX_CLARIFICATION_ROUNDS=0) — идём сразу в поиск
    if not (_full_doc and USE_FULL_FILE_CONTEXT) and MAX_CLARIFICATION_ROUNDS > 0:
        context_sufficient, missing_context = await check_question_context_sufficiency(
            q, history, is_topic_shift=is_topic_shift
        )
        if not context_sufficient:
            logger.info(
                f"[QA_MODE] Контекста недостаточно для поиска: {missing_context}. "
                f"Задаем уточняющий вопрос."
            )
            await state.update_data(qa_clarification_rounds=1)
            await _ask_clarification_question_private(
                message=message,
                question=q,
                found_chunks=[],  # Чанки еще не найдены
                missing_info=missing_context or "Недостаточно контекста для поиска",
                state=state,
                insufficient_context=True,
                is_topic_shift=is_topic_shift,
                previous_topic=previous_topic,
            )
            return

    # Отправляем промежуточное сообщение
    searching_msg = await message.answer(f"🔍 Ищу информацию в базе знаний, {user_name}...")

    try:
        # ШАГ 1: Подготовка запроса для поиска
        # «Другой вопрос» / «новая тема» — пользователь указывает на смену темы; ищем по предыдущему его вопросу
        q_clean = q.strip().lower()
        if (q_clean in ("другой вопрос", "другой вопрос.", "новая тема", "по другой теме") or
                (len(q) < 30 and "другой" in q_clean and "вопрос" in q_clean)):
            for msg in reversed(history[:-1]):  # без последнего (текущее сообщение)
                if msg.get("role") == "user":
                    prev_text = (msg.get("text") or "").strip()
                    if len(prev_text) > 10 and ("?" in prev_text or "как" in prev_text or "что" in prev_text or "какие" in prev_text):
                        q = prev_text
                        logger.info(f"[QA_MODE] Пользователь написал «другой вопрос», ищем по предыдущему: '{q[:80]}...'")
                        break
        # Если это ответ на уточнение, q уже содержит объединенный вопрос
        # Если новый вопрос или смена темы — ищем только по текущему вопросу, без контекста старой темы
        # Иначе (follow-up) — используем контекст из истории
        if is_clarification_response:
            query_text = q  # q уже содержит объединенный вопрос
            logger.info(f"[QA_MODE] Используем объединенный вопрос для поиска: '{query_text[:100]}...'")
        elif is_new_question or is_topic_shift:
            query_text = q
            logger.info(f"[QA_MODE] Новый вопрос / смена темы, ищем только по текущему: '{query_text[:80]}...'")
        else:
            context_text = "\n".join([msg.get("text", "") for msg in history[-3:]])
            query_text = f"{context_text}\n{q}" if context_text else q

        # Режим «полный файл в контексте»: ответ по одному документу без RAG
        from app.services.full_file_context import get_full_file_context
        document = get_full_file_context()
        if USE_FULL_FILE_CONTEXT and document:
            try:
                await searching_msg.edit_text(f"🔍 Ищу в документе, {user_name}...")
            except Exception:
                pass
            # Дополняем документ kilbil (help.kilbil.ru), чтобы отвечать на вопросы по платформе
            kilbil_match = None
            try:
                from app.services.kilbil_service import find_kilbil_answer
                kilbil_match = await find_kilbil_answer(q)
                if kilbil_match and kilbil_match.get("answer"):
                    document = document + "\n\n--- База kilbil (help.kilbil.ru) ---\n" + kilbil_match["answer"]
            except Exception as e:
                logger.warning(f"[QA_MODE] Не удалось дополнить kilbil: {e}")
            answer = await asyncio.to_thread(
                generate_answer_from_full_document,
                q,
                document,
                history,
                user_name=user_name,
                is_first_turn=is_first_question,
            )
            try:
                await searching_msg.delete()
            except Exception:
                pass
            history.append({
                "role": "assistant",
                "text": answer,
                "timestamp": datetime.now().isoformat(),
                "source": "full_file",
                "chunks_used": 0,
            })
            await state.update_data(
                qa_history=history[-8:],
                qa_last_answer_source="full_file",
                qa_found_chunks=[],
                qa_last_answer_text=answer,
            )
            if kilbil_match and kilbil_match.get("url"):
                answer = answer + "\n\n📎 Подробнее: " + kilbil_match["url"]
            await message.answer(
                answer + "\n\nЕсли есть ещё вопрос — просто напиши его 👇",
                reply_markup=qa_kb(),
                parse_mode="HTML",
            )
            await alog_event(
                user_id=message.from_user.id if message.from_user else None,
                username=message.from_user.username if message.from_user else None,
                event="kb_answer_generated_private",
                meta={"question": q[:100], "chunks_used": 0, "source": "full_file"},
            )
            return

        # ШАГ 2: Расширяем запрос для улучшения поиска
        expanded_query = await _expand_query_for_search(query_text)
        logger.info(
            f"[QA_MODE] Оригинальный запрос: '{query_text[:80]}...' -> "
            f"Расширенный: '{expanded_query[:100]}...'"
        )
        
        # ШАГ 3: Создаем эмбеддинги для разных вариантов запроса
        # Делаем несколько поисков для лучшего покрытия
        qdrant_service = get_qdrant_service()
        all_found_chunks = []
        seen_texts = set()
        chunks_expanded_count = 0
        chunks_original_count = 0
        chunks_keywords_count = 0
        
        # Поиск 1: Расширенный запрос
        embedding_expanded = await asyncio.to_thread(create_embedding, expanded_query)
        chunks_expanded = qdrant_service.search_multi_level(
            query_embedding=embedding_expanded,
            top_k=5,
            initial_threshold=0.5,
            fallback_thresholds=[0.3, 0.1],
        )
        chunks_expanded_count = len(chunks_expanded)
        for chunk in chunks_expanded:
            chunk_text = chunk.get("text", "")
            if chunk_text and chunk_text not in seen_texts:
                all_found_chunks.append(chunk)
                seen_texts.add(chunk_text)
        
        embedding_original = None
        # Поиск 2: Оригинальный запрос (если отличается от расширенного)
        if query_text != expanded_query and len(query_text.strip()) > 5:
            embedding_original = await asyncio.to_thread(create_embedding, query_text)
            chunks_original = qdrant_service.search_multi_level(
                query_embedding=embedding_original,
                top_k=5,
                initial_threshold=0.5,
                fallback_thresholds=[0.3, 0.1],
            )
            chunks_original_count = len(chunks_original)
            for chunk in chunks_original:
                chunk_text = chunk.get("text", "")
                if chunk_text and chunk_text not in seen_texts:
                    all_found_chunks.append(chunk)
                    seen_texts.add(chunk_text)
        
        # Поиск 3: Ключевые слова из вопроса (для конкретных вопросов)
        # Извлекаем ключевые слова из оригинального вопроса
        import re
        keywords = re.findall(r'\b\w{4,}\b', q.lower())  # Слова длиннее 3 символов
        if keywords and len(keywords) >= 2:
            # Берем первые 3-5 ключевых слов
            keywords_query = " ".join(keywords[:5])
            if keywords_query != query_text.lower() and len(keywords_query) > 5:
                embedding_keywords = await asyncio.to_thread(create_embedding, keywords_query)
                chunks_keywords = qdrant_service.search_multi_level(
                    query_embedding=embedding_keywords,
                    top_k=3,
                    initial_threshold=0.4,
                    fallback_thresholds=[0.2, 0.1],
                )
                chunks_keywords_count = len(chunks_keywords)
                for chunk in chunks_keywords:
                    chunk_text = chunk.get("text", "")
                    if chunk_text and chunk_text not in seen_texts:
                        all_found_chunks.append(chunk)
                        seen_texts.add(chunk_text)
        
        # Поиск 4: kilbil help (source=kilbil_help) — чтобы не терять релевантные ответы из help.kilbil.ru
        # Для kilbil используем оригинальный запрос (точнее для «как настроить...»), иначе расширенный
        embedding_for_kilbil = embedding_original if embedding_original is not None else embedding_expanded
        chunks_kilbil = qdrant_service.search_multi_level(
            query_embedding=embedding_for_kilbil,
            top_k=5,
            initial_threshold=0.4,
            fallback_thresholds=[0.25, 0.1],
            source_filter="kilbil_help",
        )
        for chunk in chunks_kilbil:
            chunk_text = chunk.get("text", "")
            if chunk_text and chunk_text not in seen_texts:
                all_found_chunks.append(chunk)
                seen_texts.add(chunk_text)
        
        if USE_HYDE and query_text.strip():
            from app.services.hyde_search import generate_hypothetical_answer, merge_hyde_with_main
            hyde_text = await generate_hypothetical_answer(query_text)
            if hyde_text:
                embedding_hyde = await asyncio.to_thread(create_embedding, hyde_text)
                hyde_chunks = qdrant_service.search_multi_level(
                    query_embedding=embedding_hyde,
                    top_k=10,
                    initial_threshold=0.3,
                    fallback_thresholds=[0.2, 0.1],
                )
                if hyde_chunks:
                    all_found_chunks = merge_hyde_with_main(all_found_chunks, hyde_chunks, top_n=20)
        
        # Сортируем по score и берем топ-N для re-ranking (28 для Cohere, 15 для LLM)
        all_found_chunks.sort(key=lambda x: x.get("score", 0), reverse=True)
        use_cohere_rerank = USE_CROSS_ENCODER_RERANK and bool(COHERE_API_KEY)
        candidates_limit = RERANK_CANDIDATES_LIMIT if use_cohere_rerank else 15
        if USE_HYBRID_BM25 and all_found_chunks:
            from app.services.bm25_search import hybrid_vector_bm25
            initial_chunks = hybrid_vector_bm25(query_text, all_found_chunks, top_n=candidates_limit)
        else:
            initial_chunks = all_found_chunks[:candidates_limit]
        
        # Re-ranking через LLM
        if initial_chunks:
            try:
                await searching_msg.edit_text(f"🔍 Нашёл {len(initial_chunks)} фрагментов, анализирую релевантность...")
                reranked_chunks = await rerank_chunks_with_llm(q, initial_chunks, top_k=8)
                # Выбираем лучшие уникальные чанки
                found_chunks = select_best_chunks(reranked_chunks, max_chunks=5, min_score=0.1)
                found_chunks = [c for c in found_chunks if c.get("score", 0) >= MIN_SCORE_AFTER_RERANK]
                logger.info(f"[QA_MODE] После re-ranking выбрано {len(found_chunks)} чанков (score >= {MIN_SCORE_AFTER_RERANK})")
            except Exception as e:
                logger.exception(f"[QA_MODE] Ошибка re-ranking: {e}")
                found_chunks = [c for c in initial_chunks[:5] if c.get("score", 0) >= MIN_SCORE_AFTER_RERANK]
        else:
            found_chunks = []

        if not found_chunks:
            logger.info("[QA_MODE] Нет чанков выше MIN_SCORE_AFTER_RERANK, переход к эскалации/FAQ")
        
        if len(all_found_chunks) > chunks_expanded_count:
            logger.info(
                f"[QA_MODE] Множественный поиск: найдено {len(all_found_chunks)} уникальных чанков "
                f"(из них {chunks_expanded_count} из расширенного запроса, "
                f"{chunks_original_count} из оригинального, "
                f"{chunks_keywords_count} из ключевых слов)"
            )
        
        # Детальное логирование для диагностики
        logger.info(
            f"[QA_MODE] Поиск в RAG: оригинальный вопрос='{q[:50]}...', "
            f"расширенный запрос='{expanded_query[:80]}...', "
            f"найдено чанков={len(found_chunks)}"
        )
        if found_chunks:
            scores = [chunk.get("score", 0) for chunk in found_chunks]
            max_score = max(scores) if scores else 0
            min_score = min(scores) if scores else 0
            logger.info(
                f"[QA_MODE] Scores найденных чанков: min={min_score:.3f}, "
                f"max={max_score:.3f}, все={[f'{s:.3f}' for s in scores]}"
            )
        else:
            logger.warning(
                f"[QA_MODE] Не найдено чанков в RAG для запроса: '{q[:50]}...' "
                f"(расширенный: '{expanded_query[:80]}...')"
            )
        
        q_hash = str(hash((query_text or q).strip().lower()[:200]))
        await alog_event(
            user_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            event="kb_search_performed_private",
            meta={
                "question_hash": q_hash,
                "chunks_found": len(found_chunks),
                "top_scores": [round(c.get("score", 0), 3) for c in found_chunks[:3]],
                "top_sources": [str((c.get("metadata") or {}).get("source", ""))[:50] for c in found_chunks[:3]],
            },
        )
        
        # Если нашли чанки в Qdrant
        if found_chunks:
            # Если это ответ на уточнение, объединяем с предыдущими чанками
            all_chunks = found_chunks
            if is_clarification_response and previous_chunks:
                # Объединяем предыдущие и новые чанки, убирая дубликаты по тексту
                seen_texts = {chunk.get("text", "") for chunk in previous_chunks}
                new_chunks = [chunk for chunk in found_chunks if chunk.get("text", "") not in seen_texts]
                all_chunks = previous_chunks + new_chunks
                logger.info(
                    f"[QA_MODE] Объединяем чанки: было {len(previous_chunks)}, новых {len(new_chunks)}, "
                    f"всего {len(all_chunks)}"
                )
            
            # Проверка достаточности данных (передаем историю для контекста и флаг после уточнений)
            sufficient, missing_info = await _check_sufficient_data_private(
                q, all_chunks, history, is_after_clarification=is_clarification_response
            )
            logger.info(
                f"[QA_MODE] Проверка достаточности данных: sufficient={sufficient}, "
                f"missing_info={missing_info[:50] if missing_info else None}"
            )
            
            # Проверяем, нужно ли эскалировать (используем объединенные чанки)
            should_escalate = await _should_escalate_to_manager_private(all_chunks, (sufficient, missing_info))
            logger.info(f"[QA_MODE] Решение об эскалации: should_escalate={should_escalate}")
            
            if not should_escalate:
                # Если данных недостаточно — задаем уточняющий вопрос, но не более MAX_CLARIFICATION_ROUNDS раундов
                clarification_rounds = (await state.get_data()).get("qa_clarification_rounds", 0)
                if not sufficient and missing_info:
                    if clarification_rounds >= MAX_CLARIFICATION_ROUNDS:
                        # Лимит раундов: больше не спрашиваем, отвечаем по лучшему что есть
                        sufficient = True
                        missing_info = None
                        logger.info("[QA_MODE] Лимит раундов уточнений, отвечаем по чанкам")
                    else:
                        logger.info("[QA_MODE] Задаем уточняющий вопрос пользователю")
                        await state.update_data(qa_found_chunks=all_chunks, qa_clarification_rounds=clarification_rounds + 1)
                        await _ask_clarification_question_private(message, q, all_chunks, missing_info, state)
                        return
                
                # Извлекаем темы из истории для контекста
                topics_summary = await build_topic_summary(history)
                
                # Генерируем ответ из Qdrant (используем объединенные чанки)
                logger.info(f"[QA_MODE] Генерируем ответ из найденных чанков RAG (всего {len(all_chunks)} чанков)")
                try:
                    await searching_msg.edit_text(f"✍️ Формирую ответ на основе найденной информации...")
                except:
                    pass  # Игнорируем ошибки редактирования сообщения
                answer = await _generate_answer_from_chunks_private(
                    q, all_chunks, history, user_name, 
                    is_first_question=is_first_question,
                    topics_summary=topics_summary
                )
                grounded = await check_answer_grounding(answer, all_chunks)
                if not grounded:
                    logger.warning("[QA_MODE] Ответ не обоснован фрагментами (grounding), эскалация")
                    should_escalate = True
                else:
                    pass  # продолжаем проверки и отправку ответа ниже
                
                # Проверяем, не говорит ли ответ что данных нет (хотя чанки найдены)
                answer_lower = answer.lower()
                no_data_phrases = [
                    "не содержат информации",
                    "не содержат данных",
                    "нет информации",
                    "нет данных",
                    "информация отсутствует",
                    "данные отсутствуют",
                ]
                
                if not grounded or (any(phrase in answer_lower for phrase in no_data_phrases) and all_chunks):
                    if not grounded:
                        pass  # already set should_escalate above
                    else:
                        logger.warning(
                            f"[QA_MODE] LLM говорит что данных нет, но чанки найдены ({len(all_chunks)}). "
                            f"Эскалируем менеджеру."
                        )
                        should_escalate = True
                if not should_escalate:
                    # Извлекаем ключевые моменты из ответа (простая версия)
                    # Можно улучшить через LLM для более точного извлечения
                    answer_sentences = re.split(r'[.!?]\s+', answer)
                    key_points = [s.strip()[:50] for s in answer_sentences[:3] if len(s.strip()) > 20]
                    
                    # Обновляем историю с расширенными метаданными
                    # Сохраняем чанки для возможных follow-up вопросов
                    answer_summary = answer[:200] + "..." if len(answer) > 200 else answer
                    
                    # Извлекаем темы из ответа (из последнего вопроса пользователя)
                    answer_topics = []
                    if history:
                        last_user_msg = None
                        for msg in reversed(history):
                            if msg.get("role") == "user":
                                last_user_msg = msg
                                break
                        if last_user_msg:
                            answer_topics = last_user_msg.get("topics", [])
                    
                    history.append({
                        "role": "assistant",
                        "text": answer,
                        "timestamp": datetime.now().isoformat(),
                        "source": "rag",
                        "chunks_used": len(all_chunks),
                        "answer_summary": answer_summary,
                        "topics": answer_topics,  # НОВОЕ: темы из вопроса
                        "key_points": key_points,  # НОВОЕ: ключевые моменты ответа
                    })
                    await state.update_data(
                        qa_history=history[-8:],
                        qa_last_answer_source="qdrant_rag",
                        qa_found_chunks=all_chunks,  # Сохраняем для follow-up вопросов
                        qa_last_answer_text=answer,
                    )
                    
                    # Удаляем промежуточное сообщение
                    try:
                        await searching_msg.delete()
                    except:
                        pass

                    kilbil_urls = get_article_urls_from_chunks(all_chunks, only_if_top_from_kilbil=True)
                    if kilbil_urls:
                        if len(kilbil_urls) == 1:
                            answer = answer + "\n\n📎 Подробнее: " + kilbil_urls[0]
                        else:
                            answer = answer + "\n\n📎 Подробнее:\n" + "\n".join(kilbil_urls)
                    
                    await message.answer(
                        answer + "\n\nЕсли есть ещё вопрос — просто напиши его 👇",
                        reply_markup=qa_kb(),
                        parse_mode="HTML",
                    )
                    
                    await alog_event(
                        user_id=message.from_user.id if message.from_user else None,
                        username=message.from_user.username if message.from_user else None,
                        event="kb_answer_generated_private",
                        meta={"question": q, "chunks_used": len(all_chunks)},
                    )
                    return
            
            # Если нужно эскалировать (включая случай когда LLM сказал что данных нет)
            # Продолжаем выполнение для поиска в FAQ и эскалации
        
        # ШАГ 2: Если не нашли в Qdrant или нужно эскалировать - ищем в FAQ
        if not found_chunks:
            logger.info("[QA_MODE] Чанки не найдены в RAG")
            
            # Если тема сменилась и чанки не найдены, задаем уточняющий вопрос (если не отключено)
            if is_topic_shift and MAX_CLARIFICATION_ROUNDS > 0:
                logger.info(
                    f"[QA_MODE] Тема сменилась ({previous_topic}), чанки не найдены. "
                    f"Задаем уточняющий вопрос о новой теме."
                )
                try:
                    await searching_msg.delete()
                except:
                    pass
                await state.update_data(qa_clarification_rounds=1)
                await _ask_clarification_question_private(
                    message=message,
                    question=q,
                    found_chunks=[],
                    missing_info=f"Не найдено информации по новой теме. Ранее обсуждалась тема: {previous_topic or 'другая тема'}.",
                    state=state,
                    insufficient_context=False,  # Контекст был достаточным, но тема сменилась
                    is_topic_shift=True,
                    previous_topic=previous_topic,
                )
                return
            
            logger.info("[QA_MODE] Чанки не найдены, переходим к поиску в FAQ")
        else:
            logger.info("[QA_MODE] Чанки найдены, но требуется эскалация, переходим к поиску в FAQ")
        
        best = await find_similar_question(q)
        
        if best:
            raw_answer = best["answer"]
            
            try:
                pretty = await asyncio.to_thread(polish_faq_answer, q, raw_answer, history)
            except Exception:
                pretty = raw_answer
            
            history.append({"role": "assistant", "text": pretty})
            await state.update_data(
                qa_history=history[-8:],
                qa_last_answer_source="faq",
                qa_last_answer_text=pretty,
                qa_found_chunks=[],  # FAQ — не из RAG
            )
            
            # Удаляем промежуточное сообщение, если оно есть
            try:
                if 'searching_msg' in locals():
                    await searching_msg.delete()
            except:
                pass
            
            await message.answer(
                pretty + "\n\nЕсли есть ещё вопрос — просто напиши его 👇",
                reply_markup=qa_kb(),
                parse_mode="HTML",
            )
            
            media_json = best.get("media_json", "")
            if media_json:
                await _send_media_from_json(message.bot, message.chat.id, media_json)
            
            await alog_event(
                user_id=message.from_user.id if message.from_user else None,
                username=message.from_user.username if message.from_user else None,
                event="faq_answer_shown_private",
                meta={"score": best.get("score"), "matched_q": best.get("question")},
            )
            return
        
        # ШАГ 3: Если не нашли ни в Qdrant, ни в FAQ - эскалируем менеджеру
        # Формируем полный контекст разговора для менеджера
        data = await state.get_data()
        history = data.get("qa_history", [])
        original_question = data.get("qa_original_question", q)
        
        # Собираем полный контекст разговора
        conversation_parts = []
        conversation_parts.append(f"Исходный вопрос: {original_question}")
        
        # Добавляем все сообщения из истории (вопросы пользователя и уточнения бота)
        for i, msg in enumerate(history):
            role = msg.get("role", "")
            text = msg.get("text", "")
            if role == "user":
                # Пропускаем исходный вопрос, так как он уже добавлен
                if text != original_question:
                    conversation_parts.append(f"Уточнение пользователя: {text}")
            elif role == "assistant" and "уточнения" in text.lower():
                # Извлекаем только сам вопрос из уточнения (без вводной фразы)
                question_part = text.replace("Чтобы ответить на ваш вопрос, мне нужны некоторые уточнения.\n\n", "")
                conversation_parts.append(f"Уточняющий вопрос бота: {question_part}")
        
        # Формируем полный вопрос для менеджера
        full_question = "\n\n".join(conversation_parts)
        
        logger.warning(
            f"[QA_MODE] Не найдено ответа ни в RAG, ни в FAQ. "
            f"Исходный вопрос: '{original_question[:50]}...'. "
            f"Полный контекст: '{full_question[:150]}...'. Эскалируем менеджеру."
        )
        await state.update_data(qa_last_answer_source="manager", qa_last_answer_text="", qa_found_chunks=[])
        
        # Удаляем промежуточное сообщение
        try:
            if 'searching_msg' in locals():
                await searching_msg.delete()
        except:
            pass
        
        await message.answer(
            f"Не нашёл ответа в базе знаний, {user_name} 😕\n"
            "Я передал вопрос менеджеру. Можешь задать следующий вопрос — просто напиши его 👇",
            reply_markup=qa_kb(),
        )
        
        await alog_event(
            user_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            event="kb_not_found_escalated",
            meta={"original_question": original_question, "full_context": full_question[:200]},
        )
        
        await create_ticket_and_notify_managers(message, full_question)
        
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка обработки вопроса: {e}")
        await message.answer(
            "Извините, произошла ошибка при обработке вопроса. Попробуйте переформулировать.",
            reply_markup=qa_kb(),
        )


# -----------------------------
#      ОБРАБОТКА ФИДБЭКА
# -----------------------------

@router.callback_query(FeedbackState.waiting_helped, F.data.startswith("fb_helped:"))
async def fb_helped(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    helped = cb.data.split(":", 1)[1]
    await state.update_data(fb_helped=helped)

    await state.set_state(FeedbackState.waiting_completeness)
    await cb.message.answer(
        "2/4 — Оцените полноту информации:",
        reply_markup=_kb_stars("fb_comp"),
    )


@router.callback_query(FeedbackState.waiting_completeness, F.data.startswith("fb_comp:"))
async def fb_completeness(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    val = int(cb.data.split(":", 1)[1])
    await state.update_data(fb_completeness=val)

    await state.set_state(FeedbackState.waiting_clarity)
    await cb.message.answer(
        "3/4 — Оцените понятность ответа:",
        reply_markup=_kb_stars("fb_clarity"),
    )


@router.callback_query(FeedbackState.waiting_clarity, F.data.startswith("fb_clarity:"))
async def fb_clarity(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    val = int(cb.data.split(":", 1)[1])
    await state.update_data(fb_clarity=val)

    await state.set_state(FeedbackState.waiting_comment)
    await cb.message.answer(
        "4/4 — Хотите оставить комментарий? (одной фразой)\n"
        "Можно пропустить.",
        reply_markup=_kb_skip_comment(),
    )


@router.callback_query(FeedbackState.waiting_comment, F.data == "fb_skip_comment")
async def fb_skip_comment(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await _finalize_feedback(cb.message, state, comment="")


@router.message(FeedbackState.waiting_comment, F.text)
async def fb_comment_msg(message: Message, state: FSMContext):
    await _finalize_feedback(message, state, comment=(message.text or "").strip())


async def _finalize_feedback(msg_obj, state: FSMContext, comment: str):
    data = await state.get_data()

    session_id = data.get("qa_session_id", "")
    questions_count = int(data.get("qa_questions_count", 0))
    last_question = data.get("qa_last_question", "")
    last_answer_source = data.get("qa_last_answer_source", "")

    helped = data.get("fb_helped", "")
    completeness = int(data.get("fb_completeness", 0) or 0)
    clarity = int(data.get("fb_clarity", 0) or 0)

    user_id = msg_obj.from_user.id
    username = msg_obj.from_user.username

    # Phase 5.4: при «Не помогло» логируем вопрос, chunk ids и ответ для разбора
    if helped == "no":
        last_answer_text = data.get("qa_last_answer_text", "") or ""
        found_chunks = data.get("qa_found_chunks", []) or []
        chunk_ids = [str(c.get("id", "")) for c in found_chunks if c.get("id")]
        question_hash = hashlib.sha256((last_question or "").strip().encode("utf-8")).hexdigest()[:16]
        await alog_event(
            user_id=user_id,
            username=username,
            event="qa_feedback_not_helped",
            meta={
                "question_hash": question_hash,
                "question_preview": (last_question or "")[:100],
                "chunk_ids": chunk_ids,
                "answer_preview": last_answer_text[:300],
                "answer_source": last_answer_source,
            },
        )

    save_qa_feedback(
        session_id=session_id,
        user_id=user_id,
        username=username,
        helped=helped,
        completeness=completeness,
        clarity=clarity,
        comment=comment,
        questions_count=questions_count,
        last_question=last_question,
        last_answer_source=last_answer_source,
    )

    await state.clear()
    await msg_obj.answer("Спасибо! 🙌 Отзыв сохранён.", reply_markup=main_menu_kb())

