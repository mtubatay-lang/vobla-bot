"""Обработка вопросов в групповых чатах с использованием RAG из Qdrant."""

import asyncio
import logging
import re
from typing import List, Dict, Any, Optional

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from cachetools import TTLCache
import hashlib
import time

from app.services.auth_service import find_user_by_telegram_id
from app.services.qdrant_service import get_qdrant_service
from app.services.openai_client import create_embedding, client, CHAT_MODEL
from app.services.openai_client import check_answer_grounding, generate_answer_from_full_document
from app.services.metrics_service import alog_event
from app.services.reranking_service import rerank_chunks_with_llm, select_best_chunks
from app.services.kilbil_service import get_article_urls_from_chunks
from app.config import (
    MANAGER_USERNAMES,
    get_rag_test_chat_id,
    MAX_CLARIFICATION_ROUNDS,
    MIN_SCORE_AFTER_RERANK,
    MIN_TOP_SCORE_FOR_ANSWER,
    KB_MANAGERS_CHAT_ID,
    USE_HYBRID_BM25,
    USE_HYDE,
    USE_FULL_FILE_CONTEXT,
    USE_CROSS_ENCODER_RERANK,
    COHERE_API_KEY,
    RERANK_CANDIDATES_LIMIT,
    RAG_SEND_NO_ANSWER_REPLY,
)
from app.handlers.qa_mode import _expand_query_for_search, detect_clarification_response_vs_new_question

logger = logging.getLogger(__name__)

router = Router()


class GroupChatQAState(StatesGroup):
    """Состояния для контекста диалога в групповом чате."""
    conversation_history = State()
    pending_clarification = State()


# Хранилище контекста диалогов: (chat_id, user_id) -> данные. LRU + TTL 1 час, макс. 1000 ключей.
_conversation_contexts: TTLCache = TTLCache(maxsize=1000, ttl=3600)

# Сообщения, по которым бот не нашёл ответа: (chat_id, message_id) -> {question_text, timestamp}. TTL 2 часа.
# Используется для связки с ответом менеджера (reply) и предложения записать в базу.
_awaiting_answer_cache: TTLCache = TTLCache(maxsize=2000, ttl=7200)

# Ожидающие подтверждения записи (для callback): question_hash -> {question, answer, chat_id, ...}. TTL 1 час.
_kb_confirm_pending: TTLCache = TTLCache(maxsize=500, ttl=3600)


def _get_context_key(chat_id: int, user_id: int) -> tuple[int, int]:
    """Возвращает ключ для хранения контекста."""
    return (chat_id, user_id)


def _get_user_context(chat_id: int, user_id: int) -> Dict[str, Any]:
    """Получает контекст диалога пользователя."""
    key = _get_context_key(chat_id, user_id)
    try:
        return _conversation_contexts[key]
    except KeyError:
        default = {
            "conversation_history": [],
            "pending_clarification": None,
            "clarification_rounds": 0,
        }
        _conversation_contexts[key] = default
        return default


def _update_user_context(chat_id: int, user_id: int, updates: Dict[str, Any]) -> None:
    """Обновляет контекст диалога пользователя."""
    context = _get_user_context(chat_id, user_id)
    context.update(updates)
    
    # Ограничиваем историю последними 10 сообщениями
    if "conversation_history" in updates:
        context["conversation_history"] = context["conversation_history"][-10:]


def _is_top_score_sufficient_for_answer(found_chunks: List[Dict[str, Any]], min_score: float) -> bool:
    """True если лучший score среди чанков >= min_score. Иначе False (в т.ч. при пустом списке)."""
    if not found_chunks:
        return False
    return max((c.get("score", 0) for c in found_chunks), default=0) >= min_score


async def _is_question(message_text: str) -> bool:
    """Определяет через AI, является ли сообщение вопросом, требующим ответа от поддержки/базы знаний."""
    try:
        prompt = (
            "Является ли это сообщение вопросом или запросом информации, на который ожидается ответ от поддержки или базы знаний "
            "(франшиза, касса, регламент, лояльность, документы, процедуры, ИП, ЕГАИС и т.п.)?\n"
            "Считай вопросом/запросом:\n"
            "- с вопросительным знаком: «какой документ подписать?», «касса у кого заказывать?», «где регламент?»;\n"
            "- без знака вопроса: «Подскажите какой документ подписать», «Кто помогает с ЕГАИС», «Нужна информация когда открывать ИП и счет», «Хочу уточнить про регламент»;\n"
            "- короткие запросы по теме: «Регламент по открытию магазина», «Документы для начала работы», «Кассовое оборудование» — человек просит информацию из базы.\n"
            "НЕ считай вопросом: приветствия (Привет, добрый день, здавствуйте), благодарности (Спасибо, благодарю), "
            "«Готово», «Проверьте пожалуйста», «Минуту через 5», короткие согласия, запросы контактов или конкретных людей по имени. "
            "Одно слово «Привет» или «Готово» или «Спасибо» — всегда 'no'.\n"
            "Ответь только 'yes' или 'no'.\n\n"
            f"Сообщение: {message_text}"
        )
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник. Определяешь, ждёт ли пользователь ответа от базы знаний/поддержки. Вопрос может быть без «?». Формулировки «Нужна информация…», «Подскажите…», «Хочу уточнить…», а также короткие темы («Регламент по открытию», «Документы для начала») — это запросы к базе. Отвечай только 'yes' или 'no'."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        answer = (resp.choices[0].message.content or "").strip().lower()
        return answer.startswith("yes")
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка определения вопроса: {e}")
        return "?" in message_text


async def _check_sufficient_data(
    question: str,
    found_chunks: List[Dict[str, Any]],
) -> tuple[bool, Optional[str]]:
    """Проверяет через AI, достаточно ли данных для ответа.
    
    Returns:
        (достаточно, недостающая_информация)
    """
    if not found_chunks:
        return (False, "Не найдено релевантных фрагментов в базе знаний")
    
    try:
        chunks_text = "\n\n".join([
            f"Фрагмент {i+1}:\n{chunk.get('text', '')[:500]}"
            for i, chunk in enumerate(found_chunks[:3])  # Берем топ-3
        ])
        
        prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"Найденные фрагменты из базы знаний:\n{chunks_text}\n\n"
            "Достаточно ли этих фрагментов для полного ответа на вопрос?\n"
            "Ответь 'yes' или 'no'.\n"
            "Если 'no', укажи кратко, какая информация отсутствует."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник для оценки достаточности данных для ответа."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        
        answer = (resp.choices[0].message.content or "").strip().lower()
        
        if answer.startswith("yes"):
            return (True, None)
        else:
            # Извлекаем недостающую информацию
            missing_info = answer.replace("no", "").strip()
            if not missing_info:
                missing_info = "Недостаточно информации для полного ответа"
            return (False, missing_info)
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка проверки достаточности данных: {e}")
        # При ошибке считаем, что данных достаточно
        return (True, None)


# Константы для уточнений по чанкам
CLARIFICATION_MAX_CHUNKS = 5
CLARIFICATION_CHUNK_CHARS = 400


async def _ask_clarification_question(
    message: Message,
    question: str,
    found_chunks: List[Dict[str, Any]],
    missing_info: str,
) -> None:
    """Задает уточняющий вопрос пользователю. При наличии чанков — помогает выбрать среди них."""
    try:
        if found_chunks:
            # Есть чанки: уточнение привязано к конкретным найденным фрагментам
            chunks_list = "\n\n".join([
                f"Фрагмент {i+1} (релевантность: {chunk.get('score', 0):.2f}):\n{chunk.get('text', '')[:CLARIFICATION_CHUNK_CHARS]}"
                for i, chunk in enumerate(found_chunks[:CLARIFICATION_MAX_CHUNKS])
            ])
            missing_note = f"\nСистема отметила: {missing_info[:200]}." if missing_info else ""
            system_content = (
                "Ты помощник, который формулирует уточняющие вопросы. "
                "Твоя задача — по перечисленным ниже фрагментам базы знаний сформулировать один уточняющий вопрос, "
                "который поможет пользователю выбрать среди этих конкретных вариантов (или сузить тему к одному из фрагментов). "
                "Можно использовать формат «Вас интересует» с вариантами 1), 2), 3). "
                "Важно: каждый вариант пиши с новой строки (1) на одной строке, 2) на следующей, 3) на следующей) — так удобнее читать. "
                "Не придумывай варианты, которых нет в приведённых фрагментах. Вопрос должен быть конкретным и понятным."
            )
            user_content = (
                f"Вопрос пользователя: {question}\n\n"
                f"Найденные фрагменты из базы знаний:\n{chunks_list}\n\n"
                f"{missing_note}\n\n"
                "Сформулируй один уточняющий вопрос, помогающий выбрать среди этих вариантов."
            )
        else:
            # Нет чанков: текущая логика по missing_info
            prompt = (
                f"Пользователь спросил: {question}\n\n"
                f"Недостающая информация: {missing_info}\n\n"
                "Сформулируй один уточняющий вопрос, который поможет найти нужный ответ.\n"
                "Вопрос должен быть конкретным и понятным. Если перечисляешь варианты (1), 2), 3)) — каждый вариант с новой строки."
            )
            system_content = "Ты помощник, который формулирует уточняющие вопросы."
            user_content = prompt

        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content},
            ],
            temperature=0.3,
        )

        clarification_text = resp.choices[0].message.content or "Можете уточнить ваш вопрос?"
        intro = "Чтобы ответить точнее, нужны уточнения.\n\n"
        clarification = intro + clarification_text

        await message.answer(clarification)

        # Сохраняем в контекст
        _update_user_context(
            message.chat.id,
            message.from_user.id if message.from_user else 0,
            {"pending_clarification": question},
        )

        await alog_event(
            user_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            event="kb_clarification_asked",
            meta={"original_question": question, "missing_info": missing_info},
        )
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка формулировки уточняющего вопроса: {e}")
        await message.answer("Можете уточнить ваш вопрос?")


async def _generate_answer_from_chunks(
    question: str,
    chunks: List[Dict[str, Any]],
    conversation_history: List[Dict[str, str]],
    *,
    is_first_turn: bool = False,
    user_name: str = "пользователь",
) -> str:
    """Генерирует ответ на основе найденных чанков."""
    try:
        # Формируем контекст диалога
        history_text = ""
        if conversation_history:
            history_lines = []
            for msg in conversation_history[-5:]:  # Последние 5 сообщений
                role = "Пользователь" if msg.get("role") == "user" else "Бот"
                text = msg.get("text", "")
                if text:
                    history_lines.append(f"{role}: {text}")
            history_text = "\n".join(history_lines)
        
        # Формируем текст найденных чанков
        chunks_text = "\n\n---\n\n".join([
            f"Фрагмент {i+1}:\n{chunk.get('text', '')}"
            for i, chunk in enumerate(chunks)
        ])

        # Инструкция по приветствию (при первом обращении в диалоге)
        if is_first_turn:
            greeting_instruction = (
                f"ВАЖНО: Это первое обращение пользователя в этой беседе. "
                f"Приветствуй его по имени ({user_name}) в начале ответа, например: «Привет, {user_name}!» или «Здравствуй, {user_name}!»"
            )
        else:
            greeting_instruction = (
                "ВАЖНО: Это НЕ первое сообщение в диалоге. НЕ приветствуй пользователя заново, не используй «Привет» или «Здравствуй» в начале."
            )
        
        system_prompt = (
            "Ты — AI-ассистент корпоративного бота «Воблаbeer». Общайся как живой менеджер поддержки: тепло, ясно, без канцелярита.\n\n"
            f"{greeting_instruction}\n\n"
            "Стиль:\n"
            "- Пиши на русском, дружелюбно и по делу. Тон: вежливый менеджер в чате.\n"
            "- Не начинай каждый ответ с «Здравствуйте». Приветствие только в первом сообщении диалога или если пользователь сам поздоровался.\n"
            "- Короткие абзацы. Используй списки и шаги (1–2–3), где уместно. 1–3 уместных эмодзи, без перебора.\n"
            "- Простой вопрос — краткий ответ. Сложный — разбивай на шаги.\n\n"
            "Структура ответа:\n"
            "1) Сначала краткий вывод или ответ (если можно дать сразу).\n"
            "2) Затем конкретные действия/инструкция (что сделать).\n"
            "3) В конце — один уточняющий вопрос только если он действительно нужен для точности. Если пользователь просит «просто ответ» — не задавай уточнений.\n\n"
            "Работа с фрагментами базы знаний (критично):\n"
            "- Отвечай ТОЛЬКО на основе предоставленных фрагментов. НЕ выдумывай факты, цифры, сроки, названия, стандарты.\n"
            "- Для каждого факта указывай номер фрагмента (1, 2, …), если уместно. Не используй информацию не из фрагментов.\n"
            "- Можно перефразировать и улучшать читаемость, но НЕ менять смысл.\n"
            "- Если информации в фрагментах недостаточно — скажи об этом честно и предложи уточнение или передачу менеджеру.\n"
            "- Если несколько фрагментов релевантны — объединяй в один связный ответ без противоречий. Если фрагменты противоречат друг другу — скажи об этом и попроси уточнение (контекст/город/точка и т.д.).\n\n"
            "Формат:\n"
            "- Без длинных вступлений. Не повторяй вопрос пользователя целиком, если не нужно прояснить.\n"
            "- Инструкции — шагами 1–2–3. Важное выделяй одной строкой: «Важно: …».\n\n"
            "Запрещено:\n"
            "- Придумывать стандарты, требования, юридические/финансовые детали без опоры на фрагменты.\n"
            "- Длинные полотна текста, капс, спам эмодзи."
        )
        
        user_prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"{'Контекст диалога:\n' + history_text + '\n\n' if history_text else ''}"
            f"Фрагменты из базы знаний:\n{chunks_text}\n\n"
            "Сформулируй ответ на основе этих фрагментов."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
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
        logger.exception(f"[GROUP_CHAT_QA] Ошибка генерации ответа: {e}")
        return "Извините, произошла ошибка при формировании ответа."


async def _should_escalate_to_manager(
    found_chunks: List[Dict[str, Any]],
    ai_decision: tuple[bool, Optional[str]],
) -> bool:
    """Определяет, нужно ли эскалировать вопрос менеджеру."""
    sufficient, missing_info = ai_decision
    
    # Если нет чанков вообще
    if not found_chunks:
        return True
    
    # Если AI сказал, что данных недостаточно и информация критична
    if not sufficient:
        # Проверяем, не является ли это просто уточнением деталей
        if missing_info and any(word in missing_info.lower() for word in ["конкретн", "детал", "уточн"]):
            return False  # Можно задать уточняющий вопрос
        return True
    
    # Если максимальный score слишком низкий
    max_score = max((chunk.get("score", 0) for chunk in found_chunks), default=0)
    if max_score < 0.5:
        return True
    
    return False


def _add_awaiting_answer(chat_id: int, message_id: int, question_text: str) -> None:
    """Регистрирует сообщение как «вопрос без ответа» для последующей связки с ответом менеджера."""
    key = (chat_id, message_id)
    _awaiting_answer_cache[key] = {"question_text": question_text, "timestamp": time.time()}
    logger.info("[GROUP_CHAT_QA] Вопрос без ответа добавлен в кэш awaiting_answer: chat_id=%s message_id=%s", chat_id, message_id)


async def _maybe_send_no_answer_reply(message: Message) -> None:
    """Если включено RAG_SEND_NO_ANSWER_REPLY, отправляет короткое сообщение о том, что ответ в базе не найден."""
    if not RAG_SEND_NO_ANSWER_REPLY:
        return
    try:
        await message.answer("Пока не нашёл ответ в базе. Менеджер может ответить позже.")
    except Exception as e:
        logger.warning("[GROUP_CHAT_QA] Не удалось отправить no_answer_reply: %s", e)


def _get_awaiting_answer(chat_id: int, message_id: int) -> Optional[Dict[str, Any]]:
    """Возвращает данные по сообщению из кэша «ожидающих ответа» или None."""
    key = (chat_id, message_id)
    try:
        return _awaiting_answer_cache[key]
    except KeyError:
        return None


def _pop_awaiting_answer(chat_id: int, message_id: int) -> Optional[Dict[str, Any]]:
    """Извлекает и удаляет запись из кэша «ожидающих ответа»."""
    key = (chat_id, message_id)
    try:
        data = _awaiting_answer_cache.pop(key)
        return data
    except KeyError:
        return None


async def _tag_manager_in_chat(message: Message, question: str) -> None:
    """Тегирует менеджеров в чате (не используется при «молчании» — оставлено на случай отката)."""
    username = message.from_user.username if message.from_user else None
    user_name = message.from_user.full_name if message.from_user else "Пользователь"
    
    # Формируем теги менеджеров
    manager_tags = " ".join([f"@{username}" for username in MANAGER_USERNAMES if username])
    
    text = (
        f"❓ Вопрос от {user_name}"
        f"{f' (@{username})' if username else ''}:\n\n"
        f"{question}\n\n"
        f"Не нашел ответа в базе знаний. {manager_tags}"
    )
    
    await message.answer(text)
    
    # Сохраняем в контекст для перехвата ответа менеджера
    _update_user_context(
        message.chat.id,
        message.from_user.id if message.from_user else 0,
        {"pending_manager_answer": {"question": question, "asked_by": message.from_user.id if message.from_user else 0}},
    )
    
    await alog_event(
        user_id=message.from_user.id if message.from_user else None,
        username=username,
        event="kb_manager_tagged",
        meta={"question": question},
    )


async def process_question_in_group_chat(message: Message, question_text_override: Optional[str] = None) -> None:
    """Обрабатывает вопрос в групповом чате. question_text_override: текст вопроса (для голоса/фото с подписью)."""
    if not message.from_user:
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    question = (question_text_override or (message.text or "")).strip()
    if not question:
        return
    
    # Получаем контекст диалога
    context = _get_user_context(chat_id, user_id)
    conversation_history = context.get("conversation_history", [])
    pending_clarification = context.get("pending_clarification")

    # При ожидании ответа на уточнение — ответ на уточнение или новый вопрос?
    if pending_clarification:
        q_stripped = question.strip()
        # Короткий ответ-выбор (1, 2, 3, вариант 1 и т.п.) — всегда считаем ответом на уточнение, не вызываем LLM
        is_short_choice = (
            len(q_stripped) <= 15
            and (
                q_stripped.isdigit()
                or (q_stripped.lower().startswith("вариант ") and q_stripped.split()[-1].isdigit())
                or q_stripped.lower() in ("1)", "2)", "3)", "4)", "первый", "второй", "третий", "четвёртый", "четвертый")
            )
        )
        if is_short_choice:
            clarification_vs_new = "clarification_response"
            logger.info("[GROUP_CHAT_QA] Короткий ответ-выбор, считаем ответом на уточнение")
        else:
            last_assistant_msg = ""
            for msg in reversed(conversation_history):
                if msg.get("role") == "assistant":
                    last_assistant_msg = msg.get("text", "")
                    break
            clarification_vs_new = await detect_clarification_response_vs_new_question(
                question, last_assistant_msg, pending_clarification
            )
        if clarification_vs_new == "new_question":
            # Новый вопрос — не объединяем; если написал «другой вопрос» — ищем по предыдущему его вопросу
            logger.info("[GROUP_CHAT_QA] LLM: новый вопрос вместо ответа на уточнение, обрабатываем отдельно")
            original_msg = question
            q_clean = question.strip().lower()
            if (q_clean in ("другой вопрос", "другой вопрос.", "новая тема", "по другой теме") or
                    (len(question) < 30 and "другой" in q_clean and "вопрос" in q_clean)):
                for msg in reversed(conversation_history):
                    if msg.get("role") == "user":
                        prev_text = (msg.get("text") or "").strip()
                        if len(prev_text) > 10 and ("?" in prev_text or "как" in prev_text or "что" in prev_text or "какие" in prev_text):
                            question = prev_text
                            logger.info(f"[GROUP_CHAT_QA] «Другой вопрос» — ищем по предыдущему: '{question[:80]}...'")
                            break
            query_text = question
            conversation_history.append({"role": "user", "text": original_msg})
            _update_user_context(chat_id, user_id, {"conversation_history": conversation_history, "pending_clarification": None, "clarification_rounds": 0})
        else:
            # Ответ на уточнение — объединяем как раньше
            combined = f"Исходный вопрос: {pending_clarification}\nУточнение пользователя: {question}"
            query_text = combined
            conversation_history.append({"role": "user", "text": combined})
            _update_user_context(chat_id, user_id, {"conversation_history": conversation_history, "pending_clarification": None})
    else:
        query_text = None  # определим ниже
        conversation_history.append({"role": "user", "text": question})
        # Новый вопрос — сбрасываем счётчик раундов уточнений
        _update_user_context(chat_id, user_id, {"clarification_rounds": 0})
    
    # Анализ выполняем молча: ничего не отправляем в группу до итогового ответа.
    try:
        # Определяем запрос для поиска (при ответе на уточнение уже задан query_text)
        if query_text is None:
            q_clean = question.strip().lower()
            if (q_clean in ("другой вопрос", "другой вопрос.", "новая тема", "по другой теме") or
                    (len(question) < 30 and "другой" in q_clean and "вопрос" in q_clean)):
                for msg in reversed(conversation_history[:-1]):
                    if msg.get("role") == "user":
                        prev_text = (msg.get("text") or "").strip()
                        if len(prev_text) > 10 and ("?" in prev_text or "как" in prev_text or "что" in prev_text or "какие" in prev_text):
                            query_text = prev_text
                            logger.info(f"[GROUP_CHAT_QA] Пользователь написал «другой вопрос», ищем по предыдущему: '{query_text[:80]}...'")
                            break
            if query_text is None:
                # Новый вопрос — ищем только по текущему сообщению, без контекста прошлых ответов
                query_text = question

        # Режим «полный файл в контексте»: ответ по одному документу без RAG
        from app.services.full_file_context import get_full_file_context
        document = get_full_file_context()
        if USE_FULL_FILE_CONTEXT and document:
            is_first_turn = not any(m.get("role") == "assistant" for m in conversation_history)
            user_name = (message.from_user.first_name or "пользователь") if message.from_user else "пользователь"
            answer = await asyncio.to_thread(
                generate_answer_from_full_document,
                question,
                document,
                conversation_history,
                user_name=user_name,
                is_first_turn=is_first_turn,
            )
            await message.answer(answer)
            conversation_history.append({"role": "assistant", "text": answer})
            _update_user_context(chat_id, user_id, {"conversation_history": conversation_history})
            _qh = str(hash((query_text or question).strip().lower()[:200]))
            await alog_event(
                user_id=user_id,
                username=message.from_user.username,
                event="rag_pipeline",
                meta={"question_hash": _qh, "chunks_found": 0, "outcome": "answer", "source": "full_file"},
            )
            return

        # Усиленный RAG: расширение запроса, несколько поисков, re-ranking (как в приватном чате)
        from app.services.rag_query_cache import get_cached_chunks, set_cached_chunks
        cached = get_cached_chunks(query_text)
        if cached is not None:
            found_chunks = [c for c in cached if c.get("score", 0) >= MIN_SCORE_AFTER_RERANK]
            if not found_chunks:
                logger.info("[GROUP_CHAT_QA] Кэш: нет чанков выше MIN_SCORE_AFTER_RERANK, молчим")
                _qh = str(hash(query_text.strip().lower()[:200])) if query_text else ""
                await alog_event(user_id=user_id, username=message.from_user.username, event="rag_pipeline", meta={"question_hash": _qh, "chunks_found": 0, "outcome": "no_answer_silent", "from_cache": True})
                await _maybe_send_no_answer_reply(message)
                _add_awaiting_answer(chat_id, message.message_id, query_text)
                return
            question_hash = str(hash(query_text.strip().lower()[:200])) if query_text else ""
            await alog_event(user_id=user_id, username=message.from_user.username, event="kb_search_performed", meta={"question_hash": question_hash, "chunks_found": len(found_chunks), "top_scores": [round(c.get("score", 0), 3) for c in found_chunks[:3]], "top_sources": [str((c.get("metadata") or {}).get("source", ""))[:50] for c in found_chunks[:3]], "from_cache": True})
        else:
            expanded_query = await _expand_query_for_search(query_text)
            qdrant_service = get_qdrant_service()
            all_found_chunks = []
            seen_texts = set()
            # Поиск 1: расширенный запрос
            embedding_expanded = await asyncio.to_thread(create_embedding, expanded_query)
            chunks_expanded = qdrant_service.search_multi_level(
                query_embedding=embedding_expanded,
                top_k=5,
                initial_threshold=0.5,
                fallback_thresholds=[0.3, 0.1],
            )
            for chunk in chunks_expanded:
                t = chunk.get("text", "")
                if t and t not in seen_texts:
                    all_found_chunks.append(chunk)
                    seen_texts.add(t)
            # Поиск 2: оригинальный запрос (если отличается)
            if query_text.strip() != expanded_query.strip() and len(query_text.strip()) > 5:
                embedding_original = await asyncio.to_thread(create_embedding, query_text)
                chunks_original = qdrant_service.search_multi_level(
                    query_embedding=embedding_original,
                    top_k=5,
                    initial_threshold=0.5,
                    fallback_thresholds=[0.3, 0.1],
                )
                for chunk in chunks_original:
                    t = chunk.get("text", "")
                    if t and t not in seen_texts:
                        all_found_chunks.append(chunk)
                        seen_texts.add(t)
            # Поиск 3: ключевые слова из вопроса
            keywords = re.findall(r"\b\w{4,}\b", query_text.lower())
            if keywords and len(keywords) >= 2:
                keywords_query = " ".join(keywords[:5])
                if keywords_query != query_text.lower() and len(keywords_query) > 5:
                    embedding_kw = await asyncio.to_thread(create_embedding, keywords_query)
                    chunks_kw = qdrant_service.search_multi_level(
                        query_embedding=embedding_kw,
                        top_k=3,
                        initial_threshold=0.4,
                        fallback_thresholds=[0.2, 0.1],
                    )
                    for chunk in chunks_kw:
                        t = chunk.get("text", "")
                        if t and t not in seen_texts:
                            all_found_chunks.append(chunk)
                            seen_texts.add(t)
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
            all_found_chunks.sort(key=lambda x: x.get("score", 0), reverse=True)
            use_cohere_rerank = USE_CROSS_ENCODER_RERANK and bool(COHERE_API_KEY)
            candidates_limit = RERANK_CANDIDATES_LIMIT if use_cohere_rerank else 15
            if USE_HYBRID_BM25 and all_found_chunks:
                from app.services.bm25_search import hybrid_vector_bm25
                initial_chunks = hybrid_vector_bm25(query_text, all_found_chunks, top_n=candidates_limit)
            else:
                initial_chunks = all_found_chunks[:candidates_limit]
            if initial_chunks:
                try:
                    reranked_chunks = await rerank_chunks_with_llm(query_text, initial_chunks, top_k=8)
                    found_chunks = select_best_chunks(reranked_chunks, max_chunks=5, min_score=0.1)
                    found_chunks = [c for c in found_chunks if c.get("score", 0) >= MIN_SCORE_AFTER_RERANK]
                except Exception as e:
                    logger.exception(f"[GROUP_CHAT_QA] Ошибка re-ranking: {e}")
                    found_chunks = [c for c in initial_chunks[:5] if c.get("score", 0) >= MIN_SCORE_AFTER_RERANK]
            else:
                found_chunks = []
            if not found_chunks:
                logger.info("[GROUP_CHAT_QA] Нет чанков выше MIN_SCORE_AFTER_RERANK, молчим")
                _qh = str(hash(query_text.strip().lower()[:200])) if query_text else ""
                await alog_event(user_id=user_id, username=message.from_user.username, event="rag_pipeline", meta={"question_hash": _qh, "chunks_found": 0, "outcome": "no_answer_silent"})
                await _maybe_send_no_answer_reply(message)
                _add_awaiting_answer(chat_id, message.message_id, query_text)
                return
            set_cached_chunks(query_text, found_chunks)
            question_hash = str(hash(query_text.strip().lower()[:200])) if query_text else ""
            await alog_event(
                user_id=user_id,
                username=message.from_user.username,
                event="kb_search_performed",
                meta={
                    "question_hash": question_hash,
                    "chunks_found": len(found_chunks),
                    "top_scores": [round(c.get("score", 0), 3) for c in found_chunks[:3]],
                    "top_sources": [str((c.get("metadata") or {}).get("source", ""))[:50] for c in found_chunks[:3]],
                },
            )
        
        # Порог уверенности: отвечаем только если лучший чанк выше MIN_TOP_SCORE_FOR_ANSWER
        top_score = max((c.get("score", 0) for c in found_chunks), default=0)
        if not _is_top_score_sufficient_for_answer(found_chunks, MIN_TOP_SCORE_FOR_ANSWER):
            logger.info("[GROUP_CHAT_QA] Топ score %.3f < MIN_TOP_SCORE_FOR_ANSWER, молчим", top_score)
            await alog_event(user_id=user_id, username=message.from_user.username, event="rag_pipeline", meta={"question_hash": question_hash, "outcome": "no_answer_silent", "top_score": top_score})
            await _maybe_send_no_answer_reply(message)
            _add_awaiting_answer(chat_id, message.message_id, query_text)
            return

        # Проверка достаточности данных (используем эффективный вопрос: объединённый при ответе на уточнение)
        sufficient, missing_info = await _check_sufficient_data(query_text, found_chunks)
        
        # Проверяем, нужно ли эскалировать менеджеру (при «молчании» — не тегаем, только кэш)
        if await _should_escalate_to_manager(found_chunks, (sufficient, missing_info)):
            await alog_event(user_id=user_id, username=message.from_user.username, event="rag_pipeline", meta={"question_hash": question_hash, "outcome": "no_answer_silent", "top_score": top_score, "reason": "escalate"})
            await _maybe_send_no_answer_reply(message)
            _add_awaiting_answer(chat_id, message.message_id, query_text)
            return

        # Если данных недостаточно — не задаём уточняющий вопрос в группе (молчим), считаем что нет ответа
        clarification_rounds = context.get("clarification_rounds", 0)
        if not sufficient and missing_info:
            if clarification_rounds >= MAX_CLARIFICATION_ROUNDS:
                sufficient = True
                missing_info = None
            else:
                await alog_event(user_id=user_id, username=message.from_user.username, event="rag_pipeline", meta={"question_hash": question_hash, "outcome": "no_answer_silent", "top_score": top_score, "reason": "insufficient_data"})
                await _maybe_send_no_answer_reply(message)
                _add_awaiting_answer(chat_id, message.message_id, query_text)
                return

        # Первое обращение в диалоге — приветствие в ответе
        is_first_turn = not any(m.get("role") == "assistant" for m in conversation_history)
        user_name = (message.from_user.first_name or "пользователь") if message.from_user else "пользователь"

        # Генерируем ответ (используем эффективный вопрос)
        answer = await _generate_answer_from_chunks(
            query_text, found_chunks, conversation_history,
            is_first_turn=is_first_turn,
            user_name=user_name,
        )
        grounded = await check_answer_grounding(answer, found_chunks)
        if not grounded:
            logger.warning("[GROUP_CHAT_QA] Ответ не обоснован фрагментами (grounding), молчим")
            await alog_event(user_id=user_id, username=message.from_user.username, event="rag_pipeline", meta={"question_hash": question_hash, "outcome": "no_answer_silent", "top_score": top_score, "reason": "grounding_fail"})
            await _maybe_send_no_answer_reply(message)
            _add_awaiting_answer(chat_id, message.message_id, query_text)
            return

        kilbil_urls = get_article_urls_from_chunks(found_chunks, only_if_top_from_kilbil=True)
        if kilbil_urls:
            if len(kilbil_urls) == 1:
                answer = answer + "\n\n📎 Подробнее: " + kilbil_urls[0]
            else:
                answer = answer + "\n\n📎 Подробнее:\n" + "\n".join(kilbil_urls)

        # Отправляем ответ (единственное сообщение в группу при успехе)
        await message.answer(answer)
        
        # Добавляем ответ в историю
        conversation_history.append({"role": "assistant", "text": answer})
        _update_user_context(chat_id, user_id, {"conversation_history": conversation_history})
        
        await alog_event(
            user_id=user_id,
            username=message.from_user.username,
            event="kb_answer_generated",
            meta={
                "question_hash": question_hash,
                "chunks_used": len(found_chunks),
                "outcome": "answer",
            },
        )
        
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка обработки вопроса: {e}")
        # Молчим: не отправляем сообщение об ошибке в группу


async def _get_question_text_from_message(message: Message) -> Optional[str]:
    """Извлекает текст вопроса из сообщения: текст, голосовое (→ транскрипция) или подпись к фото. Фото без подписи → None."""
    if message.text and message.text.strip():
        return message.text.strip()
    if message.voice:
        try:
            from io import BytesIO
            from app.services.voice_transcription_service import transcribe_voice
            from app.config import WHISPER_MAX_FILE_BYTES
            voice = message.voice
            file_size = getattr(voice, "file_size", None) or 0
            if file_size > WHISPER_MAX_FILE_BYTES:
                logger.warning("[GROUP_CHAT_QA] Голосовое слишком большое, пропускаем")
                return None
            buffer = BytesIO()
            await message.bot.download(voice.file_id, buffer)
            buffer.seek(0)
            voice_bytes = buffer.read()
            text = await asyncio.to_thread(transcribe_voice, voice_bytes, "audio/ogg")
            return (text or "").strip() or None
        except Exception as e:
            logger.exception("[GROUP_CHAT_QA] Ошибка транскрипции голоса: %s", e)
            return None
    if message.photo:
        if not message.caption or not message.caption.strip():
            return None
        return message.caption.strip()
    return None


@router.message(F.chat.type.in_(["group", "supergroup"]))
async def handle_group_chat_message(message: Message):
    """Обрабатывает сообщения в групповых чатах: текст, голосовое (→ текст) или фото с подписью."""
    if message.from_user and message.from_user.is_bot:
        return
    if message.text and message.text.startswith("/"):
        return

    question_text = await _get_question_text_from_message(message)
    if not question_text:
        return

    rag_test_chat_id = get_rag_test_chat_id()
    if rag_test_chat_id is not None and message.chat.id != rag_test_chat_id:
        return

    context = _get_user_context(message.chat.id, message.from_user.id if message.from_user else 0)
    if context.get("pending_clarification") is None:
        is_question = await _is_question(question_text)
        if not is_question:
            return

    await process_question_in_group_chat(message, question_text_override=question_text)


def _is_manager_message(message: Message) -> bool:
    """Проверяет, является ли отправитель менеджером."""
    if not message.from_user:
        return False
    username = message.from_user.username
    user_id = message.from_user.id
    if username and username in MANAGER_USERNAMES:
        return True
    user = find_user_by_telegram_id(user_id)
    if user:
        role = getattr(user, "role", "").strip().lower()
        if role in ("admin", "manager"):
            return True
    return False


async def _send_kb_proposal_to_managers(question: str, answer: str, payload: str, bot) -> None:
    """Отправляет в группу менеджеров предложение записать в базу с кнопками."""
    q_short = question[:500] + ("…" if len(question) > 500 else "")
    a_short = answer[:500] + ("…" if len(answer) > 500 else "")
    text = f"Собираюсь записать в базу знаний:\n\nВопрос: {q_short}\n\nОтвет: {a_short}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Записать в базу", callback_data=f"kb_confirm_add:{payload}")],
        [InlineKeyboardButton(text="Изменить ответ", callback_data=f"kb_edit_answer:{payload}")],
    ])
    await bot.send_message(KB_MANAGERS_CHAT_ID, text, reply_markup=keyboard)


# Кэш: message_id (в чате менеджеров) -> payload; для обработки «Изменить ответ» (ответ на это сообщение).
_kb_waiting_edit: TTLCache = TTLCache(maxsize=300, ttl=3600)


@router.message(F.chat.type.in_(["group", "supergroup"]), F.reply_to_message)
async def handle_manager_reply_to_user_question(message: Message):
    """Менеджер ответил на сообщение пользователя (по которому бот не нашёл ответа) — отправляем в группу менеджеров предложение записать в базу."""
    rag_test_chat_id = get_rag_test_chat_id()
    if rag_test_chat_id is not None and message.chat.id != rag_test_chat_id:
        return
    if message.from_user and message.from_user.is_bot:
        return
    if not _is_manager_message(message):
        return
    reply_to = message.reply_to_message
    if not reply_to or not reply_to.from_user or reply_to.from_user.is_bot:
        return
    # Ответ на сообщение пользователя (не бота)
    data = _pop_awaiting_answer(message.chat.id, reply_to.message_id)
    if not data:
        return
    question_text = data.get("question_text", "").strip()
    answer_text = (message.text or "").strip()
    if not answer_text:
        _add_awaiting_answer(message.chat.id, reply_to.message_id, question_text)
        return
    payload = hashlib.sha256((question_text + "|" + answer_text + "|" + str(time.time())).encode()).hexdigest()[:16]
    _kb_confirm_pending[payload] = {
        "question": question_text,
        "answer": answer_text,
        "source_chat_id": message.chat.id,
    }
    await _send_kb_proposal_to_managers(question_text, answer_text, payload, message.bot)
    await alog_event(
        user_id=message.from_user.id if message.from_user else None,
        username=message.from_user.username if message.from_user else None,
        event="kb_proposal_sent_to_managers",
        meta={"question": question_text[:200], "chat_id": message.chat.id},
    )


@router.message(F.chat.type.in_(["group", "supergroup"]), F.reply_to_message)
async def handle_manager_reply_in_group_chat(message: Message):
    """Перехватывает ответы менеджеров на вопросы в групповых чатах (reply на сообщение бота с тегом «Не нашел ответа»)."""
    # Если указан тестовый чат, обрабатываем только его
    rag_test_chat_id = get_rag_test_chat_id()
    if rag_test_chat_id is not None and message.chat.id != rag_test_chat_id:
        return
    
    # Игнорируем сообщения от бота
    if message.from_user and message.from_user.is_bot:
        return
    
    if not _is_manager_message(message):
        return
    
    # Проверяем, является ли reply_to_message от бота с тегом менеджера
    reply_to = message.reply_to_message
    if not reply_to or not reply_to.from_user or not reply_to.from_user.is_bot:
        return
    
    # Проверяем, содержит ли сообщение бота тег менеджера
    bot_message_text = reply_to.text or ""
    if "❓ Вопрос от" not in bot_message_text or "Не нашел ответа" not in bot_message_text:
        return
    
    # Извлекаем вопрос из сообщения бота
    # Формат: "❓ Вопрос от ...:\n\n{question}\n\nНе нашел ответа..."
    lines = bot_message_text.split("\n\n")
    if len(lines) < 2:
        return
    
    question = lines[1].strip()
    
    # Извлекаем ответ менеджера
    answer = message.text or ""
    if not answer.strip():
        return
    
    # Извлекаем медиа-вложения, если есть
    media_json = ""
    try:
        from app.handlers.manager_reply import _extract_media_attachments
        attachments = _extract_media_attachments(message)
        if attachments:
            import json
            media_json = json.dumps(attachments)
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка извлечения медиа: {e}")
    
    # Сохраняем в Qdrant
    try:
        # Импортируем функцию напрямую, чтобы избежать циклических зависимостей
        from app.services.chunking_service import semantic_chunk_text, extract_metadata_from_text
        from app.services.context_enrichment import enrich_chunks_batch
        from app.services.openai_client import create_embedding
        from app.services.qdrant_service import get_qdrant_service
        from datetime import datetime
        
        # Создаем текст: вопрос + ответ
        full_text = f"Вопрос: {question}\nОтвет: {answer}"
        
        # Разбиваем на чанки семантически
        chunks = semantic_chunk_text(full_text)
        if not chunks:
            chunks = [{
                "text": full_text,
                "chunk_index": 0,
                "total_chunks": 1,
                "start_char": 0,
                "end_char": len(full_text),
            }]
        
        # Обогащаем контекстом
        document_title = f"Ответ менеджера на вопрос"
        enriched_chunks = await enrich_chunks_batch(chunks, document_title)
        
        # Извлекаем метаданные из текста
        extracted_metadata = extract_metadata_from_text(full_text, source="manager_answer")
        
        # Создаем эмбеддинги
        embeddings = []
        for chunk in enriched_chunks:
            embedding = await asyncio.to_thread(create_embedding, chunk.get("text", ""))
            embeddings.append(embedding)
        
        # Подготавливаем метаданные с расширенными полями
        timestamp = datetime.now().isoformat()
        chunks_with_metadata = []
        for chunk in enriched_chunks:
            chunks_with_metadata.append({
                "text": chunk.get("text", ""),
                "metadata": {
                    "source": "manager_answer",
                    "document_type": extracted_metadata.get("document_type", "faq"),
                    "category": extracted_metadata.get("category", "общее"),
                    "tags": extracted_metadata.get("tags", []),
                    "keywords": extracted_metadata.get("keywords", []),
                    "question": question,
                    "answer": answer,
                    "manager_id": user_id,
                    "chat_id": message.chat.id,
                    "answered_at": timestamp,
                    "media_json": media_json,
                    "document_title": document_title,
                },
            })
        
        # Загружаем в Qdrant
        qdrant_service = get_qdrant_service()
        qdrant_service.add_documents(chunks_with_metadata, embeddings)
        
        # Отправляем подтверждение менеджеру
        await message.answer("✅ Ответ сохранен в базу знаний")
        
        # Логируем событие
        await alog_event(
            user_id=user_id,
            username=username,
            event="kb_manager_answer_saved",
            meta={"question": question, "chat_id": message.chat.id},
        )
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка сохранения ответа менеджера: {e}")
        await message.answer("❌ Ошибка при сохранении ответа в базу знаний")


@router.callback_query(F.data.startswith("kb_confirm_add:"))
async def on_kb_confirm_add(callback: CallbackQuery):
    """Кнопка «Записать в базу» — сохраняем вопрос/ответ в FAQ и Qdrant."""
    payload = callback.data.replace("kb_confirm_add:", "").strip()
    try:
        pending = _kb_confirm_pending.pop(payload)
    except KeyError:
        await callback.answer("Истекло время. Запрос уже обработан или устарел.")
        return
    question = pending.get("question", "")
    answer = pending.get("answer", "")
    if not question or not answer:
        await callback.answer("Нет данных для записи.")
        return
    try:
        from app.services.faq_service import add_faq_entry_to_cache
        await add_faq_entry_to_cache(question, answer, "")
        await callback.answer("Записано")
        await callback.bot.send_message(KB_MANAGERS_CHAT_ID, "✅ Записано в базу знаний.")
        await alog_event(
            user_id=callback.from_user.id if callback.from_user else None,
            username=callback.from_user.username if callback.from_user else None,
            event="kb_confirmed_add_from_chat",
            meta={"question": question[:200]},
        )
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка записи в базу по кнопке: {e}")
        await callback.answer("Ошибка при записи.")
        await callback.bot.send_message(KB_MANAGERS_CHAT_ID, "❌ Ошибка при записи в базу знаний.")


@router.callback_query(F.data.startswith("kb_edit_answer:"))
async def on_kb_edit_answer(callback: CallbackQuery):
    """Кнопка «Изменить ответ» — просим написать исправленный ответ ответом на сообщение."""
    payload = callback.data.replace("kb_edit_answer:", "").strip()
    try:
        pending = _kb_confirm_pending[payload]
    except KeyError:
        await callback.answer("Истекло время.")
        return
    msg = await callback.bot.send_message(
        KB_MANAGERS_CHAT_ID,
        "Напишите исправленный ответ ответом на это сообщение.",
    )
    _kb_waiting_edit[msg.message_id] = payload
    await callback.answer("Ожидаю исправленный ответ...")


@router.message(F.chat.id == KB_MANAGERS_CHAT_ID, F.reply_to_message)
async def handle_managers_chat_edit_reply(message: Message):
    """В группе менеджеров пришёл ответ на «Напишите исправленный ответ» — обновляем ответ и снова показываем кнопки."""
    if message.from_user and message.from_user.is_bot:
        return
    reply_to = message.reply_to_message
    if not reply_to or not reply_to.from_user or not reply_to.from_user.is_bot:
        return
    try:
        payload = _kb_waiting_edit.pop(reply_to.message_id)
    except KeyError:
        return
    try:
        pending = _kb_confirm_pending[payload]
    except KeyError:
        return
    new_answer = (message.text or "").strip()
    if not new_answer:
        _kb_waiting_edit[reply_to.message_id] = payload
        return
    pending["answer"] = new_answer
    await _send_kb_proposal_to_managers(pending["question"], new_answer, payload, message.bot)
