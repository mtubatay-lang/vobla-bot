"""Обработка вопросов в групповых чатах с использованием RAG из Qdrant."""

import asyncio
import logging
import re
from typing import List, Dict, Any, Optional

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from app.services.auth_service import find_user_by_telegram_id
from app.services.qdrant_service import get_qdrant_service
from app.services.openai_client import create_embedding, client, CHAT_MODEL
from app.services.metrics_service import alog_event
from app.services.reranking_service import rerank_chunks_with_llm, select_best_chunks
from app.config import MANAGER_USERNAMES, get_rag_test_chat_id
from app.handlers.qa_mode import _expand_query_for_search

logger = logging.getLogger(__name__)

router = Router()


class GroupChatQAState(StatesGroup):
    """Состояния для контекста диалога в групповом чате."""
    conversation_history = State()
    pending_clarification = State()


# Хранилище контекста диалогов: (chat_id, user_id) -> данные
_conversation_contexts: Dict[tuple[int, int], Dict[str, Any]] = {}


def _get_context_key(chat_id: int, user_id: int) -> tuple[int, int]:
    """Возвращает ключ для хранения контекста."""
    return (chat_id, user_id)


def _get_user_context(chat_id: int, user_id: int) -> Dict[str, Any]:
    """Получает контекст диалога пользователя."""
    key = _get_context_key(chat_id, user_id)
    if key not in _conversation_contexts:
        _conversation_contexts[key] = {
            "conversation_history": [],
            "pending_clarification": None,
        }
    return _conversation_contexts[key]


def _update_user_context(chat_id: int, user_id: int, updates: Dict[str, Any]) -> None:
    """Обновляет контекст диалога пользователя."""
    context = _get_user_context(chat_id, user_id)
    context.update(updates)
    
    # Ограничиваем историю последними 10 сообщениями
    if "conversation_history" in updates:
        context["conversation_history"] = context["conversation_history"][-10:]


async def _is_question(message_text: str) -> bool:
    """Определяет через AI, является ли сообщение вопросом."""
    try:
        prompt = (
            "Определи, является ли это сообщение вопросом, требующим ответа.\n"
            "Ответь только 'yes' или 'no'.\n\n"
            f"Сообщение: {message_text}"
        )
        
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник для определения, является ли сообщение вопросом. Отвечай только 'yes' или 'no'."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        
        answer = (resp.choices[0].message.content or "").strip().lower()
        return answer.startswith("yes")
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка определения вопроса: {e}")
        # Fallback: считаем вопросом, если есть знак вопроса
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
        
        resp = client.chat.completions.create(
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


async def _ask_clarification_question(
    message: Message,
    question: str,
    found_chunks: List[Dict[str, Any]],
    missing_info: str,
) -> None:
    """Задает уточняющий вопрос пользователю."""
    try:
        chunks_summary = "\n".join([
            f"- {chunk.get('text', '')[:200]}..."
            for chunk in found_chunks[:2]
        ])
        
        prompt = (
            f"Пользователь спросил: {question}\n\n"
            f"Найденные фрагменты:\n{chunks_summary}\n\n"
            f"Недостающая информация: {missing_info}\n\n"
            "Сформулируй один уточняющий вопрос, который поможет найти нужный ответ.\n"
            "Вопрос должен быть конкретным и понятным."
        )
        
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты помощник, который формулирует уточняющие вопросы."},
                {"role": "user", "content": prompt},
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
        
        system_prompt = (
            "Ты — AI-ассистент корпоративного бота «Воблаbeer». Общайся как живой менеджер поддержки: тепло, ясно, без канцелярита.\n\n"
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


async def _tag_manager_in_chat(message: Message, question: str) -> None:
    """Тегирует менеджеров в чате."""
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


async def process_question_in_group_chat(message: Message) -> None:
    """Обрабатывает вопрос в групповом чате."""
    if not message.from_user:
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    question = message.text.strip()
    
    # Получаем контекст диалога
    context = _get_user_context(chat_id, user_id)
    conversation_history = context.get("conversation_history", [])
    pending_clarification = context.get("pending_clarification")

    # Ответ на уточнение: объединяем исходный вопрос и уточнение, сбрасываем флаг
    if pending_clarification:
        combined = f"Исходный вопрос: {pending_clarification}\nУточнение пользователя: {question}"
        query_text = combined
        conversation_history.append({"role": "user", "text": combined})
        _update_user_context(chat_id, user_id, {"conversation_history": conversation_history, "pending_clarification": None})
    else:
        query_text = None  # определим ниже
        conversation_history.append({"role": "user", "text": question})
    
    try:
        # Определяем запрос для поиска (при ответе на уточнение уже задан query_text)
        if query_text is None:
            context_text = "\n".join([
                msg.get("text", "") for msg in conversation_history[-3:]
            ])
            query_text = f"{context_text}\n{question}" if context_text else question
        
        # Усиленный RAG: расширение запроса, несколько поисков, re-ranking (как в приватном чате)
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
        
        all_found_chunks.sort(key=lambda x: x.get("score", 0), reverse=True)
        initial_chunks = all_found_chunks[:10]
        
        if initial_chunks:
            try:
                reranked_chunks = await rerank_chunks_with_llm(query_text, initial_chunks, top_k=8)
                found_chunks = select_best_chunks(reranked_chunks, max_chunks=5, min_score=0.1)
            except Exception as e:
                logger.exception(f"[GROUP_CHAT_QA] Ошибка re-ranking: {e}")
                found_chunks = initial_chunks[:5]
        else:
            found_chunks = []
        
        await alog_event(
            user_id=user_id,
            username=message.from_user.username,
            event="kb_search_performed",
            meta={"question": question, "chunks_found": len(found_chunks)},
        )
        
        # Проверка достаточности данных (используем эффективный вопрос: объединённый при ответе на уточнение)
        sufficient, missing_info = await _check_sufficient_data(query_text, found_chunks)
        
        # Проверяем, нужно ли эскалировать менеджеру
        if await _should_escalate_to_manager(found_chunks, (sufficient, missing_info)):
            await _tag_manager_in_chat(message, query_text)
            return
        
        # Если данных недостаточно, задаем уточняющий вопрос
        if not sufficient and missing_info:
            await _ask_clarification_question(message, query_text, found_chunks, missing_info)
            return
        
        # Генерируем ответ (используем эффективный вопрос)
        answer = await _generate_answer_from_chunks(query_text, found_chunks, conversation_history)
        
        # Отправляем ответ
        await message.answer(answer)
        
        # Добавляем ответ в историю
        conversation_history.append({"role": "assistant", "text": answer})
        _update_user_context(chat_id, user_id, {"conversation_history": conversation_history})
        
        await alog_event(
            user_id=user_id,
            username=message.from_user.username,
            event="kb_answer_generated",
            meta={"question": question, "chunks_used": len(found_chunks)},
        )
        
    except Exception as e:
        logger.exception(f"[GROUP_CHAT_QA] Ошибка обработки вопроса: {e}")
        await message.answer("Извините, произошла ошибка при обработке вопроса.")


@router.message(F.chat.type.in_(["group", "supergroup"]))
async def handle_group_chat_message(message: Message):
    """Обрабатывает сообщения в групповых чатах."""
    # Игнорируем сообщения от бота
    if message.from_user and message.from_user.is_bot:
        return
    
    # Игнорируем команды
    if message.text and message.text.startswith("/"):
        return
    
    # Игнорируем сообщения без текста
    if not message.text or not message.text.strip():
        return

    # Если задан тестовый чат, обрабатываем только его
    rag_test_chat_id = get_rag_test_chat_id()
    if rag_test_chat_id is not None and message.chat.id != rag_test_chat_id:
        return
    
    # Проверяем через AI, является ли сообщение вопросом
    is_question = await _is_question(message.text)
    
    if not is_question:
        return
    
    # Обрабатываем вопрос
    await process_question_in_group_chat(message)


@router.message(F.chat.type.in_(["group", "supergroup"]), F.reply_to_message)
async def handle_manager_reply_in_group_chat(message: Message):
    """Перехватывает ответы менеджеров на вопросы в групповых чатах."""
    # Если указан тестовый чат, обрабатываем только его
    rag_test_chat_id = get_rag_test_chat_id()
    if rag_test_chat_id is not None and message.chat.id != rag_test_chat_id:
        return
    
    # Игнорируем сообщения от бота
    if message.from_user and message.from_user.is_bot:
        return
    
    # Проверяем, является ли отправитель менеджером
    user_id = message.from_user.id if message.from_user else 0
    username = message.from_user.username if message.from_user else None
    
    # Проверяем по username
    is_manager = username and username in MANAGER_USERNAMES
    
    # Проверяем по роли в базе
    if not is_manager:
        user = find_user_by_telegram_id(user_id)
        if user:
            role = getattr(user, "role", "").strip().lower()
            is_manager = role in ("admin", "manager")
    
    if not is_manager:
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
