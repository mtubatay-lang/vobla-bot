import asyncio
import json
import logging
import uuid
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
from app.services.qdrant_service import get_qdrant_service
from app.services.pending_questions_service import create_ticket_and_notify_managers
from app.services.qa_feedback_service import save_qa_feedback
from app.ui.keyboards import qa_kb, main_menu_kb

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
) -> tuple[bool, Optional[str]]:
    """Проверяет через AI, достаточно ли данных для ответа (для приватных чатов)."""
    if not found_chunks:
        return (False, "Не найдено релевантных фрагментов в базе знаний")
    
    # Проверяем максимальный score
    max_score = max((chunk.get("score", 0) for chunk in found_chunks), default=0)
    
    # Если score очень высокий, считаем данные достаточными
    if max_score >= 0.75:
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
        
        prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"{'Контекст диалога:\n' + context_text + '\n\n' if context_text else ''}"
            f"Найденные фрагменты из базы знаний:\n{chunks_text}\n\n"
            "Оцени, достаточно ли этих фрагментов для ответа на вопрос пользователя.\n"
            "Учитывай контекст диалога - если пользователь уточняет предыдущий вопрос, используй этот контекст.\n"
            "Если фрагменты релевантны вопросу, даже если не полностью покрывают все аспекты, можно считать данные достаточными.\n"
            "Ответь 'yes' или 'no'.\n"
            "Если 'no', укажи кратко, какая информация отсутствует."
        )
        
        resp = client.chat.completions.create(
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
) -> None:
    """Задает уточняющий вопрос пользователю (для приватных чатов)."""
    try:
        chunks_summary = "\n".join([
            f"- {chunk.get('text', '')[:200]}..."
            for chunk in found_chunks[:2]
        ])
        
        prompt = (
            f"Пользователь спросил: {question}\n\n"
            f"Найденные фрагменты:\n{chunks_summary}\n\n"
            f"Недостающая информация: {missing_info}\n\n"
            "Сформулируй один развернутый и понятный уточняющий вопрос, который поможет найти нужный ответ.\n"
            "Вопрос должен быть максимально конкретным и понятным, как будто ты менеджер, который хочет помочь клиенту.\n"
            "Не используй технические термины, говори простым языком.\n"
            "Вопрос должен быть полным предложением, не используй сокращения."
        )
        
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты дружелюбный менеджер, который помогает клиентам, задавая понятные уточняющие вопросы."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        
        clarification_text = resp.choices[0].message.content or "Можете уточнить ваш вопрос?"
        
        # Добавляем вводную фразу
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
        
        await alog_event(
            user_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            event="kb_clarification_asked_private",
            meta={"original_question": question, "missing_info": missing_info},
        )
    except Exception as e:
        logger.exception(f"[QA_MODE] Ошибка формулировки уточняющего вопроса: {e}")
        await message.answer("Чтобы ответить на ваш вопрос, мне нужны некоторые уточнения.\n\nМожете уточнить ваш вопрос?", reply_markup=qa_kb())


async def _generate_answer_from_chunks_private(
    question: str,
    chunks: List[Dict[str, Any]],
    conversation_history: List[Dict[str, str]],
) -> str:
    """Генерирует ответ на основе найденных чанков (для приватных чатов)."""
    try:
        history_text = ""
        if conversation_history:
            history_lines = []
            for msg in conversation_history[-5:]:
                role = "Пользователь" if msg.get("role") == "user" else "Бот"
                text = msg.get("text", "")
                if text:
                    history_lines.append(f"{role}: {text}")
            history_text = "\n".join(history_lines)
        
        chunks_text = "\n\n---\n\n".join([
            f"Фрагмент {i+1}:\n{chunk.get('text', '')}"
            for i, chunk in enumerate(chunks)
        ])
        
        system_prompt = (
            "Ты помощник корпоративного бота сети магазинов Воблабир.\n"
            "Твоя задача — ответить на вопрос пользователя на основе предоставленных фрагментов базы знаний.\n\n"
            "ВАЖНО: Отвечай ТОЛЬКО на текущий вопрос пользователя. Не смешивай разные темы.\n"
            "Если фрагменты не относятся к текущему вопросу, честно скажи об этом.\n\n"
            "Правила:\n"
            "1. Используй ТОЛЬКО информацию из предоставленных фрагментов.\n"
            "2. НЕ придумывай факты, которых нет в фрагментах.\n"
            "3. Если информация в фрагментах не относится к текущему вопросу, скажи об этом честно.\n"
            "4. Структурируй ответ: абзацы, списки, если уместно.\n"
            "5. Будь дружелюбным и понятным.\n"
            "6. Учитывай контекст предыдущих сообщений, но отвечай на текущий вопрос."
        )
        
        user_prompt = (
            f"Текущий вопрос пользователя: {question}\n\n"
            f"{'Контекст диалога:\n' + history_text + '\n\n' if history_text else ''}"
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
    
    if not sufficient:
        if missing_info and any(word in missing_info.lower() for word in ["конкретн", "детал", "уточн"]):
            logger.info(f"[QA_MODE] Не эскалируем: данных недостаточно, но можно уточнить (max_score={max_score:.3f})")
            return False
        # Если данных недостаточно, но score хороший - все равно пытаемся ответить
        if max_score >= 0.6:
            logger.info(f"[QA_MODE] Не эскалируем: данных недостаточно, но score хороший ({max_score:.3f})")
            return False
        logger.info(f"[QA_MODE] Эскалация: данных недостаточно, score низкий ({max_score:.3f})")
        return True
    
    if max_score < 0.5:
        logger.info(f"[QA_MODE] Эскалация: max_score слишком низкий ({max_score:.3f})")
        return True
    
    logger.info(f"[QA_MODE] Не эскалируем: данных достаточно, score хороший ({max_score:.3f})")
    return False


@router.callback_query(F.data == "qa_start")
async def qa_start(cb: CallbackQuery, state: FSMContext):
    if not await _require_auth(cb):
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

    await cb.message.answer(
        "🧠 <b>Навык: Ответы на вопросы</b>\n\n"
        "Напиши вопрос — я попробую ответить по базе знаний.\n"
        "Можно задавать вопросы подряд.\n\n"
        "Чтобы выйти — нажми «Завершить навык».",
        reply_markup=qa_kb(),
        parse_mode="HTML",
    )

    await cb.answer()


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

    # Увеличиваем счётчик вопросов
    data = await state.get_data()
    cnt = int(data.get("qa_questions_count", 0)) + 1
    history = data.get("qa_history", [])
    original_question = data.get("qa_original_question", "")
    awaiting_clarification = data.get("qa_awaiting_clarification", False)
    
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
    
    is_new_question = False
    if last_assistant_msg and not is_first_question:
        # Если последний ответ бота НЕ был уточняющим (не содержит ключевой фразы),
        # значит текущее сообщение - это новый вопрос
        if "уточнения" not in last_assistant_msg.lower():
            is_new_question = True
            logger.info("[QA_MODE] Определен новый вопрос (последний ответ был полным)")
    
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
    
    # Добавляем вопрос в историю (сохраняем оригинальный текст пользователя, не объединенный)
    history.append({"role": "user", "text": message.text.strip()})
    
    await state.update_data(
        qa_questions_count=cnt,
        qa_last_question=q,
        qa_original_question=original_question,
        qa_awaiting_clarification=False,  # Сбрасываем флаг после обработки
        qa_history=history[-8:],  # Ограничиваем историю
    )

    # Показываем индикатор обработки
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        # ШАГ 1: Поиск в Qdrant RAG
        # Если это ответ на уточнение, q уже содержит объединенный вопрос
        # Иначе используем контекст из истории
        if is_clarification_response:
            query_text = q  # q уже содержит объединенный вопрос
            logger.info(f"[QA_MODE] Используем объединенный вопрос для поиска: '{query_text[:100]}...'")
        else:
            context_text = "\n".join([msg.get("text", "") for msg in history[-3:]])
            query_text = f"{context_text}\n{q}" if context_text else q
        
        embedding = await asyncio.to_thread(create_embedding, query_text)
        
        qdrant_service = get_qdrant_service()
        found_chunks = qdrant_service.search(
            query_embedding=embedding,
            top_k=5,
            score_threshold=0.5,  # Понижен с 0.7 для более гибкого поиска
        )
        
        # Детальное логирование для диагностики
        logger.info(
            f"[QA_MODE] Поиск в RAG: вопрос='{q[:50]}...', "
            f"найдено чанков={len(found_chunks)}"
        )
        if found_chunks:
            scores = [chunk.get("score", 0) for chunk in found_chunks]
            logger.info(f"[QA_MODE] Scores найденных чанков: {[f'{s:.3f}' for s in scores]}")
        
        await alog_event(
            user_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            event="kb_search_performed_private",
            meta={"question": q, "chunks_found": len(found_chunks)},
        )
        
        # Если нашли чанки в Qdrant
        if found_chunks:
            # Проверка достаточности данных (передаем историю для контекста)
            sufficient, missing_info = await _check_sufficient_data_private(q, found_chunks, history)
            logger.info(
                f"[QA_MODE] Проверка достаточности данных: sufficient={sufficient}, "
                f"missing_info={missing_info[:50] if missing_info else None}"
            )
            
            # Проверяем, нужно ли эскалировать
            should_escalate = await _should_escalate_to_manager_private(found_chunks, (sufficient, missing_info))
            logger.info(f"[QA_MODE] Решение об эскалации: should_escalate={should_escalate}")
            
            if not should_escalate:
                # Если данных недостаточно, задаем уточняющий вопрос
                if not sufficient and missing_info:
                    logger.info("[QA_MODE] Задаем уточняющий вопрос пользователю")
                    await _ask_clarification_question_private(message, q, found_chunks, missing_info, state)
                    return
                
                # Генерируем ответ из Qdrant
                logger.info("[QA_MODE] Генерируем ответ из найденных чанков RAG")
                answer = await _generate_answer_from_chunks_private(q, found_chunks, history)
                
                # Обновляем историю
                history.append({"role": "assistant", "text": answer})
                await state.update_data(
                    qa_history=history[-8:],
                    qa_last_answer_source="qdrant_rag",
                )
                
                await message.answer(
                    answer + "\n\nЕсли есть ещё вопрос — просто напиши его 👇",
                    reply_markup=qa_kb(),
                    parse_mode="HTML",
                )
                
                await alog_event(
                    user_id=message.from_user.id if message.from_user else None,
                    username=message.from_user.username if message.from_user else None,
                    event="kb_answer_generated_private",
                    meta={"question": q, "chunks_used": len(found_chunks)},
                )
                return
        
        # ШАГ 2: Если не нашли в Qdrant или нужно эскалировать - ищем в FAQ
        if not found_chunks:
            logger.info("[QA_MODE] Чанки не найдены в RAG, переходим к поиску в FAQ")
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
            )
            
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
        await state.update_data(qa_last_answer_source="manager")
        
        await message.answer(
            "Не нашёл ответа в базе знаний 😕\n"
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

