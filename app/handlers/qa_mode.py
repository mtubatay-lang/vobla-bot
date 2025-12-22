import asyncio
import logging
import uuid

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from app.services.faq_service import find_similar_question
from app.services.metrics_service import alog_event  # async-логгер
from app.services.openai_client import polish_faq_answer
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


@router.callback_query(F.data == "qa_start")
async def qa_start(cb: CallbackQuery, state: FSMContext):
    session_id = uuid.uuid4().hex[:12]
    await state.set_state(QAMode.active)
    await state.update_data(
        qa_history=[],
        qa_session_id=session_id,
        qa_questions_count=0,
        qa_last_question="",
        qa_last_answer_source="",
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
    session_id = uuid.uuid4().hex[:12]
    await state.set_state(QAMode.active)
    await state.update_data(
        qa_history=[],
        qa_session_id=session_id,
        qa_questions_count=0,
        qa_last_question="",
        qa_last_answer_source="",
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
    session_id = uuid.uuid4().hex[:12]
    await state.set_state(QAMode.active)
    await state.update_data(
        qa_history=[],
        qa_session_id=session_id,
        qa_questions_count=0,
        qa_last_question="",
        qa_last_answer_source="",
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
    q = (message.text or "").strip()
    if not q:
        await message.answer("Напиши вопрос текстом 🙂", reply_markup=qa_kb())
        return

    # Увеличиваем счётчик вопросов и сохраняем последний вопрос
    data = await state.get_data()
    cnt = int(data.get("qa_questions_count", 0)) + 1
    await state.update_data(
        qa_questions_count=cnt,
        qa_last_question=q,
    )

    # 1) Пытаемся найти ответ в FAQ
    best = await find_similar_question(q)

    if best:
        # Достаём историю из FSM state
        data = await state.get_data()
        history = data.get("qa_history", [])

        raw_answer = best["answer"]

        # Обновим историю: пользовательский вопрос
        history.append({"role": "user", "text": q})

        # Полировка в отдельном потоке, чтобы не блокировать loop
        try:
            pretty = await asyncio.to_thread(polish_faq_answer, q, raw_answer, history)
        except Exception:
            pretty = raw_answer

        # Обновим историю: ответ бота (уже красивый)
        history.append({"role": "assistant", "text": pretty})

        # Обрежем историю до последних 8 сообщений и сохраним
        await state.update_data(
            qa_history=history[-8:],
            qa_last_answer_source="faq",
        )

        # ✅ автоответ из FAQ (полированный)
        await message.answer(
            pretty + "\n\nЕсли есть ещё вопрос — просто напиши его 👇",
            reply_markup=qa_kb(),
            parse_mode="HTML",
        )

        await alog_event(
            user_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            event="faq_answer_shown",
            meta={"score": best.get("score"), "matched_q": best.get("question")},
        )
        return

    # 2) Если не нашли — эскалируем менеджеру
    await state.update_data(qa_last_answer_source="manager")
    
    await message.answer(
        "Не нашёл точного ответа в базе 😕\n"
        "Я передал вопрос менеджеру. Можешь задать следующий вопрос — просто напиши его 👇",
        reply_markup=qa_kb(),
    )

    await alog_event(
        user_id=message.from_user.id if message.from_user else None,
        username=message.from_user.username if message.from_user else None,
        event="faq_not_helpful_escalated",
        meta={"question": q},
    )

    await create_ticket_and_notify_managers(message, q)


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

