import asyncio
import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from app.services.faq_service import find_similar_question
from app.services.metrics_service import alog_event  # async-логгер
from app.services.openai_client import polish_faq_answer
from app.services.pending_questions_service import create_ticket_and_notify_managers
from app.ui.keyboards import qa_kb, main_menu_kb

logger = logging.getLogger(__name__)

router = Router()


class QAMode(StatesGroup):
    active = State()


@router.callback_query(F.data == "qa_start")
async def qa_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(QAMode.active)

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
    await state.set_state(QAMode.active)
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
    await state.set_state(QAMode.active)
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
    await state.clear()

    await cb.message.answer(
        "✅ Навык завершён. Возвращаю в меню.",
        reply_markup=main_menu_kb(),
    )
    await cb.answer()


@router.message(QAMode.active, F.text)
async def qa_handle_question(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    if not q:
        await message.answer("Напиши вопрос текстом 🙂", reply_markup=qa_kb())
        return

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

        # Обрежем историю (последние 8 сообщений)
        history = history[-8:]
        await state.update_data(qa_history=history)

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


@router.callback_query()
async def debug_all_callbacks(cb: CallbackQuery):
    logger.info("[DEBUG CALLBACK] data=%s", cb.data)
    await cb.answer()

