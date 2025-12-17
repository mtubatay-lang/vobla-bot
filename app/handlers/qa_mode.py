from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from app.services.faq_service import find_similar_question
from app.services.metrics_service import alog_event  # async-логгер
from app.services.pending_questions_service import create_ticket_and_notify_managers
from app.ui.keyboards import qa_kb, main_menu_kb

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
        # ✅ автоответ из FAQ
        await message.answer(
            f"🤖 <b>Ответ из базы знаний</b>\n\n{best['answer']}\n\n"
            "Если есть ещё вопрос — просто напиши его 👇",
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

