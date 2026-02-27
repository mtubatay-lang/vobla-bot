from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.core.types import KeyboardButton, KeyboardRow
from app.platforms.telegram import rows_to_inline_markup


def main_menu_rows(user_id: int = None) -> list[KeyboardRow]:
    """Главное меню — платформо-независимые ряды кнопок."""
    return [
        [KeyboardButton(text="❓ Задать вопрос", callback_data="qa_start")],
    ]


def main_menu_kb(user_id: int = None) -> InlineKeyboardMarkup:
    """Главное меню (Telegram)."""
    return rows_to_inline_markup(main_menu_rows(user_id=user_id))


def qa_kb_rows() -> list[KeyboardRow]:
    """Кнопка «Завершить навык» — платформо-независимые ряды."""
    return [
        [KeyboardButton(text="✅ Завершить навык", callback_data="qa_exit")],
    ]


def qa_kb() -> InlineKeyboardMarkup:
    """Клавиатура режима навыка (Telegram)."""
    return rows_to_inline_markup(qa_kb_rows())
