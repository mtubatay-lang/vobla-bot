from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.core.callbacks import (
    MAX_MENU_ADMIN,
    MAX_MENU_HELP,
    MAX_MENU_KILBIL,
    MAX_MENU_LOGIN,
    QA_START,
    QA_EXIT,
    START_AUTH,
)
from app.core.types import KeyboardButton, KeyboardRow
from app.platforms.telegram import rows_to_inline_markup


def main_menu_rows(user_id: int = None) -> list[KeyboardRow]:
    """Главное меню — платформо-независимые ряды кнопок."""
    return [
        [KeyboardButton(text="❓ Задать вопрос", callback_data=QA_START)],
    ]


def main_menu_kb(user_id: int = None) -> InlineKeyboardMarkup:
    """Главное меню (Telegram)."""
    return rows_to_inline_markup(main_menu_rows(user_id=user_id))


def qa_kb_rows() -> list[KeyboardRow]:
    """Кнопка «Завершить навык» — платформо-независимые ряды."""
    return [
        [KeyboardButton(text="✅ Завершить навык", callback_data=QA_EXIT)],
    ]


def qa_kb() -> InlineKeyboardMarkup:
    """Клавиатура режима навыка (Telegram)."""
    return rows_to_inline_markup(qa_kb_rows())


def max_main_menu_rows(*, is_authorized: bool, is_admin: bool) -> list[KeyboardRow]:
    """Гибридное меню для MAX: основные действия + команды."""
    rows: list[KeyboardRow] = []
    if not is_authorized:
        rows.append([KeyboardButton(text="🔐 Авторизация", callback_data=START_AUTH)])
        rows.append([KeyboardButton(text="📘 Помощь", callback_data=MAX_MENU_HELP)])
        return rows

    rows.append([KeyboardButton(text="❓ Задать вопрос", callback_data=QA_START)])
    rows.append([KeyboardButton(text="📘 Помощь", callback_data=MAX_MENU_HELP)])
    rows.append([KeyboardButton(text="🧾 Kilbil", callback_data=MAX_MENU_KILBIL)])
    if is_admin:
        rows.append([KeyboardButton(text="🛠 Админ-раздел", callback_data=MAX_MENU_ADMIN)])
    rows.append([KeyboardButton(text="🔐 Авторизация", callback_data=MAX_MENU_LOGIN)])
    return rows
