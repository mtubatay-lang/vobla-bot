from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❓ Задать вопрос", callback_data="qa_start")],
            # если хочешь кнопку FAQ — добавим потом отдельный callback/команду
        ]
    )


def qa_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Завершить навык", callback_data="qa_exit")]
        ]
    )


