from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def main_menu_kb(user_id: int = None) -> InlineKeyboardMarkup:
    """Главное меню."""
    buttons = [
        [InlineKeyboardButton(text="❓ Задать вопрос", callback_data="qa_start")],
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def qa_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Завершить навык", callback_data="qa_exit")]
        ]
    )


