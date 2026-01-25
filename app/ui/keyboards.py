from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from app.services.auth_service import find_user_by_telegram_id
from app.handlers.broadcast import _check_admin


def main_menu_kb(user_id: int = None) -> InlineKeyboardMarkup:
    """Главное меню. Если user_id указан и пользователь админ, добавляет админ-кнопки."""
    buttons = [
        [InlineKeyboardButton(text="❓ Задать вопрос", callback_data="qa_start")],
    ]
    
    # Добавляем админ-кнопки, если пользователь админ
    if user_id:
        user = find_user_by_telegram_id(user_id)
        if user and _check_admin(user):
            buttons.append(
                [InlineKeyboardButton(text="📢 Запуск рассылки", callback_data="broadcast_start")]
            )
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def qa_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Завершить навык", callback_data="qa_exit")]
        ]
    )


