"""
Платформо-независимые хендлеры: вызываются из Telegram-роутеров и из MAX entrypoint.
Принимают адаптер и внутренние типы (IncomingMessage / CallbackEvent).
"""

from __future__ import annotations

import asyncio
from typing import Dict, Any

from app.core.types import (
    CallbackEvent,
    IncomingMessage,
    OutgoingMessage,
    KeyboardButton,
    KeyboardRow,
    Platform,
)
from app.services.auth_service import (
    find_user_by_platform_id,
    find_user_by_code,
    bind_platform_id,
)
from app.services.metrics_service import log_event
from app.ui.keyboards import main_menu_rows

# Глобальное состояние ожидания кода (ключ = (platform, user_id))
_pending_auth: Dict[tuple[str, int], bool] = {}


def _commands_menu_text() -> str:
    """Текст с меню команд после авторизации."""
    return (
        "📋 <b>Доступные команды:</b>\n"
        "• /help — подсказка по всем командам бота\n"
        "• /ask — задать вопрос (режим навыка)\n\n"
        "Или просто нажмите кнопку «❓ Задать вопрос» ниже 👇"
    )


def auth_keyboard_rows() -> list[KeyboardRow]:
    """Клавиатура с кнопкой авторизации."""
    return [[KeyboardButton(text="🔐 Авторизация", callback_data="start_auth")]]


async def handle_start(adapter: Any, incoming: IncomingMessage) -> None:
    """Обработка /start: приветствие и кнопка авторизации или меню."""
    platform = adapter.platform
    user_id = incoming.user.id
    chat_id = incoming.chat.id

    user = find_user_by_platform_id(platform, user_id)
    if user:
        log_event(
            user_id=user_id,
            username=incoming.user.username,
            event="start_authorized",
            meta={"role": getattr(user, "role", ""), "platform": platform},
        )
        msg = OutgoingMessage(
            chat_id=chat_id,
            platform=platform,
            text=f"👋 Привет, {user.name}!\n"
            f"Вы авторизованы как <b>{user.role}</b>.\n\n"
            + _commands_menu_text(),
            keyboard_rows=main_menu_rows(),
            is_group_chat=incoming.chat.is_group,
        )
        await adapter.send_message(msg)
        return

    log_event(
        user_id=user_id,
        username=incoming.user.username,
        event="start_unauthorized",
        meta={"platform": platform},
    )
    text = (
        "🔒 Этот бот доступен только для партнёров Воблабир.\n\n"
        "Если у вас есть код доступа, нажмите кнопку ниже, "
        "чтобы пройти авторизацию."
    )
    msg = OutgoingMessage(
        chat_id=chat_id,
        platform=platform,
        text=text,
        keyboard_rows=auth_keyboard_rows(),
        is_group_chat=incoming.chat.is_group,
    )
    await adapter.send_message(msg)


async def handle_start_auth_callback(adapter: Any, event: CallbackEvent) -> None:
    """Обработка нажатия кнопки «Авторизация»: просим ввести код."""
    platform = adapter.platform
    user_id = event.user.id
    chat_id = event.chat.id
    key = (platform, int(user_id) if isinstance(user_id, int) else user_id)
    _pending_auth[key] = True

    log_event(
        user_id=user_id,
        username=event.user.username,
        event="auth_button_click",
        meta={"platform": platform},
    )

    msg = OutgoingMessage(
        chat_id=chat_id,
        platform=platform,
        text=(
            "🔐 Для входа в систему введите код доступа, выданный менеджером.\n"
            "Код можно использовать только один раз."
        ),
        is_group_chat=event.chat.is_group,
    )
    await adapter.send_message(msg)
    if event.callback_id is not None:
        await adapter.answer_callback(event.callback_id)


def is_pending_auth(platform: Platform, user_id: int | str) -> bool:
    """Проверка, ожидаем ли мы от пользователя ввод кода."""
    uid = int(user_id) if isinstance(user_id, str) else user_id
    return _pending_auth.get((platform, uid), False)


async def handle_auth_code(
    adapter: Any,
    incoming: IncomingMessage,
) -> None:
    """Обработка ввода кода доступа (после нажатия «Авторизация»)."""
    platform = adapter.platform
    user_id = incoming.user.id
    chat_id = incoming.chat.id
    text = (incoming.text or "").strip()
    key = (platform, int(user_id) if isinstance(user_id, int) else user_id)

    if not _pending_auth.get(key):
        return

    await adapter.send_typing(chat_id, is_group_chat=incoming.chat.is_group)
    await asyncio.sleep(1.2)

    log_event(
        user_id=user_id,
        username=incoming.user.username,
        event="auth_code_submitted",
        meta={"platform": platform},
    )

    user = find_user_by_code(text)
    if not user:
        log_event(
            user_id=user_id,
            username=incoming.user.username,
            event="auth_failed_code_not_found",
            meta={"platform": platform},
        )
        await adapter.send_message(OutgoingMessage(
            chat_id=chat_id,
            platform=platform,
            text="❌ Код не найден. Проверьте правильность и попробуйте снова.",
            is_group_chat=incoming.chat.is_group,
        ))
        return

    if not user.is_active:
        log_event(
            user_id=user_id,
            username=incoming.user.username,
            event="auth_failed_inactive",
            meta={"platform": platform},
        )
        await adapter.send_message(OutgoingMessage(
            chat_id=chat_id,
            platform=platform,
            text="⛔ Ваш код не активирован. Обратитесь к менеджеру.",
            is_group_chat=incoming.chat.is_group,
        ))
        return

    bind_platform_id(user, platform, user_id)
    log_event(
        user_id=user_id,
        username=incoming.user.username,
        event="auth_success",
        meta={"role": getattr(user, "role", ""), "name": getattr(user, "name", ""), "platform": platform},
    )
    _pending_auth.pop(key, None)

    await adapter.send_message(OutgoingMessage(
        chat_id=chat_id,
        platform=platform,
        text=f"✅ Добро пожаловать, {user.name}!\n"
        f"Вы авторизованы как <b>{user.role}</b>.\n\n"
        + _commands_menu_text(),
        keyboard_rows=main_menu_rows(),
        is_group_chat=incoming.chat.is_group,
    ))


def _help_text_authorized() -> str:
    return (
        "📌 <b>Как пользоваться ботом</b>\n\n"
        "❓ <b>Задать вопрос</b>\n"
        "• Нажми кнопку «❓ Задать вопрос» в меню\n"
        "• или введи команду /ask\n\n"
        "🧠 <b>Навык «Ответы на вопросы»</b>\n"
        "• Можно задавать вопросы подряд\n"
        "• Для выхода нажми «✅ Завершить навык»\n\n"
        "🔐 <b>Авторизация</b>\n"
        "• /login — если нужно войти заново\n"
    )


def _help_text_unauthorized() -> str:
    return (
        "🔒 Этот бот доступен только для партнёров Воблабир.\n\n"
        "Чтобы начать:\n"
        "1) Нажми /start\n"
        "2) Пройди авторизацию по коду доступа\n\n"
        "Если кода нет — обратись к менеджеру."
    )


async def handle_help(adapter: Any, incoming: IncomingMessage) -> None:
    """Обработка /help."""
    platform = adapter.platform
    user_id = incoming.user.id
    chat_id = incoming.chat.id

    user = find_user_by_platform_id(platform, user_id)
    if user:
        msg = OutgoingMessage(
            chat_id=chat_id,
            platform=platform,
            text=_help_text_authorized(),
            keyboard_rows=main_menu_rows(),
            is_group_chat=incoming.chat.is_group,
        )
        await adapter.send_message(msg)
    else:
        await adapter.send_message(OutgoingMessage(
            chat_id=chat_id,
            platform=platform,
            text=_help_text_unauthorized(),
            is_group_chat=incoming.chat.is_group,
        ))
