"""Точка входа Telegram-бота Vobla Bot."""

import asyncio
import logging

import sentry_sdk
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand

from app.config import BOT_TOKEN, LOG_LEVEL, SENTRY_DSN
from app.handlers.debug_passthrough import router as debug_router
from app.handlers.start import router as start_router
from app.handlers.help import router as help_router
from app.handlers.auth_handler import auth_router
from app.handlers.echo import router as echo_router
from app.handlers.faq import router as faq_router
from app.handlers.manager_reply import router as manager_router
from app.handlers.qa_mode import router as qa_router
from app.handlers.knowledge_base_admin import router as kb_admin_router


async def main() -> None:
    """Основная функция запуска бота."""

    # --- Инициализация Sentry (если указан DSN) ---
    if SENTRY_DSN:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            traces_sample_rate=0.0,  # только ошибки, без трейсинга
        )

    # --- Настройка логирования ---
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN не задан. Проверь .env или переменные окружения на сервере."
        )

    # --- Создаём бота и диспетчер ---
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # --- Команды бота в меню Telegram ---
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Начать"),
            BotCommand(command="help", description="Помощь"),
            BotCommand(command="login", description="Авторизация"),
            BotCommand(command="ask", description="Задать вопрос"),
        ]
    )

    # --- Регистрируем роутеры ---
    dp.include_router(debug_router)  # сквозной дебаг (первым!)
    dp.include_router(start_router)
    dp.include_router(help_router)  # роутер помощи
    dp.include_router(auth_router)  # роутер авторизации
    dp.include_router(manager_router)  # роутер для менеджеров
    dp.include_router(qa_router)  # роутер режима навыка
    dp.include_router(faq_router)   # FAQ-роутер
    dp.include_router(kb_admin_router)  # админ-панель для базы знаний
    dp.include_router(echo_router)

    logger.info("Запускаем бота...")

    # На всякий случай удаляем вебхук и сбрасываем старые апдейты
    await bot.delete_webhook(drop_pending_updates=True)

    # Запускаем polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())