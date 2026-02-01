"""Точка входа Telegram-бота Vobla Bot."""

import asyncio
import logging

import sentry_sdk
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, Update

from app.config import BOT_TOKEN, LOG_LEVEL, SENTRY_DSN
from app.handlers.debug_passthrough import router as debug_router
from app.handlers.start import router as start_router
from app.handlers.help import router as help_router
from app.handlers.auth_handler import auth_router
from app.handlers.echo import router as echo_router
from app.handlers.faq import router as faq_router
from app.handlers.manager_reply import router as manager_router
from app.handlers.qa_mode import router as qa_router
from app.handlers.group_chat_qa import router as group_chat_qa_router
from app.handlers.knowledge_base_admin import router as kb_admin_router
from app.handlers.broadcast import router as broadcast_router


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

    # --- Создаём бота и диспетчер ---
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # --- Middleware для логирования команд ---
    class CommandLoggingMiddleware(BaseMiddleware):
        async def __call__(self, handler, event, data):
            # В aiogram 3.x event уже является Message для message handlers
            if hasattr(event, 'text') and event.text and event.text.startswith('/'):
                logger.info(f"[COMMAND] Получена команда: {event.text} от пользователя {event.from_user.id if event.from_user else 'unknown'}")
            return await handler(event, data)

    dp.message.middleware(CommandLoggingMiddleware())

    # --- Команды бота в меню Telegram ---
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Начать"),
            BotCommand(command="help", description="Помощь"),
            BotCommand(command="login", description="Авторизация"),
            BotCommand(command="ask", description="Задать вопрос"),
            BotCommand(command="kb_add", description="Пополнение базы знаний (админ)"),
            BotCommand(command="broadcast", description="Запуск рассылки (админ)"),
        ]
    )

    # --- Регистрируем роутеры ---
    dp.include_router(debug_router)  # сквозной дебаг (первым!)
    dp.include_router(start_router)
    dp.include_router(help_router)  # роутер помощи
    dp.include_router(auth_router)  # роутер авторизации
    dp.include_router(kb_admin_router)  # админ-панель для базы знаний (перемещен выше)
    dp.include_router(manager_router)  # роутер для менеджеров
    dp.include_router(broadcast_router)  # роутер рассылок
    dp.include_router(qa_router)  # роутер режима навыка
    dp.include_router(group_chat_qa_router)  # групповой чат RAG
    dp.include_router(faq_router)   # FAQ-роутер
    dp.include_router(echo_router)
    
    # Проверка зарегистрированных роутеров
    logger.info(f"[MAIN] Зарегистрировано роутеров: {len(dp.sub_routers)}")
    for idx, router in enumerate(dp.sub_routers):
        router_name = getattr(router, 'name', f'router_{idx}')
        logger.info(f"[MAIN] Роутер {idx+1}: {router_name}")

    logger.info("Запускаем бота...")

    # На всякий случай удаляем вебхук и сбрасываем старые апдейты
    await bot.delete_webhook(drop_pending_updates=True)

    # Запускаем polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())