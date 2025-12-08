"""Точка входа Telegram-бота Vobla Bot."""

import asyncio
import logging

import sentry_sdk
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from app.config import BOT_TOKEN, LOG_LEVEL, SENTRY_DSN
from app.handlers.start import router as start_router
from app.handlers.echo import router as echo_router


async def main() -> None:
    """Основная функция запуска бота."""

    # Инициализация Sentry (если указан DSN)
    if SENTRY_DSN:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            traces_sample_rate=0.0,  # только ошибки, без трейсинга
        )

    # Настройка логирования
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN не задан. Проверь .env или переменные окружения на сервере."
        )

    # Создаём бота и диспетчер
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # Регистрируем роутеры
    dp.include_router(start_router)
    dp.include_router(echo_router)

    logger.info("Запускаем бота...")

    # На всякий случай удаляем вебхук и сбрасываем старые апдейты
    await bot.delete_webhook(drop_pending_updates=True)

    # Запускаем polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())