"""Точка входа Telegram-бота Vobla Bot."""

import asyncio
import importlib
import logging
import os
from contextlib import suppress

import sentry_sdk
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, Update

from app.config import APP_REVISION, BOT_TOKEN, LOG_LEVEL, REDIS_URL, SENTRY_DSN, TELEGRAM_POLLING_ENABLED
from app.handlers.debug_passthrough import router as debug_router
from app.handlers.start import router as start_router
from app.handlers.help import router as help_router
from app.handlers.auth_handler import auth_router
from app.handlers.echo import router as echo_router
from app.handlers.faq import router as faq_router
from app.handlers.kilbil import router as kilbil_router
from app.handlers.manager_reply import router as manager_router
from app.handlers.qa_mode import router as qa_router
from app.handlers.group_chat_qa import router as group_chat_qa_router
from app.handlers.knowledge_base_admin import router as kb_admin_router
from app.handlers.broadcast import router as broadcast_router
from app.handlers.document_generator import router as document_generator_router
from app.handlers.recipients_collector import router as recipients_collector_router
from app.handlers.voice_to_text import router as voice_to_text_router


async def _railway_health_runner(logger: logging.Logger):
    """
    Railway healthcheck (railway.toml healthcheckPath=/health) бьёт в $PORT.
    Long polling сам по себе HTTP-сервер на PORT не поднимает — без этого шага деплой падает.
    """
    port_str = os.getenv("PORT")
    if not port_str:
        return None
    try:
        from aiohttp import web
    except ImportError:
        logger.warning("[MAIN] aiohttp недоступен — пропускаем /health на PORT")
        return None

    app = web.Application()

    async def health(_request):
        return web.json_response({"status": "ok", "role": "telegram"})

    app.router.add_get("/health", health)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(port_str)).start()
    logger.info("[MAIN] Railway: GET /health на 0.0.0.0:%s", port_str)
    return runner


async def _acquire_polling_guard(redis_url: str, bot_token: str):
    """
    Best-effort distributed guard: предотвращает одновременный polling одного токена.
    Работает только если доступен redis.asyncio и REDIS_URL задан.
    """
    try:
        redis = importlib.import_module("redis.asyncio")
    except Exception:
        return None

    key = f"vobla:telegram_polling_lock:{bot_token[:12]}"
    lock_value = f"pid:{id(asyncio.current_task())}"
    client = redis.from_url(redis_url, decode_responses=True)
    ttl_seconds = 90

    acquired = await client.set(key, lock_value, ex=ttl_seconds, nx=True)
    if not acquired:
        await client.close()
        return False

    async def _heartbeat() -> None:
        while True:
            await asyncio.sleep(25)
            try:
                current = await client.get(key)
                if current != lock_value:
                    break
                await client.expire(key, ttl_seconds)
            except Exception:
                break

    task = asyncio.create_task(_heartbeat())
    return (client, key, lock_value, task)


async def _release_polling_guard(lock_tuple) -> None:
    if not lock_tuple:
        return
    client, key, lock_value, heartbeat_task = lock_tuple
    heartbeat_task.cancel()
    with suppress(asyncio.CancelledError):
        await heartbeat_task
    try:
        current = await client.get(key)
        if current == lock_value:
            await client.delete(key)
    finally:
        await client.close()


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
    _root_lvl = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    if isinstance(_root_lvl, int) and _root_lvl >= logging.INFO:
        # Иначе каждый long poll пишет URL с токеном в INFO (утечка в Railway-логах).
        for _name in ("httpx", "httpcore"):
            logging.getLogger(_name).setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    if APP_REVISION:
        logger.info("[MAIN] APP_REVISION=%s (сверьте с main при отладке деплоя)", APP_REVISION)

    # --- Создаём бота и диспетчер ---
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    if REDIS_URL:
        from aiogram.fsm.storage.redis import RedisStorage

        storage = RedisStorage.from_url(REDIS_URL)
        logger.info("[MAIN] FSM storage: Redis (REDIS_URL задан — состояние общее для всех воркеров)")
    else:
        storage = MemoryStorage()
        logger.warning(
            "[MAIN] FSM storage: MemoryStorage. Несколько процессов с getUpdates по одному BOT_TOKEN "
            "(TelegramConflictError) или несколько реплик без Redis — FSM рассылки и других сценариев "
            "может рассинхронизироваться. Задайте REDIS_URL или один инстанс polling."
        )
    dp = Dispatcher(storage=storage)

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
            BotCommand(command="kilbil", description="Вопросы по kilbil"),
            BotCommand(command="admin", description="Раздел администраторов"),
        ]
    )

    # --- Регистрируем роутеры ---
    dp.include_router(debug_router)  # сквозной дебаг (первым!)
    dp.include_router(recipients_collector_router)  # сбор получателей (чаты/юзеры) для рассылки
    dp.include_router(start_router)
    dp.include_router(help_router)  # роутер помощи
    dp.include_router(voice_to_text_router)  # голос → текст (до echo и qa)
    dp.include_router(auth_router)  # роутер авторизации
    dp.include_router(kb_admin_router)  # админ-панель для базы знаний (перемещен выше)
    dp.include_router(manager_router)  # роутер для менеджеров
    dp.include_router(broadcast_router)  # роутер рассылок
    dp.include_router(document_generator_router)  # создание документов из шаблонов
    dp.include_router(qa_router)  # роутер режима навыка
    dp.include_router(group_chat_qa_router)  # групповой чат RAG
    dp.include_router(kilbil_router)  # kilbil RAG (перед faq)
    dp.include_router(faq_router)   # FAQ-роутер
    dp.include_router(echo_router)
    
    # Проверка зарегистрированных роутеров
    logger.info(f"[MAIN] Зарегистрировано роутеров: {len(dp.sub_routers)}")
    for idx, router in enumerate(dp.sub_routers):
        router_name = getattr(router, 'name', f'router_{idx}')
        logger.info(f"[MAIN] Роутер {idx+1}: {router_name}")

    if not TELEGRAM_POLLING_ENABLED:
        logger.warning(
            "[MAIN] TELEGRAM_POLLING_ENABLED=false — long polling не запускается в этом процессе. "
            "Должен работать ровно один другой процесс/реплика с polling на этот BOT_TOKEN; "
            "при двух поллерах — TelegramConflictError. Для FSM при нескольких воркерах нужен REDIS_URL."
        )
        stop = asyncio.Event()
        await stop.wait()
        return

    logger.info("Запускаем бота...")
    health_runner = None
    polling_guard = None
    try:
        health_runner = await _railway_health_runner(logger)
        if REDIS_URL:
            guard = await _acquire_polling_guard(REDIS_URL, BOT_TOKEN)
            if guard is False:
                logger.error(
                    "[MAIN] Polling guard: lock занят другим инстансом. "
                    "Этот процесс не стартует long polling, чтобы не вызывать TelegramConflictError."
                )
                stop = asyncio.Event()
                await stop.wait()
                return
            polling_guard = guard

        # На всякий случай удаляем вебхук и сбрасываем старые апдейты
        await bot.delete_webhook(drop_pending_updates=True)

        await dp.start_polling(bot)
    finally:
        if health_runner:
            await health_runner.cleanup()
        await _release_polling_guard(polling_guard)


if __name__ == "__main__":
    asyncio.run(main())