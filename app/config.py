"""Конфигурация приложения Vobla Bot."""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env (локально) и из окружения (Railway)
load_dotenv()

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Уровень логирования (по умолчанию INFO)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# DSN для Sentry (может быть пустым)
SENTRY_DSN = os.getenv("SENTRY_DSN", "")

if not BOT_TOKEN:
    raise ValueError(
        "BOT_TOKEN не найден в переменных окружения! "
        "Проверьте .env или Variables на сервере Railway."
    )