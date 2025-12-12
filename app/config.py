"""Конфигурация приложения Vobla Bot."""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env (локально) и из окружения (Railway)
load_dotenv()

# --- Базовые настройки бота ---

BOT_TOKEN = os.getenv("BOT_TOKEN")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
SENTRY_DSN = os.getenv("SENTRY_DSN", "")

# --- OpenAI ---

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_EMBEDDING_MODEL = os.getenv(
    "OPENAI_EMBEDDING_MODEL",
    "text-embedding-3-small",
)

# --- Google Sheets ---

SHEET_ID = os.getenv("SHEET_ID")
SHEET_RANGE = os.getenv("SHEET_RANGE", "'Sheet1'!C:D")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# --- Куда слать вопросы без ответа ---

# В .env у тебя MANAGER_CHAT_ID=3243490449
_manager_chat_id_raw = os.getenv("MANAGER_CHAT_ID", "0")
try:
    MANAGER_CHAT_ID = int(_manager_chat_id_raw)
except ValueError:
    MANAGER_CHAT_ID = 0


# --- Валидация критичных настроек ---

if not BOT_TOKEN:
    raise ValueError(
        "BOT_TOKEN не найден в переменных окружения! "
        "Проверь .env или Variables на сервере Railway."
    )

if not OPENAI_API_KEY:
    raise ValueError(
        "OPENAI_API_KEY не найден в переменных окружения! "
        "Проверь .env или Variables на сервере Railway."
    )

if not SHEET_ID:
    raise ValueError(
        "SHEET_ID не задан. Укажи ID Google-таблицы в .env/Variables."
    )

if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise ValueError(
        "GOOGLE_SERVICE_ACCOUNT_JSON не задан. "
        "Вставь JSON сервисного аккаунта в .env/Variables."
    )

# Google Sheets: пользователи бота
USERS_SHEET_ID = os.getenv("USERS_SHEET_ID")
USERS_SHEET_RANGE = os.getenv("USERS_SHEET_RANGE", "'Пользователи'!A2:H1000")

# Google Sheets: статистика и логирование событий
STATS_SHEET_ID = os.getenv("STATS_SHEET_ID", "")
STATS_SHEET_TAB = os.getenv("STATS_SHEET_TAB", "bot_stats")
PENDING_SHEET_TAB = os.getenv("PENDING_SHEET_TAB", "pending_questions")