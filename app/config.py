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
QA_FEEDBACK_SHEET_TAB = os.getenv("QA_FEEDBACK_SHEET_TAB", "qa_feedback")

# Google Sheets: получатели для рассылок
RECIPIENTS_USERS_TAB = os.getenv("RECIPIENTS_USERS_TAB", "recipients_users")
RECIPIENTS_CHATS_TAB = os.getenv("RECIPIENTS_CHATS_TAB", "recipients_chats")
BROADCASTS_TAB = os.getenv("BROADCASTS_TAB", "broadcasts")
BROADCAST_LOGS_TAB = os.getenv("BROADCAST_LOGS_TAB", "broadcast_logs")

# --- Qdrant Vector Database ---
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "knowledge_base")

# --- Knowledge Base Settings ---
_manager_usernames_raw = os.getenv("MANAGER_USERNAMES", "")
MANAGER_USERNAMES = [u.strip() for u in _manager_usernames_raw.split(",") if u.strip()] if _manager_usernames_raw else []
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "100"))
# --- RAG Test Chat Settings (для ограничения работы только в тестовом чате) ---
# Опциональная переменная: если не указана, бот работает во всех чатах
_test_chat_id_raw = os.environ.get("RAG_TEST_CHAT_ID")  # Не указываем default, чтобы Railway не требовал её
RAG_TEST_CHAT_ID = None
if _test_chat_id_raw:
    try:
        RAG_TEST_CHAT_ID = int(_test_chat_id_raw)
    except (ValueError, TypeError):
        RAG_TEST_CHAT_ID = None
