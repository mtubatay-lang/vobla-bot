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
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
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

# --- OpenAI timeouts (seconds) ---
OPENAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "60"))

# --- Qdrant Vector Database ---
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "knowledge_base")
QDRANT_TIMEOUT = float(os.getenv("QDRANT_TIMEOUT", "30"))

# --- Knowledge Base Settings ---
_manager_usernames_raw = os.getenv("MANAGER_USERNAMES", "")
MANAGER_USERNAMES = [u.strip() for u in _manager_usernames_raw.split(",") if u.strip()] if _manager_usernames_raw else []
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "100"))

# Semantic Chunking Settings
SEMANTIC_CHUNK_MIN_SIZE = int(os.getenv("SEMANTIC_CHUNK_MIN_SIZE", "200"))
SEMANTIC_CHUNK_MAX_SIZE = int(os.getenv("SEMANTIC_CHUNK_MAX_SIZE", "1500"))
SEMANTIC_CHUNK_OVERLAP = int(os.getenv("SEMANTIC_CHUNK_OVERLAP", "150"))

# Re-ranking Settings
RERANK_TOP_K = int(os.getenv("RERANK_TOP_K", "10"))
RERANK_USE_LLM = os.getenv("RERANK_USE_LLM", "true").lower() == "true"
# Минимальный score после rerank: чанки ниже порога не отдаются в генерацию; если все ниже — эскалация
MIN_SCORE_AFTER_RERANK = float(os.getenv("MIN_SCORE_AFTER_RERANK", "0.25"))
# Cross-encoder reranker (Cohere): если True и задан COHERE_API_KEY — используем Cohere Rerank вместо LLM
USE_CROSS_ENCODER_RERANK = os.getenv("USE_CROSS_ENCODER_RERANK", "false").lower() == "true"
COHERE_API_KEY = os.getenv("COHERE_API_KEY", "")
# Гибридный поиск: векторный + BM25 по кандидатам, объединение через RRF
USE_HYBRID_BM25 = os.getenv("USE_HYBRID_BM25", "false").lower() == "true"
# HyDE: генерировать гипотетический ответ, искать по нему в Qdrant, объединять с основным поиском через RRF
USE_HYDE = os.getenv("USE_HYDE", "false").lower() == "true"
# Дедупликация при индексации: не добавлять чанк, если уже есть очень похожий (cosine >= 0.95)
DEDUP_AT_INDEX = os.getenv("DEDUP_AT_INDEX", "true").lower() == "true"
DEDUP_AT_INDEX_THRESHOLD = float(os.getenv("DEDUP_AT_INDEX_THRESHOLD", "0.95"))
# Кэш результатов RAG по запросу (in-memory, TTL в секундах)
RAG_QUERY_CACHE_ENABLED = os.getenv("RAG_QUERY_CACHE_ENABLED", "false").lower() == "true"
RAG_QUERY_CACHE_TTL = int(os.getenv("RAG_QUERY_CACHE_TTL", "3600"))

# Chunk Analysis Settings
CHUNK_ANALYSIS_ENABLED = os.getenv("CHUNK_ANALYSIS_ENABLED", "true").lower() == "true"
MAX_CHUNKS_TO_ANALYZE = int(os.getenv("MAX_CHUNKS_TO_ANALYZE", "10"))

# Максимум раундов уточняющих вопросов (группа и приват); 0 = не задавать уточнений, отвечать сразу по чанкам
MAX_CLARIFICATION_ROUNDS = int(os.getenv("MAX_CLARIFICATION_ROUNDS", "0"))

# --- Full file context (временная замена RAG: один файл целиком в контекст LLM) ---
USE_FULL_FILE_CONTEXT = os.getenv("USE_FULL_FILE_CONTEXT", "false").lower() == "true"
FULL_FILE_PATH = os.getenv("FULL_FILE_PATH", "").strip()
FULL_FILE_MAX_CHARS = int(os.getenv("FULL_FILE_MAX_CHARS", "120000"))

# --- RAG Test Chat Settings (для ограничения работы только в тестовом чате) ---
# RAG_TEST_CHAT_ID: не задана — дефолт -1003377597100 (тестовый чат); пустая строка — все чаты
RAG_TEST_CHAT_ID_DEFAULT = -1003377597100


def get_rag_test_chat_id() -> int | None:
    """Возвращает ID тестового чата для RAG или None (все чаты).
    Не задана env — дефолт RAG_TEST_CHAT_ID_DEFAULT. Пустая env — None (все чаты).
    """
    raw = os.environ.get("RAG_TEST_CHAT_ID")
    if raw is None:
        return RAG_TEST_CHAT_ID_DEFAULT
    if not str(raw).strip():
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        return RAG_TEST_CHAT_ID_DEFAULT
