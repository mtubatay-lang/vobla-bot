"""Загрузка одного ключевого файла (txt/md/pdf) в контекст для QA без RAG."""

import logging
from pathlib import Path
from typing import Optional

from app.config import (
    USE_FULL_FILE_CONTEXT,
    FULL_FILE_PATH,
    FULL_FILE_MAX_CHARS,
)

logger = logging.getLogger(__name__)

_cached_content: Optional[str] = None


def get_full_file_context() -> Optional[str]:
    """
    Возвращает текст документа для режима «полный файл в контексте».
    Поддерживаются .txt, .md и .pdf. Результат кэшируется.
    Если режим выключен, путь пустой или файл не удалось прочитать — возвращает None.
    """
    global _cached_content
    if not USE_FULL_FILE_CONTEXT or not FULL_FILE_PATH:
        return None
    if _cached_content is not None:
        return _cached_content
    path = Path(FULL_FILE_PATH)
    if not path.exists():
        logger.warning("[FULL_FILE_CONTEXT] Файл не найден: %s", FULL_FILE_PATH)
        return None
    suffix = path.suffix.lower()
    raw: Optional[str] = None
    file_type = "txt"
    if suffix == ".pdf":
        file_type = "pdf"
        try:
            import fitz
            doc = fitz.open(path)
            parts = []
            for page in doc:
                parts.append(page.get_text())
            doc.close()
            raw = "\n".join(parts)
        except Exception as e:
            logger.exception("[FULL_FILE_CONTEXT] Ошибка чтения PDF %s: %s", path, e)
            return None
    else:
        try:
            raw = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            try:
                raw = path.read_text(encoding="cp1251")
            except Exception as e:
                logger.exception("[FULL_FILE_CONTEXT] Ошибка чтения текстового файла %s: %s", path, e)
                return None
        except Exception as e:
            logger.exception("[FULL_FILE_CONTEXT] Ошибка чтения %s: %s", path, e)
            return None
    if not raw or not raw.strip():
        logger.warning("[FULL_FILE_CONTEXT] Файл пустой: %s", path)
        return None
    text = raw.strip()
    if len(text) > FULL_FILE_MAX_CHARS:
        text = text[:FULL_FILE_MAX_CHARS]
        logger.info(
            "[FULL_FILE_CONTEXT] Текст обрезан до %s символов (было больше)",
            FULL_FILE_MAX_CHARS,
        )
    _cached_content = text
    logger.info(
        "[FULL_FILE_CONTEXT] Загружен файл %s (%s), символов: %s",
        path.name,
        file_type,
        len(_cached_content),
    )
    return _cached_content
