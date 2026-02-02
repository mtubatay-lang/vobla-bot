"""Кэш результатов RAG-поиска по запросу (in-memory, TTL)."""

import hashlib
from typing import List, Dict, Any, Optional

from cachetools import TTLCache

from app.config import RAG_QUERY_CACHE_TTL, RAG_QUERY_CACHE_ENABLED

_cache: TTLCache = TTLCache(maxsize=500, ttl=RAG_QUERY_CACHE_TTL)


def _normalize(q: str) -> str:
    return (q or "").strip().lower()[:300]


def _key(query: str) -> str:
    return hashlib.sha256(_normalize(query).encode("utf-8")).hexdigest()


def get_cached_chunks(query: str) -> Optional[List[Dict[str, Any]]]:
    """Возвращает закэшированные found_chunks для запроса или None."""
    if not RAG_QUERY_CACHE_ENABLED:
        return None
    k = _key(query)
    try:
        return _cache[k]
    except KeyError:
        return None


def set_cached_chunks(query: str, chunks: List[Dict[str, Any]]) -> None:
    """Сохраняет found_chunks в кэш по запросу."""
    if not RAG_QUERY_CACHE_ENABLED or not chunks:
        return
    _cache[_key(query)] = chunks
