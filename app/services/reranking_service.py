"""Сервис для re-ranking результатов поиска через LLM."""

import asyncio
import logging
from typing import List, Dict, Any, Optional

from app.services.openai_client import client, CHAT_MODEL
from app.config import RERANK_TOP_K, RERANK_USE_LLM, USE_CROSS_ENCODER_RERANK, COHERE_API_KEY

logger = logging.getLogger(__name__)


async def rerank_chunks_with_llm(
    question: str,
    chunks: List[Dict[str, Any]],
    top_k: int = None,
) -> List[Dict[str, Any]]:
    """Переранжирует чанки по релевантности к вопросу через LLM.
    
    Args:
        question: Вопрос пользователя
        chunks: Список найденных чанков (должны содержать "text" и "score")
        top_k: Количество топ-чанков для возврата (по умолчанию из конфига)
    
    Returns:
        Отсортированный список чанков с обновленными scores
    """
    if not chunks:
        return []

    if USE_CROSS_ENCODER_RERANK and COHERE_API_KEY:
        from app.services.cross_encoder_reranker import rerank_chunks_with_cohere
        return await rerank_chunks_with_cohere(question, chunks, top_k=top_k or RERANK_TOP_K)

    if not RERANK_USE_LLM:
        # Если re-ranking отключен, просто сортируем по исходному score
        sorted_chunks = sorted(chunks, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_chunks[:top_k or RERANK_TOP_K]
    
    if top_k is None:
        top_k = RERANK_TOP_K
    
    # Ограничиваем количество чанков для переранжирования (слишком много - дорого)
    chunks_to_rerank = chunks[:min(len(chunks), top_k * 2)]  # Берем в 2 раза больше для лучшего выбора
    
    if len(chunks_to_rerank) <= 1:
        return chunks_to_rerank
    
    try:
        # Формируем промпт для LLM
        chunks_text = "\n\n---\n\n".join([
            f"Чанк {i+1} (исходный score: {chunk.get('score', 0):.3f}):\n{chunk.get('text', '')[:500]}"
            for i, chunk in enumerate(chunks_to_rerank)
        ])
        
        system_prompt = (
            "Ты помощник для переранжирования результатов поиска.\n"
            "Твоя задача — оценить релевантность каждого чанка к вопросу пользователя "
            "и отсортировать их по убыванию релевантности.\n\n"
            "Правила:\n"
            "1. Оценивай релевантность от 0.0 до 1.0\n"
            "2. Чанк должен напрямую отвечать на вопрос или содержать релевантную информацию\n"
            "3. Учитывай исходный score, но можешь его корректировать\n"
            "4. Если в вопросе есть «чек-лист», «критерии», «требования к помещению/месту», «выбор месторасположения» — отдавай приоритет чанкам с подробным нумерованным списком (много пунктов), а не короткому перечню из 2–3 пунктов.\n"
            "5. При равной релевантности предпочитай чанки из официальных источников (чек-лист, регламент, document_type или source с такими значениями).\n"
            "6. Верни список номеров чанков в порядке убывания релевантности\n"
            "7. Формат ответа: только номера чанков через запятую, например: 3,1,5,2,4"
        )
        
        user_prompt = (
            f"Вопрос пользователя: {question}\n\n"
            f"Найденные чанки:\n{chunks_text}\n\n"
            "Отсортируй чанки по релевантности к вопросу. "
            "Верни только номера чанков через запятую в порядке убывания релевантности."
        )
        
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=100,
        )
        
        answer = (resp.choices[0].message.content or "").strip()
        
        # Парсим ответ (номера чанков)
        try:
            # Извлекаем числа из ответа
            import re
            numbers = [int(n) for n in re.findall(r'\d+', answer)]
            # Номера в ответе начинаются с 1, индексы с 0
            reranked_indices = [n - 1 for n in numbers if 1 <= n <= len(chunks_to_rerank)]
            
            # Если парсинг не удался, используем исходный порядок
            if not reranked_indices:
                logger.warning("[RERANKING] Не удалось распарсить ответ LLM, используем исходный порядок")
                reranked_chunks = chunks_to_rerank[:top_k]
            else:
                # Создаем переранжированный список
                reranked_chunks = []
                seen_indices = set()
                for idx in reranked_indices:
                    if idx not in seen_indices and idx < len(chunks_to_rerank):
                        reranked_chunks.append(chunks_to_rerank[idx])
                        seen_indices.add(idx)
                
                # Добавляем оставшиеся чанки, которые не были упомянуты
                for i, chunk in enumerate(chunks_to_rerank):
                    if i not in seen_indices:
                        reranked_chunks.append(chunk)
                
                # Берем только top_k
                reranked_chunks = reranked_chunks[:top_k]
            
            logger.info(
                f"[RERANKING] Переранжировано {len(reranked_chunks)} чанков из {len(chunks_to_rerank)} "
                f"для вопроса: '{question[:50]}...'"
            )
            
            return reranked_chunks
            
        except Exception as e:
            logger.exception(f"[RERANKING] Ошибка парсинга ответа LLM: {e}")
            # При ошибке возвращаем исходный порядок
            return chunks_to_rerank[:top_k]
            
    except Exception as e:
        logger.exception(f"[RERANKING] Ошибка re-ranking через LLM: {e}")
        # При ошибке возвращаем исходный порядок
        return sorted(chunks_to_rerank, key=lambda x: x.get("score", 0), reverse=True)[:top_k]


def _source_priority(chunk: Dict[str, Any]) -> int:
    """Возвращает 1 если чанк из официального источника (чек-лист, регламент), иначе 0."""
    meta = chunk.get("metadata") or {}
    if not isinstance(meta, dict):
        return 0
    source = (meta.get("source") or "").lower()
    doc_type = (meta.get("document_type") or "").lower()
    for term in ("чек-лист", "чеклист", "регламент", "checklist", "официальн"):
        if term in source or term in doc_type:
            return 1
    return 0


def select_best_chunks(
    chunks: List[Dict[str, Any]],
    max_chunks: int = 5,
    min_score: float = 0.0,
) -> List[Dict[str, Any]]:
    """Выбирает лучшие чанки, убирая дубликаты и перекрывающуюся информацию.
    
    Args:
        chunks: Список чанков
        max_chunks: Максимальное количество чанков для возврата
        min_score: Минимальный score для включения
    
    Returns:
        Отфильтрованный список лучших чанков
    """
    if not chunks:
        return []
    
    # Фильтруем по минимальному score
    filtered_chunks = [chunk for chunk in chunks if chunk.get("score", 0) >= min_score]
    # При равном score предпочитаем официальные источники (чек-лист, регламент)
    filtered_chunks.sort(
        key=lambda c: (c.get("score", 0), _source_priority(c)),
        reverse=True,
    )
    
    # Убираем дубликаты по тексту (схожие чанки)
    seen_texts = set()
    unique_chunks = []
    
    for chunk in filtered_chunks:
        text = chunk.get("text", "").strip()
        # Нормализуем текст для сравнения (убираем пробелы, приводим к нижнему регистру)
        normalized = " ".join(text.lower().split())
        
        # Проверяем, не похож ли этот текст на уже добавленный
        is_duplicate = False
        for seen_text in seen_texts:
            # Если тексты совпадают более чем на 80%, считаем дубликатом
            if len(normalized) > 0 and len(seen_text) > 0:
                similarity = len(set(normalized.split()) & set(seen_text.split())) / max(
                    len(set(normalized.split())), len(set(seen_text.split())), 1
                )
                if similarity > 0.8:
                    is_duplicate = True
                    break
        
        if not is_duplicate:
            unique_chunks.append(chunk)
            seen_texts.add(normalized)
            
            if len(unique_chunks) >= max_chunks:
                break
    
    logger.info(
        f"[RERANKING] Выбрано {len(unique_chunks)} уникальных чанков из {len(chunks)} "
        f"(max={max_chunks}, min_score={min_score})"
    )
    
    return unique_chunks


def _diversity_group_key(chunk: Dict[str, Any], diversity_key: str = "section_heading") -> str:
    """Ключ группы для diversity: section_heading или is_checklist + первые слова текста."""
    meta = chunk.get("metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    if diversity_key == "section_heading":
        sh = (meta.get("section_heading") or "").strip()
        if sh:
            return sh[:150]
    # Fallback: is_checklist + first 80 chars of text
    text = (chunk.get("text") or "").strip()
    prefix = " ".join(text.split()[:15])[:80] if text else ""
    is_cl = meta.get("is_checklist", False)
    return f"cl_{is_cl}_{prefix}"


def select_best_chunks_diverse(
    chunks: List[Dict[str, Any]],
    max_chunks: int = 5,
    min_score: float = 0.0,
    diversity_key: str = "section_heading",
    max_per_group: int = 2,
) -> List[Dict[str, Any]]:
    """Выбирает лучшие чанки с учётом разнообразия: не более max_per_group из одной группы.
    
    Группировка по section_heading (или по is_checklist + начало текста). Сначала набираем
    по одному-два чанка из разных групп, затем добиваем по score до max_chunks.
    """
    if not chunks:
        return []
    
    filtered = [c for c in chunks if c.get("score", 0) >= min_score]
    filtered.sort(
        key=lambda c: (c.get("score", 0), _source_priority(c)),
        reverse=True,
    )
    
    group_counts: Dict[str, int] = {}
    unique_chunks: List[Dict[str, Any]] = []
    seen_texts: set = set()
    
    # Фаза 1: набираем до max_chunks с лимитом max_per_group на группу
    for chunk in filtered:
        if len(unique_chunks) >= max_chunks:
            break
        text = chunk.get("text", "").strip()
        normalized = " ".join(text.lower().split())
        is_dup = any(
            len(set(normalized.split()) & set(s.split())) / max(len(set(normalized.split())), len(set(s.split())), 1) > 0.8
            for s in seen_texts
        )
        if is_dup:
            continue
        gk = _diversity_group_key(chunk, diversity_key)
        if group_counts.get(gk, 0) >= max_per_group:
            continue
        unique_chunks.append(chunk)
        seen_texts.add(normalized)
        group_counts[gk] = group_counts.get(gk, 0) + 1
    
    # Фаза 2: если не набрали max_chunks, добиваем по score без лимита по группе
    for chunk in filtered:
        if len(unique_chunks) >= max_chunks:
            break
        if chunk in unique_chunks:
            continue
        text = chunk.get("text", "").strip()
        normalized = " ".join(text.lower().split())
        is_dup = any(
            len(set(normalized.split()) & set(s.split())) / max(len(set(normalized.split())), len(set(s.split())), 1) > 0.8
            for s in seen_texts
        )
        if is_dup:
            continue
        unique_chunks.append(chunk)
        seen_texts.add(normalized)
    
    logger.info(
        f"[RERANKING] Выбрано {len(unique_chunks)} чанков (diverse, max={max_chunks}, min_score={min_score})"
    )
    return unique_chunks
