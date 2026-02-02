"""Сервис для re-ranking результатов поиска через LLM."""

import asyncio
import logging
from typing import List, Dict, Any, Optional

from app.services.openai_client import client, CHAT_MODEL
from app.config import RERANK_TOP_K, RERANK_USE_LLM

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
    if not RERANK_USE_LLM:
        # Если re-ranking отключен, просто сортируем по исходному score
        sorted_chunks = sorted(chunks, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_chunks[:top_k or RERANK_TOP_K]
    
    if not chunks:
        return []
    
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
            "5. Верни список номеров чанков в порядке убывания релевантности\n"
            "6. Формат ответа: только номера чанков через запятую, например: 3,1,5,2,4"
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
