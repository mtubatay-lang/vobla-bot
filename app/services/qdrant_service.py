"""Сервис для работы с Qdrant векторной базой данных."""

import logging
import uuid
from typing import List, Dict, Optional, Any
from datetime import datetime

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue, Query

from app.config import (
    QDRANT_URL,
    QDRANT_API_KEY,
    QDRANT_COLLECTION_NAME,
    QDRANT_TIMEOUT,
    DEDUP_AT_INDEX,
    DEDUP_AT_INDEX_THRESHOLD,
)

logger = logging.getLogger(__name__)


class QdrantService:
    """Сервис для работы с Qdrant векторной БД."""
    
    def __init__(self):
        """Инициализирует подключение к Qdrant и создает/получает коллекцию."""
        try:
            if QDRANT_API_KEY:
                self.client = QdrantClient(
                    url=QDRANT_URL,
                    api_key=QDRANT_API_KEY,
                    timeout=QDRANT_TIMEOUT,
                )
            else:
                self.client = QdrantClient(url=QDRANT_URL, timeout=QDRANT_TIMEOUT)
            
            self.collection_name = QDRANT_COLLECTION_NAME
            self._ensure_collection()
            logger.info(f"[QDRANT] Подключен к {QDRANT_URL}, коллекция: {self.collection_name}")
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка подключения: {e}")
            raise
    
    def _ensure_collection(self) -> None:
        """Создает коллекцию, если её нет."""
        try:
            collections = self.client.get_collections().collections
            collection_names = [c.name for c in collections]
            
            if self.collection_name not in collection_names:
                # Создаем коллекцию с векторами размерности 1536 (text-embedding-3-small)
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=1536,
                        distance=Distance.COSINE,
                    ),
                )
                logger.info(f"[QDRANT] Создана коллекция {self.collection_name}")
            else:
                logger.debug(f"[QDRANT] Коллекция {self.collection_name} уже существует")
            
            # Обеспечиваем наличие индекса для поля source
            self._ensure_payload_index()
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка создания коллекции: {e}")
            raise
    
    def _ensure_payload_index(self) -> None:
        """Создает индексы для полей в payload, если их нет."""
        # Список полей для индексации
        fields_to_index = [
            ("source", "keyword"),
            ("document_type", "keyword"),
            ("category", "keyword"),
            ("tags", "keyword"),  # Для массивов тоже keyword
        ]
        
        for field_name, field_schema in fields_to_index:
            try:
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field_name,
                    field_schema=field_schema,
                )
                logger.info(f"[QDRANT] Создан индекс для поля '{field_name}' в коллекции {self.collection_name}")
            except Exception as e:
                # Индекс уже существует или другая ошибка
                error_msg = str(e).lower()
                if "already exists" in error_msg or "index" in error_msg:
                    logger.debug(f"[QDRANT] Индекс для '{field_name}' уже существует или не требуется: {e}")
                else:
                    logger.warning(f"[QDRANT] Не удалось создать индекс для '{field_name}': {e}")
    
    def add_documents(
        self,
        chunks: List[Dict[str, Any]],
        embeddings: List[List[float]],
    ) -> None:
        """Добавляет документы (чанки) с эмбеддингами в Qdrant.
        
        Args:
            chunks: Список словарей с данными чанков. Каждый должен содержать:
                - text: текст чанка
                - metadata: словарь с метаданными (source, document_title, и т.д.)
            embeddings: Список эмбеддингов (каждый - список из 1536 float)
        """
        if len(chunks) != len(embeddings):
            raise ValueError(f"Количество чанков ({len(chunks)}) не совпадает с количеством эмбеддингов ({len(embeddings)})")
        
        if not chunks:
            return
        
        from app.services.chunking_service import get_chunk_structural_metadata

        points = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            if DEDUP_AT_INDEX:
                dup = self.search(
                    query_embedding=embedding,
                    top_k=1,
                    score_threshold=DEDUP_AT_INDEX_THRESHOLD,
                )
                if dup:
                    logger.debug(
                        f"[QDRANT] Пропуск чанка (дубликат, score={dup[0].get('score', 0):.3f})"
                    )
                    continue
            point_id = str(uuid.uuid4())
            text = chunk.get("text", "")
            meta = dict(chunk.get("metadata", {}))
            for key, value in get_chunk_structural_metadata(text).items():
                if key not in meta:
                    meta[key] = value
            payload = {"text": text, **meta}
            points.append(
                PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload=payload,
                )
            )
        if not points:
            logger.info("[QDRANT] Все чанки отфильтрованы как дубликаты, нечего добавлять")
            return
        try:
            self.client.upsert(
                collection_name=self.collection_name,
                points=points,
            )
            logger.info(f"[QDRANT] Добавлено {len(points)} документов в коллекцию {self.collection_name}")
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка добавления документов: {e}")
            raise
    
    def search(
        self,
        query_embedding: List[float],
        top_k: int = 5,
        score_threshold: float = 0.7,
        source_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Поиск похожих документов в Qdrant.
        
        Args:
            query_embedding: Эмбеддинг запроса (список из 1536 float)
            top_k: Количество результатов для возврата
            score_threshold: Минимальный score для включения в результаты
            source_filter: Опциональный фильтр по полю source в метаданных
        
        Returns:
            Список словарей с результатами:
            {
                "text": str,
                "metadata": dict,
                "score": float,
            }
        """
        query_filter = None
        if source_filter:
            query_filter = Filter(
                must=[
                    FieldCondition(
                        key="source",
                        match=MatchValue(value=source_filter),
                    )
                ]
            )

        for attempt in range(2):
            try:
                # Используем новый API query_points вместо устаревшего search
                results = self.client.query_points(
                    collection_name=self.collection_name,
                    query=query_embedding,
                    limit=top_k,
                    score_threshold=score_threshold,
                    query_filter=query_filter,
                )
                break
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"[QDRANT] Ошибка поиска (попытка {attempt + 1}/2): {e}, повтор...")
                else:
                    logger.exception(f"[QDRANT] Ошибка поиска после повтора: {e}")
                    return []
        else:
            return []

        try:
            # Преобразуем результаты в удобный формат
            # query_points возвращает объект QueryResult, нужно получить points
            formatted_results = []
            scores_list = []
            for point in results.points:
                # Получаем score из объекта ScoredPoint
                score = getattr(point, 'score', None)
                if score is None:
                    # Если score не указан напрямую, пропускаем точку
                    continue
                
                scores_list.append(score)
                payload = getattr(point, 'payload', {}) or {}
                point_id = getattr(point, 'id', None)
                formatted_results.append({
                    "id": str(point_id) if point_id is not None else "",
                    "text": payload.get("text", ""),
                    "metadata": {k: v for k, v in payload.items() if k != "text"},
                    "score": score,
                })
            
            # Логируем результаты поиска для диагностики
            if formatted_results:
                logger.info(
                    f"[QDRANT] Найдено {len(formatted_results)} чанков "
                    f"(scores: {[f'{s:.3f}' for s in scores_list[:3]]})"
                )
            else:
                logger.warning(
                    f"[QDRANT] Не найдено чанков с score >= {score_threshold}. "
                    f"Всего точек в результате: {len(results.points)}"
                )
                # Логируем все scores, если они есть, но не прошли threshold
                if results.points:
                    all_scores = [getattr(p, 'score', None) for p in results.points if hasattr(p, 'score')]
                    if all_scores:
                        logger.warning(f"[QDRANT] Все scores: {[f'{s:.3f}' for s in all_scores[:5]]}")
            
            return formatted_results
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка поиска: {e}")
            return []
    
    def search_multi_level(
        self,
        query_embedding: List[float],
        top_k: int = 5,
        initial_threshold: float = 0.5,
        fallback_thresholds: List[float] = None,
        source_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Многоуровневый поиск с постепенным снижением threshold.
        
        Если первый поиск не находит результатов, пробует с более низкими threshold.
        Это помогает находить релевантные чанки даже при низкой изначальной семантической близости.
        
        Args:
            query_embedding: Эмбеддинг запроса (список из 1536 float)
            top_k: Количество результатов для возврата
            initial_threshold: Начальный threshold для поиска
            fallback_thresholds: Список threshold для попыток, если первый поиск не нашел результатов
            source_filter: Опциональный фильтр по полю source в метаданных
        
        Returns:
            Список словарей с результатами (формат как в search)
        """
        if fallback_thresholds is None:
            fallback_thresholds = [0.3, 0.1]
        
        # Пробуем с начальным threshold
        results = self.search(
            query_embedding=query_embedding,
            top_k=top_k,
            score_threshold=initial_threshold,
            source_filter=source_filter,
        )
        
        if results:
            logger.info(
                f"[QDRANT] Многоуровневый поиск: найдено {len(results)} чанков "
                f"с threshold={initial_threshold:.2f}"
            )
            return results
        
        # Если не нашли, пробуем с fallback thresholds
        logger.info(
            f"[QDRANT] Многоуровневый поиск: не найдено с threshold={initial_threshold:.2f}, "
            f"пробуем fallback thresholds: {fallback_thresholds}"
        )
        
        for threshold in fallback_thresholds:
            results = self.search(
                query_embedding=query_embedding,
                top_k=top_k,
                score_threshold=threshold,
                source_filter=source_filter,
            )
            
            if results:
                logger.info(
                    f"[QDRANT] Многоуровневый поиск: найдено {len(results)} чанков "
                    f"с threshold={threshold:.2f}"
                )
                # Предупреждение, если threshold очень низкий
                if threshold < 0.3:
                    logger.warning(
                        f"[QDRANT] ВНИМАНИЕ: Найдены чанки с низким threshold={threshold:.2f}. "
                        f"Возможно, релевантность низкая."
                    )
                return results
        
        # Если ничего не нашли на всех уровнях
        logger.warning(
            f"[QDRANT] Многоуровневый поиск: не найдено чанков даже с минимальным threshold={fallback_thresholds[-1]:.2f}"
        )
        return []
    
    def delete_by_source(self, source: str) -> None:
        """Удаляет все документы с указанным source из коллекции.
        
        Args:
            source: Значение поля source для фильтрации
        """
        try:
            # Получаем все точки с указанным source
            scroll_result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="source",
                            match=MatchValue(value=source),
                        )
                    ]
                ),
                limit=10000,  # Максимум для удаления
            )
            
            if scroll_result[0]:  # Есть точки для удаления
                point_ids = [point.id for point in scroll_result[0]]
                self.client.delete(
                    collection_name=self.collection_name,
                    points_selector=point_ids,
                )
                logger.info(f"[QDRANT] Удалено {len(point_ids)} документов с source={source}")
            else:
                logger.debug(f"[QDRANT] Нет документов с source={source} для удаления")
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка удаления по source: {e}")
            raise
    
    def get_collection_info(self) -> Dict[str, Any]:
        """Возвращает информацию о коллекции (размер, количество документов)."""
        try:
            collection_info = self.client.get_collection(self.collection_name)
            return {
                "name": self.collection_name,
                "points_count": collection_info.points_count,
                "vectors_count": collection_info.vectors_count,
                "config": {
                    "vector_size": collection_info.config.params.vectors.size,
                    "distance": collection_info.config.params.vectors.distance,
                },
            }
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка получения информации о коллекции: {e}")
            return {}


# Глобальный экземпляр сервиса
_qdrant_service: Optional[QdrantService] = None


def get_qdrant_service() -> QdrantService:
    """Возвращает глобальный экземпляр QdrantService (singleton)."""
    global _qdrant_service
    if _qdrant_service is None:
        _qdrant_service = QdrantService()
    return _qdrant_service
