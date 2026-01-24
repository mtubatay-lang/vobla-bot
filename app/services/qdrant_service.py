"""Сервис для работы с Qdrant векторной базой данных."""

import logging
import uuid
from typing import List, Dict, Optional, Any
from datetime import datetime

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue, Query

from app.config import QDRANT_URL, QDRANT_API_KEY, QDRANT_COLLECTION_NAME

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
                )
            else:
                self.client = QdrantClient(url=QDRANT_URL)
            
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
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка создания коллекции: {e}")
            raise
    
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
        
        points = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            point_id = str(uuid.uuid4())
            
            # Подготавливаем payload (метаданные)
            payload = {
                "text": chunk.get("text", ""),
                **chunk.get("metadata", {}),
            }
            
            points.append(
                PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload=payload,
                )
            )
        
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
        try:
            # Строим фильтр, если указан source
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
            
            # Используем новый API query_points вместо устаревшего search
            # Передаем query_embedding напрямую как список чисел
            results = self.client.query_points(
                collection_name=self.collection_name,
                query=query_embedding,  # Передаем список напрямую
                limit=top_k,
                score_threshold=score_threshold,
                query_filter=query_filter,
            )
            
            # Преобразуем результаты в удобный формат
            # query_points возвращает объект QueryResult, нужно получить points
            formatted_results = []
            for point in results.points:
                # Получаем score из объекта ScoredPoint
                score = getattr(point, 'score', None)
                if score is None:
                    # Если score не указан напрямую, пропускаем точку
                    continue
                
                payload = getattr(point, 'payload', {}) or {}
                formatted_results.append({
                    "text": payload.get("text", ""),
                    "metadata": {k: v for k, v in payload.items() if k != "text"},
                    "score": score,
                })
            
            return formatted_results
        except Exception as e:
            logger.exception(f"[QDRANT] Ошибка поиска: {e}")
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
