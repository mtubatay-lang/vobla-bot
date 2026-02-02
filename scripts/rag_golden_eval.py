"""
Скрипт оценки RAG по золотому набору вопросов.
Запуск: из корня проекта с настроенным .env:
  python -m scripts.rag_golden_eval [путь к golden_set.json]

Формат golden_set.json:
  [
    {"question": "как выбрать место для магазина", "expected_contains": "чек-лист выбора месторасположения"},
    {"question": "...", "expected_contains": "фрагмент текста из эталонного чанка"}
  ]
"""

import asyncio
import json
import os
import sys

# Добавляем корень проекта в path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.openai_client import create_embedding
from app.services.qdrant_service import get_qdrant_service
from app.services.reranking_service import rerank_chunks_with_llm, select_best_chunks


def load_golden_set(path: str):
    p = path or os.path.join(os.path.dirname(__file__), "golden_set.json")
    if not os.path.exists(p):
        return []
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


async def run_search(question: str, top_k: int = 5):
    qdrant = get_qdrant_service()
    emb = await asyncio.to_thread(create_embedding, question)
    chunks = qdrant.search_multi_level(
        query_embedding=emb,
        top_k=15,
        initial_threshold=0.4,
        fallback_thresholds=[0.3, 0.1],
    )
    if not chunks:
        return []
    reranked = await rerank_chunks_with_llm(question, chunks, top_k=8)
    return select_best_chunks(reranked, max_chunks=top_k, min_score=0.0)


async def main():
    path = sys.argv[1] if len(sys.argv) > 1 else None
    golden = load_golden_set(path)
    if not golden:
        print("Золотой набор пуст или файл не найден. Создайте scripts/golden_set.json")
        return
    hits = 0
    for i, item in enumerate(golden):
        q = item.get("question", "")
        expected = (item.get("expected_contains") or "").strip().lower()
        if not q or not expected:
            continue
        chunks = await run_search(q, top_k=5)
        texts = " ".join([(c.get("text") or "") for c in chunks]).lower()
        if expected in texts:
            hits += 1
            print(f"[OK] {i+1}: '{q[:50]}...'")
        else:
            print(f"[MISS] {i+1}: '{q[:50]}...' (expected substring not in top-5)")
    total = len([x for x in golden if x.get("question") and x.get("expected_contains")])
    if total:
        print(f"\nHit@5: {hits}/{total} ({100 * hits / total:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())
