"""Поиск похожих вопросов среди FAQ (Google Sheets + OpenAI embeddings + AI rerank)."""

from typing import Optional, List, Dict
import math
import asyncio
import re

from app.services.sheets_client import load_faq_rows
from app.services.openai_client import create_embedding, choose_best_faq_answer


# -----------------------------
#   ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -----------------------------

def normalize(text: str) -> str:
    """Простая нормализация текста для эмбеддингов.

    - нижний регистр
    - убираем пунктуацию
    - схлопываем пробелы
    """
    text = text.lower()
    text = re.sub(r"[^\w\sёЁа-яА-Я0-9]", " ", text)  # оставляем буквы/цифры/пробелы
    text = re.sub(r"\s+", " ", text).strip()
    return text


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Косинусное сходство двух векторов."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# -----------------------------
#   КЭШ ВОПРОСОВ И ЭМБЕДДИНГОВ
# -----------------------------

FAQ_DATA: List[Dict[str, str]] = []
FAQ_EMBEDS: List[List[float]] = []
CACHE_READY = False


async def load_faq_cache() -> None:
    """Загружает вопросы + ответы + эмбеддинги в память (один раз)."""
    global FAQ_DATA, FAQ_EMBEDS, CACHE_READY

    if CACHE_READY:
        return

    rows = load_faq_rows()  # [{question, answer}, ...]

    FAQ_DATA = []
    FAQ_EMBEDS = []

    for row in rows:
        question = row["question"]
        answer = row["answer"]

        norm_question = normalize(question)
        emb = await asyncio.to_thread(create_embedding, norm_question)

        FAQ_DATA.append(
            {
                "question": question,
                "norm_question": norm_question,
                "answer": answer,
            }
        )
        FAQ_EMBEDS.append(emb)

    CACHE_READY = True
    print(f"[FAQ] Загружено {len(FAQ_DATA)} записей FAQ (эмбеддинги готовы).")


# -----------------------------
#    ПОИСК ПОХОЖЕГО ВОПРОСА
# -----------------------------

async def find_similar_question(user_question: str) -> Optional[Dict[str, str]]:
    """Возвращает {question, answer, score} или None, если ничего похожего нет.

    Этап 1: эмбеддинги → выбираем топ-K по cosine similarity.
    Этап 2: передаём кандидатов в GPT, он решает, что лучше подходит
            (или что лучше ничего не отвечать).
    """

    await load_faq_cache()

    if not FAQ_DATA:
        return None

    # Этап 1: эмбеддинг пользователя + базовый фильтр по cosine
    norm_user = normalize(user_question)
    user_emb = await asyncio.to_thread(create_embedding, norm_user)

    scores: List[float] = []
    for emb in FAQ_EMBEDS:
        scores.append(cosine_similarity(user_emb, emb))

    # Базовый порог, чтобы совсем мусор не отправлять в GPT
    BASE_THRESHOLD = 0.55

    indexed_scores = [
        (idx, score) for idx, score in enumerate(scores) if score >= BASE_THRESHOLD
    ]

    if not indexed_scores:
        # Ничего даже отдалённо похожего
        return None

    # Сортируем по убыванию, берём топ-K
    indexed_scores.sort(key=lambda x: x[1], reverse=True)
    TOP_K = 5
    top_indices = [idx for idx, _ in indexed_scores[:TOP_K]]

    candidates: List[Dict[str, str]] = []
    for idx in top_indices:
        data = FAQ_DATA[idx]
        candidates.append(
            {
                "question": data["question"],
                "answer": data["answer"],
                "score": scores[idx],
            }
        )

    # Этап 2: AI-rerank — даём GPT выбрать лучшего кандидата
    best = await asyncio.to_thread(
        choose_best_faq_answer,
        user_question,
        candidates,
    )

    # best либо dict с question/answer/score, либо None
    return best