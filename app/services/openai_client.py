"""Клиент OpenAI для эмбеддингов и работы с ответами FAQ."""

import os
from typing import List, Dict, Optional

from openai import OpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY не задан в переменных окружения")

client = OpenAI(api_key=OPENAI_API_KEY)


# -----------------------------
#     ЭМБЕДДИНГИ
# -----------------------------

EMBEDDING_MODEL = "text-embedding-3-small"
CHAT_MODEL = "gpt-4.1-mini"  # можно заменить на другой, если нужно


def create_embedding(text: str) -> List[float]:
    """Создаёт эмбеддинг для строки."""
    resp = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text,
    )
    return resp.data[0].embedding


# -----------------------------
#   АДАПТАЦИЯ ГОТОВОГО ОТВЕТА
# -----------------------------

def adapt_faq_answer(user_question: str, base_answer: str) -> str:
    """Адаптирует готовый ответ из базы под формулировку пользователя.

    Важно: ИИ НЕ должен менять суть / фактуру ответа, только форму.
    """
    messages = [
        {
            "role": "system",
            "content": (
                "Ты помощник для корпоративного чат-бота сети магазинов Воблабир. "
                "Твоя задача — аккуратно переформулировать готовый ответ менеджера "
                "под формулировку вопроса пользователя. "
                "Ты не придумываешь новых правил, акций или условий, "
                "а только объясняешь уже написанное своими словами. "
                "Отвечай по-деловому, но дружелюбно, на русском языке."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Вопрос пользователя:\n{user_question}\n\n"
                f"Готовый ответ из базы (его смысл менять нельзя):\n{base_answer}\n\n"
                "Сформулируй финальный ответ для пользователя, сохраняя фактический смысл."
            ),
        },
    ]

    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=messages,
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()


# -----------------------------
#        AI RERANK FAQ
# -----------------------------

def choose_best_faq_answer(
    user_question: str,
    candidates: List[Dict[str, str]],
) -> Optional[Dict[str, str]]:
    """Выбирает лучшего кандидата среди FAQ с помощью GPT.

    candidates: список словарей вида:
      { "question": str, "answer": str, "score": float }

    Возвращает одного кандидата (dict) или None, если GPT считает,
    что ни один вариант не подходит достаточно хорошо.
    """
    if not candidates:
        return None

    # Собираем текст кандидатов для промпта
    lines = []
    for i, cand in enumerate(candidates):
        lines.append(
            f"{i}. Вопрос из базы: {cand['question']}\n"
            f"   Ответ из базы: {cand['answer']}\n"
            f"   Базовый score: {cand.get('score', 0):.3f}"
        )
    candidates_block = "\n\n".join(lines)

    system_prompt = (
        "Ты помогаешь оператору поддержки сети магазинов Воблабир. "
        "Тебе даётся вопрос пользователя и несколько похожих вопросов из базы "
        "с ответами менеджеров.\n\n"
        "Твоя задача:\n"
        "1) Понять, насколько каждый вариант действительно отвечает на вопрос пользователя.\n"
        "2) Если есть подходящий вариант, выбрать ОДИН лучший.\n"
        "3) Если ни один вариант не подходит по смыслу, нужно явно указать, "
        "что подходящего варианта нет.\n\n"
        "Очень важно: ты НЕ придумываешь новый ответ, а только выбираешь лучший "
        "из предложенных вариантов."
    )

    user_prompt = (
        f"Вопрос пользователя:\n{user_question}\n\n"
        f"Кандидаты из базы:\n{candidates_block}\n\n"
        "Ответь строго в формате:\n"
        "- Если один из вариантов подходит, напиши только его номер (0, 1, 2, ...), без комментариев.\n"
        "- Если ни один вариант не подходит, напиши слово NONE.\n"
    )

    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0,
    )

    content = resp.choices[0].message.content.strip()
    upper = content.upper()

    if upper.startswith("NONE"):
        return None

    # Пытаемся разобрать номер кандидата
    try:
        idx = int(content.split()[0])
    except ValueError:
        return None

    if 0 <= idx < len(candidates):
        return candidates[idx]

    return None