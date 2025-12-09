"""Клиент для работы с OpenAI (ChatGPT + embeddings)."""

from typing import List

from openai import OpenAI

from app.config import OPENAI_API_KEY, OPENAI_MODEL, OPENAI_EMBEDDING_MODEL

client = OpenAI(api_key=OPENAI_API_KEY)


def create_embedding(text: str) -> List[float]:
    """Создаёт embedding для текста (синхронно)."""
    response = client.embeddings.create(
        model=OPENAI_EMBEDDING_MODEL,
        input=text,
    )
    return response.data[0].embedding


def adapt_faq_answer(user_question: str, base_answer: str) -> str:
    """Адаптирует ответ менеджера под живой диалог, не меняя сути."""
    system_prompt = (
        "Ты помощник сети магазинов разливных напитков «Воблабир».\n"
        "Отвечай франчайзи дружелюбно, но строго по регламенту.\n"
        "НЕЛЬЗЯ менять условия, скидки, правила, цены и формальные требования.\n"
        "Можно перефразировать, структурировать, добавлять вежливые вводные,\n"
        "но суть ответа и все условия должны остаться такими же."
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                "Вопрос франчайзи:\n"
                f"{user_question}\n\n"
                "Эталонный ответ менеджера (его смысл менять нельзя):\n"
                f"{base_answer}\n\n"
                "Сделай из этого аккуратный ответ для франчайзи. "
                "Если в ответе есть списки/шаги — сохрани их."
            ),
        },
    ]

    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        temperature=0.2,
    )

    return response.choices[0].message.content.strip()

