"""Клиент OpenAI для эмбеддингов и работы с ответами FAQ."""

import asyncio
import json
import os
import re
from typing import List, Dict, Optional

from openai import OpenAI

from app.config import (
    OPENAI_API_KEY,
    OPENAI_TIMEOUT,
    OPENAI_MODEL,
    OPENAI_EMBEDDING_MODEL,
)

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY не задан в переменных окружения")

client = OpenAI(api_key=OPENAI_API_KEY, timeout=OPENAI_TIMEOUT)


# -----------------------------
#     ЭМБЕДДИНГИ
# -----------------------------

EMBEDDING_MODEL = OPENAI_EMBEDDING_MODEL
CHAT_MODEL = OPENAI_MODEL


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

    Важно: ИИ НЕ должен менять суть / фактуру ответа, только форму и форматирование.
    Делает текст более дружелюбным, структурированным, с абзацами и лёгкими эмодзи.
    """
    # На всякий случай небольшой fallback
    if not base_answer:
        return "У меня пока нет готового ответа на этот вопрос. Менеджер свяжется с вами и уточнит детали 🙏"

    system_prompt = (
        "Ты помощник корпоративного бота сети магазинов Воблабир.\n"
        "Твоя задача — аккуратно адаптировать готовый ответ менеджера "
        "под формулировку вопроса пользователя.\n\n"
        "Правила:\n"
        "1. НЕ меняй фактический смысл и условия ответа. Никаких новых акций, цен или правил.\n"
        "2. Сделай текст структурированным и читаемым:\n"
        "   • разбивай на абзацы по смыслу;\n"
        "   • используй маркированные списки, если есть перечисления, шаги или варианты.\n"
        "3. Добавь немного дружелюбных эмодзи по смыслу (3–5 на весь ответ, не больше).\n"
        "4. Пиши простым, человеческим языком, без канцелярита.\n"
        "5. Если в исходном тексте есть формулировки, которые звучат сухо или агрессивно — "
        "смягчи тон, но не меняй содержание.\n"
        "6. Не добавляй ссылок, если их не было в исходном ответе.\n"
        "7. Отвечай только текстом финального ответа, без комментариев и пояснений от себя."
    )

    user_prompt = (
        "Вот вопрос пользователя (нужно учесть контекст, но не подстраивать условия):\n"
        f"{user_question}\n\n"
        "Вот готовый ответ менеджера из базы. Его смысл менять НЕЛЬЗЯ, "
        "можно только улучшить структуру и подачу:\n"
        f"{base_answer}\n\n"
        "Перепиши этот ответ по правилам выше. Сохрани все важные условия, цифры и ограничения, "
        "но сделай текст более понятным, дружелюбным и аккуратно оформленным."
    )

    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.25,
    )
    content = resp.choices[0].message.content or ""
    return content.strip()


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

    content = (resp.choices[0].message.content or "").strip()
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


# -----------------------------
#      ПОЛИРОВКА ОТВЕТА
# -----------------------------

def _strip_greeting(text: str) -> str:
    """Убирает приветствие из начала текста, если оно там есть."""
    return re.sub(
        r"^(Здравствуйте|Добрый день|Привет)[!,.]?\s*",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )


def polish_faq_answer(
    user_question: str,
    raw_answer: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> str:
    """
    Делает текст ответа более читаемым, не меняя факты.
    history: [{"role": "user"/"assistant", "text": "..."}]
    """
    history = history or []

    # Соберём контекст коротко (последние 6 сообщений)
    ctx_lines = []
    for h in history[-6:]:
        role = "Пользователь" if h.get("role") == "user" else "Бот"
        txt = (h.get("text") or "").strip()
        if txt:
            ctx_lines.append(f"{role}: {txt}")
    ctx = "\n".join(ctx_lines).strip()

    system = (
        "Ты редактор ответов службы поддержки. "
        "Тебе дают исходный ответ из базы знаний. "
        "Твоя задача — улучшить читаемость и подачу, НЕ меняя факты.\n\n"
        "ЖЁСТКИЕ ПРАВИЛА:\n"
        "1) Используй ТОЛЬКО информацию из исходного ответа.\n"
        "2) НЕ добавляй новых фактов, условий, цифр, дат, адресов, ссылок.\n"
        "3) НЕ исправляй факты. Если в исходном ответе есть странности — оставь как есть.\n"
        "4) Сохраняй все числа, даты, время, названия без изменений.\n"
        "5) Можно: переформулировать, убрать канцелярит, разбить на абзацы/списки, добавить эмодзи.\n"
        "6) Адаптируй под вопрос пользователя и контекст диалога (тон/обращение), но без новых фактов.\n"
        "7) НЕ начинай ответ с приветствия, если это не первое сообщение в диалоге.\n"
        "8) Если пользователь не здоровался — не здоровайся.\n"
    )

    user = (
        f"Вопрос пользователя:\n{user_question}\n\n"
        f"Контекст последних сообщений (если есть):\n{ctx if ctx else '—'}\n\n"
        "Исходный ответ из базы знаний (факты НЕ менять):\n"
        f"\"\"\"\n{raw_answer}\n\"\"\"\n\n"
        "Сделай итоговый ответ:\n"
        "- дружелюбно\n"
        "- структурировано (абзацы, списки)\n"
        "- с уместными эмодзи\n"
        "- без добавления новых фактов\n"
    )

    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )
    out = (resp.choices[0].message.content or "").strip()
    
    # Убираем приветствие, если это не первое сообщение в диалоге
    if history and any(h.get("role") == "assistant" for h in history):
        out = _strip_greeting(out)
    
    return out


# -----------------------------
#   ПРОВЕРКА GROUNDING ОТВЕТА RAG
# -----------------------------


def check_answer_grounding_sync(answer: str, chunks_text: str) -> bool:
    """Проверяет, что в ответе нет утверждений, которых нет в фрагментах. Возвращает True если ответ обоснован фрагментами."""
    if not answer or not chunks_text or len(chunks_text.strip()) < 50:
        return True
    prompt = (
        f"Ответ модели:\n{answer[:1500]}\n\n"
        f"Фрагменты из базы знаний:\n{chunks_text[:3000]}\n\n"
        "Есть ли в ответе модели утверждения, факты или цифры, которых нет в приведённых фрагментах? "
        "Ответь только: да или нет."
    )
    try:
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Ты проверяешь, обоснован ли ответ модели фрагментами. Ответь только «да» или «нет»."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=5,
        )
        out = (resp.choices[0].message.content or "").strip().lower()
        return "нет" in out or "no" in out
    except Exception:
        return True


async def check_answer_grounding(answer: str, chunks: List[Dict]) -> bool:
    """Асинхронная проверка grounding по списку чанков."""
    chunks_text = "\n\n---\n\n".join([c.get("text", "")[:800] for c in (chunks or [])])
    return await asyncio.to_thread(check_answer_grounding_sync, answer, chunks_text)


# -----------------------------
#   ОТВЕТ ПО ПОЛНОМУ ДОКУМЕНТУ (ВРЕМЕННАЯ ЗАМЕНА RAG)
# -----------------------------

def generate_answer_from_full_document(
    question: str,
    document: str,
    conversation_history: List[Dict],
    *,
    user_name: str = "пользователь",
    is_first_turn: bool = False,
) -> str:
    """
    Генерирует ответ на вопрос пользователя на основе полного документа в контексте.
    Используется при USE_FULL_FILE_CONTEXT вместо RAG по чанкам.
    """
    if not document or not document.strip():
        return "Извините, документ для ответа недоступен."
    history_lines = []
    for msg in conversation_history[-5:]:
        role = "Пользователь" if msg.get("role") == "user" else "Бот"
        text = msg.get("text", "")
        if text:
            history_lines.append(f"{role}: {text}")
    history_text = "\n".join(history_lines) if history_lines else ""
    if is_first_turn:
        greeting_instruction = (
            f"ВАЖНО: Это первое обращение пользователя в этой беседе. "
            f"Приветствуй его по имени ({user_name}) в начале ответа."
        )
    else:
        greeting_instruction = (
            "ВАЖНО: Это НЕ первое сообщение в диалоге. НЕ приветствуй пользователя заново."
        )
    system_prompt = (
        "Ты — AI-ассистент корпоративного бота «Воблабир». Общайся как живой менеджер поддержки: тепло, ясно, без канцелярита.\n\n"
        f"{greeting_instruction}\n\n"
        "Ниже приведён полный документ (база знаний). Отвечай на вопросы пользователя только на его основе. "
        "Не придумывай факты, цифры, сроки.\n"
        "Не задавай пользователю уточняющих вопросов (про город, тип магазина, факторы и т.п.). "
        "Отвечай сразу на основе документа: если в документе есть общие критерии или чек-лист по теме вопроса — приведи их; "
        "если информации нет — кратко скажи об этом и предложи передачу менеджеру.\n"
        "Стиль: короткие абзацы, списки где уместно, 1–3 эмодзи. Без длинных вступлений."
    )
    user_prompt = (
        f"{'Контекст диалога:\n' + history_text + '\n\n' if history_text else ''}"
        f"Документ:\n{document}\n\n"
        f"Вопрос пользователя: {question}\n\n"
        "Сформулируй ответ на основе документа."
    )
    try:
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        answer = resp.choices[0].message.content or "Извините, не могу сформировать ответ."
        return answer.strip()
    except Exception as e:
        import logging
        logging.getLogger(__name__).exception("Ошибка генерации ответа по полному документу: %s", e)
        return "Извините, произошла ошибка при формировании ответа."


# -----------------------------
#   УЛУЧШЕНИЕ ТЕКСТА РАССЫЛКИ
# -----------------------------

def improve_broadcast_text(text: str) -> Dict[str, str]:
    """
    Улучшает текст рассылки: исправляет орфографию/пунктуацию и предлагает улучшенный вариант.
    
    Возвращает:
    {
        "fixed": "...",      # минимальные правки орфографии/пунктуации
        "suggested": "..."   # улучшенный формат
    }
    
    Если text пустой — возвращает пустые строки.
    """
    if not text or not text.strip():
        return {"fixed": "", "suggested": ""}
    
    system_prompt = (
        "Ты помощник корпоративного бота сети магазинов Воблабир.\n"
        "Твоя задача — улучшить текст рассылки для партнёров.\n\n"
        "Правила:\n"
        "1. НЕ меняй фактический смысл и факты. Никаких новых акций, цен или правил.\n"
        "2. Исправь орфографию и пунктуацию.\n"
        "3. Сделай текст структурированным и читаемым:\n"
        "   • разбивай на абзацы по смыслу;\n"
        "   • используй маркированные списки, если есть перечисления, шаги или варианты.\n"
        "4. Эмодзи используй умеренно (2-4 на весь текст, не больше).\n"
        "5. Пиши простым, человеческим языком, без канцелярита.\n"
        "6. Не добавляй ссылок, если их не было в исходном тексте.\n\n"
        "Верни JSON с двумя полями:\n"
        "- \"fixed\": текст с минимальными правками орфографии/пунктуации\n"
        "- \"suggested\": улучшенный формат с структурой и абзацами\n"
        "Оба текста должны сохранять смысл оригинала."
    )
    
    user_prompt = (
        "Вот текст рассылки, который нужно улучшить:\n\n"
        f"{text}\n\n"
        "Верни JSON с полями \"fixed\" и \"suggested\"."
    )
    
    try:
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            response_format={"type": "json_object"},
        )
        
        result_text = (resp.choices[0].message.content or "").strip()
        if not result_text:
            return {"fixed": text, "suggested": text}
        result_json = json.loads(result_text)
        fixed = result_json.get("fixed", text)
        suggested = result_json.get("suggested", text)
        return {
            "fixed": fixed if fixed else text,
            "suggested": suggested if suggested else text,
        }
    except Exception:
        # При ошибке возвращаем оригинал
        return {"fixed": text, "suggested": text}


def generate_broadcast_title(text: str) -> str:
    """
    Генерирует короткое название рассылки (3–7 слов) по тексту для отображения в списке плановых рассылок.
    Возвращает обрезанную до 80 символов строку. При пустом text или ошибке — fallback.
    """
    FALLBACK = "Рассылка без текста"
    if not text or not text.strip():
        return FALLBACK
    system_prompt = (
        "Ты помощник корпоративного бота. По тексту рассылки сгенерируй короткое название (3–7 слов), "
        "отражающее суть. Ответь только названием, без кавычек и точки в конце. Язык — русский."
    )
    user_prompt = f"Текст рассылки:\n\n{text[:2000]}\n\nКороткое название:"
    try:
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        result = (resp.choices[0].message.content or "").strip().strip('."\'')
        if not result:
            return FALLBACK
        return result[:80]
    except Exception:
        return FALLBACK
