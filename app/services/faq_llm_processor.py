"""Обработка FAQ через LLM: дедупликация и улучшение формулировок."""

import asyncio
import json
import logging
import re
from typing import List, Dict, Any, Optional

from app.services.openai_client import client, CHAT_MODEL

logger = logging.getLogger(__name__)

BATCH_SIZE = 20


def _extract_json_array(text: str) -> List[Dict[str, Any]]:
    """Извлекает JSON-массив из ответа LLM (убирает markdown-обёртку при наличии)."""
    text = (text or "").strip()
    # Убираем обёртку ```json ... ```
    if "```" in text:
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
        if match:
            text = match.group(1).strip()
    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        return []


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """Извлекает JSON-объект из ответа LLM."""
    text = (text or "").strip()
    if "```" in text:
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
        if match:
            text = match.group(1).strip()
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


async def deduplicate_and_normalize_faq(
    rows: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """Проход 1: группирует семантически одинаковые вопросы, объединяет ответы.

    Args:
        rows: список из load_faq_rows(): [{"question", "answer", "media_json"}, ...].

    Returns:
        Список записей: canonical_question, question_variants, merged_answer, media_json.
    """
    if not rows:
        return []

    all_normalized: List[Dict[str, Any]] = []

    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        batch_text = "\n".join(
            f"{i+1}. Вопрос: {r.get('question', '').strip()}\n   Ответ: {r.get('answer', '').strip()[:500]}"
            for i, r in enumerate(batch)
        )

        system_prompt = (
            "Ты помощник для дедупликации FAQ. Дан список пар (вопрос, ответ). "
            "Сгруппируй по смыслу вопроса: в одну группу — разные формулировки одного и того же. "
            "Для каждой группы верни: один канонический вопрос, список всех формулировок вопросов, "
            "один объединённый ответ (если ответы различаются — объедини без потери смысла). "
            "Ответь ТОЛЬКО валидным JSON массивом объектов с полями: "
            "canonical_question (строка), question_variants (массив строк, все формулировки включая каноническую), "
            "merged_answer (строка). Не добавляй комментариев и лишнего текста."
        )
        user_prompt = (
            f"Список пар вопрос-ответ:\n{batch_text}\n\n"
            "Сгруппируй по смыслу и верни JSON массив объектов с полями canonical_question, question_variants, merged_answer."
        )

        try:
            resp = await asyncio.to_thread(
                client.chat.completions.create,
                model=CHAT_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                max_tokens=4000,
            )
            answer = (resp.choices[0].message.content or "").strip()
            groups = _extract_json_array(answer)
            if not groups:
                # LLM вернул пустой массив — трактуем каждую строку батча как отдельную запись
                for r in batch:
                    q = (r.get("question") or "").strip()
                    a = (r.get("answer") or "").strip()
                    if q and a:
                        all_normalized.append({
                            "canonical_question": q,
                            "question_variants": [q],
                            "merged_answer": a,
                            "media_json": (r.get("media_json") or "").strip(),
                        })
                continue

            for entry in groups:
                canonical = (entry.get("canonical_question") or "").strip()
                variants = entry.get("question_variants")
                if isinstance(variants, list):
                    variants = [str(v).strip() for v in variants if v]
                else:
                    variants = [canonical] if canonical else []
                if not variants:
                    variants = [canonical] if canonical else ["?"]
                merged = (entry.get("merged_answer") or "").strip()
                if not merged:
                    continue

                # Берём media_json из первой попавшей в группу строки (в этом батче)
                media_json = ""
                for r in batch:
                    q = (r.get("question") or "").strip()
                    if q in variants or (variants and (q in variants[0] or variants[0] in q)):
                        media_json = (r.get("media_json") or "").strip()
                        break

                all_normalized.append({
                    "canonical_question": canonical or variants[0],
                    "question_variants": variants,
                    "merged_answer": merged,
                    "media_json": media_json,
                })
        except Exception as e:
            logger.warning(f"[FAQ_LLM] Ошибка дедупликации батча {start}-{start+len(batch)}: {e}. Оставляю записи как есть.")
            for r in batch:
                q = (r.get("question") or "").strip()
                a = (r.get("answer") or "").strip()
                if q and a:
                    all_normalized.append({
                        "canonical_question": q,
                        "question_variants": [q],
                        "merged_answer": a,
                        "media_json": (r.get("media_json") or "").strip(),
                    })

    return all_normalized


async def improve_faq_entry_llm(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Проход 2: улучшает формулировку ответа и добавляет синонимы вопроса.

    Args:
        entry: запись после прохода 1 (canonical_question, question_variants, merged_answer, media_json).

    Returns:
        Та же запись с полями improved_answer, synonym_questions.
    """
    canonical = entry.get("canonical_question", "")
    variants = entry.get("question_variants", [])
    merged = entry.get("merged_answer", "")

    system_prompt = (
        "Ты помощник для улучшения FAQ. Дан вопрос(ы) и ответ. "
        "Задачи: (1) Улучши формулировку ответа: ясность, структура, без потери смысла. "
        "(2) Добавь 1–2 короткие синонимичные формулировки вопроса, если их ещё нет. "
        "Ответь ТОЛЬКО валидным JSON объектом с полями: "
        "improved_answer (строка), synonym_questions (массив строк, может быть пустым). "
        "Не добавляй комментариев и лишнего текста."
    )
    questions_text = ", ".join(variants[:5]) if variants else canonical
    user_prompt = (
        f"Вопрос(ы): {questions_text}\n\nОтвет: {merged[:2000]}\n\n"
        "Верни JSON: improved_answer, synonym_questions (массив строк)."
    )

    try:
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=2000,
        )
        answer = (resp.choices[0].message.content or "").strip()
        data = _extract_json_object(answer)
        if data:
            improved = (data.get("improved_answer") or "").strip() or merged
            synonyms = data.get("synonym_questions")
            if isinstance(synonyms, list):
                synonyms = [str(s).strip() for s in synonyms if s][:3]
            else:
                synonyms = []
            entry["improved_answer"] = improved
            entry["synonym_questions"] = synonyms
            return entry
    except Exception as e:
        logger.warning(f"[FAQ_LLM] Ошибка улучшения записи '{canonical[:50]}...': {e}")

    entry["improved_answer"] = merged
    entry["synonym_questions"] = []
    return entry
