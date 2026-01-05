import calendar
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.services.metrics_service import read_events_by_dates
from app.services.pending_questions_read_service import read_pending_open_count
from app.services.qa_feedback_read_service import read_qa_feedback_by_dates


# --- Алиасы событий ---
EVENT_ALIASES: Dict[str, str] = {"pending_ticket_createc": "pending_ticket_created"}


def _ev_name(event: str) -> str:
    """Нормализует имя события (применяет алиасы)."""
    return EVENT_ALIASES.get(event, event)


# --- События для подсчёта ---
TICKET_CREATED_EVENTS = {"ticket_created", "pending_ticket_created"}  # алиас pending_ticket_createc применяется через _ev_name
ANSWER_EVENTS = {"pending_answer_written"}


def _parse_iso_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_meta(meta: Any) -> Dict[str, Any]:
    """Безопасно парсит meta (может быть строкой JSON или dict)."""
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str) and meta.strip():
        try:
            return json.loads(meta)
        except Exception:
            return {}
    return {}


def _count(events: List[Dict[str, Any]], names: set[str]) -> int:
    """Подсчитывает события по именам (с применением алиасов)."""
    normalized_names = {_ev_name(name) for name in names}
    return sum(1 for e in events if _ev_name(e.get("event", "")) in normalized_names)


def _uniq_users(events: List[Dict[str, Any]]) -> int:
    """Подсчитывает уникальных пользователей по всем событиям."""
    users = set()
    for e in events:
        uid = (e.get("user_id") or "").strip()
        if uid:
            users.add(uid)
    return len(users)


def _response_times_minutes(events: List[Dict[str, Any]]) -> List[int]:
    """
    Время ответа по ticket_id:
    ticket_created(ts) -> pending_answer_written(ts)

    Нужно, чтобы в meta у обоих событий был ticket_id.
    """
    created: Dict[str, datetime] = {}
    answered: Dict[str, datetime] = {}

    for e in events:
        ev = _ev_name(e.get("event", ""))
        meta = _parse_meta(e.get("meta"))

        ticket_id = str(meta.get("ticket_id") or "").strip()
        if not ticket_id:
            continue

        ts = _parse_iso_ts(e.get("ts") or "")
        if not ts:
            continue

        if ev in TICKET_CREATED_EVENTS:
            if ticket_id not in created or ts < created[ticket_id]:
                created[ticket_id] = ts

        if ev in ANSWER_EVENTS:
            if ticket_id not in answered or ts < answered[ticket_id]:
                answered[ticket_id] = ts

    deltas: List[int] = []
    for tid, cts in created.items():
        ats = answered.get(tid)
        if not ats:
            continue
        minutes = int((ats - cts).total_seconds() // 60)
        if minutes >= 0:
            deltas.append(minutes)

    deltas.sort()
    return deltas


def _median(values: List[int]) -> Optional[int]:
    if not values:
        return None
    n = len(values)
    mid = n // 2
    if n % 2 == 1:
        return values[mid]
    return (values[mid - 1] + values[mid]) // 2


def build_daily_report(target: date) -> str:
    d = target.isoformat()
    events = read_events_by_dates(d, d)

    # Нормализуем события (применяем алиасы)
    for e in events:
        e["event"] = _ev_name(e.get("event", ""))

    # Отладочный словарь событий (для диагностики, не выводится в отчёт по умолчанию)
    counts_by_event: Dict[str, int] = {}
    for e in events:
        ev_name = e.get("event", "")
        counts_by_event[ev_name] = counts_by_event.get(ev_name, 0) + 1
    # counts_by_event доступен для отладки (можно добавить print или логирование при необходимости)

    # === МЕТРИКА 1: Активные пользователи ===
    active_users = _uniq_users(events)

    # === МЕТРИКА 2: Всего вопросов задано ===
    # Если есть faq_question_submit - считаем по нему, иначе по outcome событиям
    questions_submit_count = _count(events, {"faq_question_submit"})
    questions_outcome_count = (
        _count(events, {"faq_answer_shown"})
        + _count(events, {"faq_answer_not_found"})
        + _count(events, {"faq_not_helpful_escalated"})
    )

    if questions_submit_count > 0:
        questions_total = questions_submit_count
    else:
        questions_total = questions_outcome_count

    # === МЕТРИКА 3: Ответил бот ===
    faq_answer_shown_count = _count(events, {"faq_answer_shown"})
    faq_not_helpful_escalated_count = _count(events, {"faq_not_helpful_escalated"})
    bot_answered = max(faq_answer_shown_count - faq_not_helpful_escalated_count, 0)

    # === МЕТРИКА 4: Ушли менеджерам ===
    to_managers = faq_not_helpful_escalated_count
    tickets_created = (
        _count(events, {"ticket_created"})
        + _count(events, {"pending_ticket_created"})
    )

    # === МЕТРИКА 5: Процент автоматических ответов ===
    auto_answer_rate = (bot_answered / questions_total * 100) if questions_total > 0 else 0.0

    # === МЕТРИКА 6: Ответы от менеджеров ===
    manager_answers = _count(events, {"pending_answer_written"})

    # === МЕТРИКА 7: В работе ===
    open_in_work = read_pending_open_count()

    # === МЕТРИКА 8: Время ответа ===
    times = _response_times_minutes(events)
    avg_time = int(sum(times) / len(times)) if times else None
    med_time = _median(times)

    # === МЕТРИКА 9-12: Оценки из qa_feedback ===
    feedback_rows = read_qa_feedback_by_dates(d, d)
    feedback_count = len(feedback_rows)

    # МЕТРИКА 10: Помог ли бот (%)
    helped_count = sum(1 for f in feedback_rows if f.get("helped") == "helped")
    partial_count = sum(1 for f in feedback_rows if f.get("helped") == "partial")
    not_helped_count = sum(1 for f in feedback_rows if f.get("helped") == "not_helped")
    total_feedback_with_helped = helped_count + partial_count + not_helped_count

    helped_pct = (helped_count / total_feedback_with_helped * 100) if total_feedback_with_helped > 0 else 0.0
    partial_pct = (partial_count / total_feedback_with_helped * 100) if total_feedback_with_helped > 0 else 0.0
    not_helped_pct = (not_helped_count / total_feedback_with_helped * 100) if total_feedback_with_helped > 0 else 0.0

    # МЕТРИКА 11: Средняя полнота
    completeness_values = [f.get("completeness") for f in feedback_rows if f.get("completeness") is not None]
    avg_completeness = (sum(completeness_values) / len(completeness_values)) if completeness_values else None

    # МЕТРИКА 12: Средняя понятность
    clarity_values = [f.get("clarity") for f in feedback_rows if f.get("clarity") is not None]
    avg_clarity = (sum(clarity_values) / len(clarity_values)) if clarity_values else None

    # === ФОРМИРОВАНИЕ ОТЧЁТА ===
    lines = [
        f"📊 <b>Ежедневный отчёт</b> — <b>{d}</b>",
        "",
        f"👥 Активные пользователи: <b>{active_users}</b>",
        "",
        f"❓ Всего вопросов задано: <b>{questions_total}</b>",
        f"🤖 Ответил бот: <b>{bot_answered}</b>",
        f"🧑‍💼 Ушли менеджерам: <b>{to_managers}</b>",
        f"📈 Процент автоматических ответов: <b>{auto_answer_rate:.1f}%</b>",
        "",
        f"✅ Ответы от менеджеров: <b>{manager_answers}</b>",
        f"🕒 В работе: <b>{open_in_work}</b>",
        "",
    ]

    if times:
        lines.append(f"⏱ Время ответа: среднее <b>{avg_time} мин</b>, медиана <b>{med_time} мин</b>")
    else:
        lines.append("⏱ Время ответа: <i>н/д</i>")

    lines.append("")
    lines.append(f"⭐️ Оценок получено: <b>{feedback_count}</b>")

    if total_feedback_with_helped > 0:
        lines.append(f"👍 Помог: <b>{helped_pct:.1f}%</b> | 🤝 Частично: <b>{partial_pct:.1f}%</b> | 👎 Не помог: <b>{not_helped_pct:.1f}%</b>")
    else:
        lines.append("👍 Помог: <i>н/д</i> | 🤝 Частично: <i>н/д</i> | 👎 Не помог: <i>н/д</i>")

    if avg_completeness is not None:
        lines.append(f"📚 Полнота: <b>{avg_completeness:.1f}/5</b>")
    else:
        lines.append("📚 Полнота: <i>н/д</i>")

    if avg_clarity is not None:
        lines.append(f"🧠 Понятность: <b>{avg_clarity:.1f}/5</b>")
    else:
        lines.append("🧠 Понятность: <i>н/д</i>")

    return "\n".join(lines)


def build_monthly_report(year: int, month: int) -> str:
    last_day = calendar.monthrange(year, month)[1]
    start = date(year, month, 1)
    end = date(year, month, last_day)

    events = read_events_by_dates(start.isoformat(), end.isoformat())

    # Нормализуем события (применяем алиасы)
    for e in events:
        e["event"] = _ev_name(e.get("event", ""))

    # === МЕТРИКА 1: Активные пользователи ===
    active_users = _uniq_users(events)

    # === МЕТРИКА 2: Всего вопросов задано ===
    questions_submit_count = _count(events, {"faq_question_submit"})
    questions_outcome_count = (
        _count(events, {"faq_answer_shown"})
        + _count(events, {"faq_answer_not_found"})
        + _count(events, {"faq_not_helpful_escalated"})
    )

    if questions_submit_count > 0:
        questions_total = questions_submit_count
    else:
        questions_total = questions_outcome_count

    # === МЕТРИКА 3: Ответил бот ===
    faq_answer_shown_count = _count(events, {"faq_answer_shown"})
    faq_not_helpful_escalated_count = _count(events, {"faq_not_helpful_escalated"})
    bot_answered = max(faq_answer_shown_count - faq_not_helpful_escalated_count, 0)

    # === МЕТРИКА 4: Ушли менеджерам ===
    to_managers = faq_not_helpful_escalated_count
    tickets_created = (
        _count(events, {"ticket_created"})
        + _count(events, {"pending_ticket_created"})
    )

    # === МЕТРИКА 5: Процент автоматических ответов ===
    auto_answer_rate = (bot_answered / questions_total * 100) if questions_total > 0 else 0.0

    # === МЕТРИКА 6: Ответы от менеджеров ===
    manager_answers = _count(events, {"pending_answer_written"})

    # === МЕТРИКА 8: Время ответа ===
    times = _response_times_minutes(events)
    avg_time = int(sum(times) / len(times)) if times else None
    med_time = _median(times)

    # === МЕТРИКА 9-12: Оценки из qa_feedback ===
    feedback_rows = read_qa_feedback_by_dates(start.isoformat(), end.isoformat())
    feedback_count = len(feedback_rows)

    helped_count = sum(1 for f in feedback_rows if f.get("helped") == "helped")
    partial_count = sum(1 for f in feedback_rows if f.get("helped") == "partial")
    not_helped_count = sum(1 for f in feedback_rows if f.get("helped") == "not_helped")
    total_feedback_with_helped = helped_count + partial_count + not_helped_count

    helped_pct = (helped_count / total_feedback_with_helped * 100) if total_feedback_with_helped > 0 else 0.0
    partial_pct = (partial_count / total_feedback_with_helped * 100) if total_feedback_with_helped > 0 else 0.0
    not_helped_pct = (not_helped_count / total_feedback_with_helped * 100) if total_feedback_with_helped > 0 else 0.0

    completeness_values = [f.get("completeness") for f in feedback_rows if f.get("completeness") is not None]
    avg_completeness = (sum(completeness_values) / len(completeness_values)) if completeness_values else None

    clarity_values = [f.get("clarity") for f in feedback_rows if f.get("clarity") is not None]
    avg_clarity = (sum(clarity_values) / len(clarity_values)) if clarity_values else None

    # === ФОРМИРОВАНИЕ ОТЧЁТА ===
    title = f"{start.strftime('%m.%Y')}"

    lines = [
        f"📈 <b>Месячный отчёт</b> — <b>{title}</b>",
        "",
        f"👥 Активные пользователи: <b>{active_users}</b>",
        "",
        f"❓ Всего вопросов задано: <b>{questions_total}</b>",
        f"🤖 Ответил бот: <b>{bot_answered}</b>",
        f"🧑‍💼 Ушли менеджерам: <b>{to_managers}</b>",
        f"📈 Процент автоматических ответов: <b>{auto_answer_rate:.1f}%</b>",
        "",
        f"✅ Ответы от менеджеров: <b>{manager_answers}</b>",
        "",
    ]

    if times:
        lines.append(f"⏱ Время ответа: среднее <b>{avg_time} мин</b>, медиана <b>{med_time} мин</b>")
    else:
        lines.append("⏱ Время ответа: <i>н/д</i>")

    lines.append("")
    lines.append(f"⭐️ Оценок получено: <b>{feedback_count}</b>")

    if total_feedback_with_helped > 0:
        lines.append(f"👍 Помог: <b>{helped_pct:.1f}%</b> | 🤝 Частично: <b>{partial_pct:.1f}%</b> | 👎 Не помог: <b>{not_helped_pct:.1f}%</b>")
    else:
        lines.append("👍 Помог: <i>н/д</i> | 🤝 Частично: <i>н/д</i> | 👎 Не помог: <i>н/д</i>")

    if avg_completeness is not None:
        lines.append(f"📚 Полнота: <b>{avg_completeness:.1f}/5</b>")
    else:
        lines.append("📚 Полнота: <i>н/д</i>")

    if avg_clarity is not None:
        lines.append(f"🧠 Понятность: <b>{avg_clarity:.1f}/5</b>")
    else:
        lines.append("🧠 Понятность: <i>н/д</i>")

    return "\n".join(lines)
