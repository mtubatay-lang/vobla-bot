import calendar
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.services.metrics_service import read_events_by_dates


# -----------------------------
# НОРМАЛИЗАЦИЯ СЕМЕЙСТВ СОБЫТИЙ
# -----------------------------

# Создание тикета (эскалация к менеджеру)
TICKET_CREATE_EVENTS = {
    "ticket_created",
    "pending_ticket_created",
}

# Вопрос задан пользователем (внутри FAQ/QA навыка)
QUESTION_EVENTS = {
    "faq_question_submitted",
    "qa_question_submitted",  # на будущее/если появится
}

# Показали автоответ из базы (FAQ)
FAQ_SHOWN_EVENTS = {
    "faq_answer_shown",
}

# Ответ менеджера записан/отправлен пользователю
ANSWER_EVENTS = {
    "pending_answer_written",
}

# События, по которым считаем активных пользователей (DAU/MAU)
# Важно: НЕ включаем manager_reply_click (там user_id = менеджер).
ACTIVE_EVENTS = (
    QUESTION_EVENTS
    | FAQ_SHOWN_EVENTS
    | TICKET_CREATE_EVENTS
    | {
        "faq_mode_enter",
        "qa_mode_enter",
        "faq_answer_not_found",
        "faq_not_helpful_escalated",
        "start_authorized",
        "auth_success",
    }
)


def _parse_iso_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _count(events: List[Dict[str, Any]], names: set[str]) -> int:
    return sum(1 for e in events if e.get("event") in names)


def _count_unique_ticket_ids(events: List[Dict[str, Any]], names: set[str]) -> int:
    """Счётчик по уникальным ticket_id (чтобы не было дублей по одному тикету)."""
    ids = set()
    for e in events:
        if e.get("event") not in names:
            continue
        meta = e.get("meta") or {}
        tid = str(meta.get("ticket_id") or "").strip()
        if tid:
            ids.add(tid)
    return len(ids)


def _uniq_active_users(events: List[Dict[str, Any]]) -> int:
    users = set()
    for e in events:
        if e.get("event") in ACTIVE_EVENTS:
            uid = str(e.get("user_id") or "").strip()
            if uid:
                users.add(uid)
    return len(users)


def _response_times_minutes(events: List[Dict[str, Any]]) -> List[int]:
    """
    Считаем время ответа по ticket_id:
    ticket_created/pending_ticket_created(ts) -> pending_answer_written(ts)
    """
    created: Dict[str, datetime] = {}
    answered: Dict[str, datetime] = {}

    for e in events:
        ev = e.get("event")
        meta = e.get("meta") or {}
        ticket_id = str(meta.get("ticket_id") or "").strip()
        if not ticket_id:
            continue

        ts = _parse_iso_ts(e.get("ts") or "")
        if not ts:
            continue

        if ev in TICKET_CREATE_EVENTS:
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

    dau = _uniq_active_users(events)

    # Вопросы считаем по факту "вопрос задан", а не по тикетам
    questions = _count(events, QUESTION_EVENTS)

    # Показали автоответы из базы
    faq_shown = _count(events, FAQ_SHOWN_EVENTS)

    # Эскалация к менеджеру = создание тикета (уникальные ticket_id)
    escalated = _count_unique_ticket_ids(events, TICKET_CREATE_EVENTS)

    # Ответы менеджера = уникальные ticket_id с pending_answer_written
    answered = _count_unique_ticket_ids(events, ANSWER_EVENTS)

    times = _response_times_minutes(events)
    avg = int(sum(times) / len(times)) if times else None
    med = _median(times)

    # В работе = тикеты без ответа
    open_estimate = max(escalated - answered, 0)

    lines = [
        f"📊 <b>Ежедневный отчёт</b> — <b>{d}</b>",
        "",
        f"👥 Активные пользователи (DAU): <b>{dau}</b>",
        f"❓ Новых вопросов: <b>{questions}</b>",
        f"🤖 Автоответов из FAQ показано: <b>{faq_shown}</b>",
        f"🧑‍💼 Ушли к менеджеру после FAQ: <b>{escalated}</b>",
        f"✅ Ответов менеджера: <b>{answered}</b>",
        f"🕒 В работе (оценка): <b>{open_estimate}</b>",
    ]

    if times:
        lines.append(f"⏱ Время ответа: среднее <b>{avg} мин</b>, медиана <b>{med} мин</b>")
    else:
        lines.append("⏱ Время ответа: <i>н/д</i> (нужны ticket_id в событиях тикета/ответа)")

    lines.append("")
    lines.append("🧩 Примечание: DAU считается по событиям входа/вопросов/показов FAQ/создания тикетов (без manager_reply_click).")

    return "\n".join(lines)


def build_monthly_report(year: int, month: int) -> str:
    last_day = calendar.monthrange(year, month)[1]
    start = date(year, month, 1)
    end = date(year, month, last_day)

    events = read_events_by_dates(start.isoformat(), end.isoformat())

    mau = _uniq_active_users(events)
    questions = _count(events, QUESTION_EVENTS)
    faq_shown = _count(events, FAQ_SHOWN_EVENTS)
    escalated = _count_unique_ticket_ids(events, TICKET_CREATE_EVENTS)
    answered = _count_unique_ticket_ids(events, ANSWER_EVENTS)

    times = _response_times_minutes(events)
    avg = int(sum(times) / len(times)) if times else None
    med = _median(times)

    title = f"{start.strftime('%m.%Y')}"

    lines = [
        f"📈 <b>Месячный отчёт</b> — <b>{title}</b>",
        "",
        f"👥 Активные пользователи (MAU): <b>{mau}</b>",
        f"❓ Вопросов за месяц: <b>{questions}</b>",
        f"🤖 Автоответов из FAQ показано: <b>{faq_shown}</b>",
        f"🧑‍💼 Ушли к менеджеру после FAQ: <b>{escalated}</b>",
        f"✅ Ответов менеджера: <b>{answered}</b>",
    ]

    if times:
        lines.append(f"⏱ Время ответа: среднее <b>{avg} мин</b>, медиана <b>{med} мин</b>")
    else:
        lines.append("⏱ Время ответа: <i>н/д</i> (нужны ticket_id в событиях тикета/ответа)")

    lines.append("")
    lines.append("🧩 Примечание: MAU считается по событиям входа/вопросов/показов FAQ/создания тикетов (без manager_reply_click).")

    return "\n".join(lines)