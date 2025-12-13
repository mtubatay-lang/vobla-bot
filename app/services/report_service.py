import calendar
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.services.metrics_service import read_events_by_dates


# События, по которым считаем активных пользователей (чтобы не учитывать менеджеров)
ACTIVE_EVENTS = {
    "ticket_created",
    "faq_answer_shown",
    "faq_not_helpful_escalated",
}

# Для ответов менеджера (у тебя уже пишется из manager_reply.py)
ANSWER_EVENTS = {
    "pending_answer_written",
}

# Для автоответов из FAQ
FAQ_SHOWN_EVENTS = {"faq_answer_shown"}

# Ушли к менеджеру после FAQ
ESCALATE_EVENTS = {"faq_not_helpful_escalated"}


def _parse_iso_ts(ts: str) -> Optional[datetime]:
    try:
        # ожидаем '2025-12-13T10:12:34+00:00' или без зоны
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _uniq_active_users(events: List[Dict[str, Any]]) -> int:
    users = set()
    for e in events:
        if e.get("event") in ACTIVE_EVENTS:
            uid = (e.get("user_id") or "").strip()
            if uid:
                users.add(uid)
    return len(users)


def _count(events: List[Dict[str, Any]], names: set[str]) -> int:
    return sum(1 for e in events if e.get("event") in names)


def _response_times_minutes(events: List[Dict[str, Any]]) -> List[int]:
    """
    Считаем время ответа по ticket_id:
    ticket_created(ts) -> pending_answer_written(ts)

    Нужно, чтобы в meta у обоих событий был ticket_id.
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

        if ev == "ticket_created":
            # если несколько — берём самый ранний
            if ticket_id not in created or ts < created[ticket_id]:
                created[ticket_id] = ts

        if ev in ANSWER_EVENTS:
            # если несколько — берём самый ранний ответ
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
    questions = _count(events, {"ticket_created"})
    faq_shown = _count(events, FAQ_SHOWN_EVENTS)
    escalated = _count(events, ESCALATE_EVENTS)
    answered = _count(events, ANSWER_EVENTS)

    times = _response_times_minutes(events)
    avg = int(sum(times) / len(times)) if times else None
    med = _median(times)

    # простая sanity-метрика: сколько вопросов без ответа (по событиям)
    open_estimate = max(questions - answered, 0)

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
        lines.append("⏱ Время ответа: <i>н/д</i> (нужны ticket_id в ticket_created)")

    lines.append("")
    lines.append("🧩 Примечание: DAU считается по событиям ticket_created/faq_answer_shown/faq_not_helpful_escalated.")

    return "\n".join(lines)


def build_monthly_report(year: int, month: int) -> str:
    last_day = calendar.monthrange(year, month)[1]
    start = date(year, month, 1)
    end = date(year, month, last_day)

    events = read_events_by_dates(start.isoformat(), end.isoformat())

    mau = _uniq_active_users(events)
    questions = _count(events, {"ticket_created"})
    faq_shown = _count(events, FAQ_SHOWN_EVENTS)
    escalated = _count(events, ESCALATE_EVENTS)
    answered = _count(events, ANSWER_EVENTS)

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
        lines.append("⏱ Время ответа: <i>н/д</i> (нужны ticket_id в ticket_created)")

    lines.append("")
    lines.append("🧩 Примечание: MAU считается по событиям ticket_created/faq_answer_shown/faq_not_helpful_escalated.")

    return "\n".join(lines)

