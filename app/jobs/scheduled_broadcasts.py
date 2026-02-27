"""Job: выполнение плановых рассылок (запускать по cron каждые 15–60 мин).

Использует execute_broadcast(bot, ...), который внутри вызывает execute_broadcast_multi
с Telegram-адаптером. Получатели берутся из Sheets (пока только platform=telegram).
При появлении получателей MAX их можно будет включить в рассылку через get_broadcast_recipients_list.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.config import BOT_TOKEN, STATS_SHEET_ID
from app.services.broadcast_service import execute_broadcast
from app.services.scheduled_broadcast_service import (
    compute_next_run_iso_after_execution,
    read_active_due_scheduled_broadcasts,
    update_scheduled_broadcast_after_run,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    if not STATS_SHEET_ID:
        logger.warning("[SCHEDULED_BROADCASTS] STATS_SHEET_ID не задан, пропуск")
        return

    due = read_active_due_scheduled_broadcasts()
    if not due:
        logger.info("[SCHEDULED_BROADCASTS] Нет плановых рассылок к выполнению")
        return

    logger.info("[SCHEDULED_BROADCASTS] К выполнению: %s рассылок", len(due))
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    try:
        for record in due:
            schedule_id = record.get("schedule_id", "")
            text_final = record.get("text_final", "")
            media_json = record.get("media_json", "")
            mode = record.get("mode", "chats")
            mode_extra_str = record.get("mode_extra", "{}")
            schedule_type = record.get("schedule_type", "weekly")
            schedule_config_str = record.get("schedule_config", "{}")

            try:
                mode_extra = json.loads(mode_extra_str) if mode_extra_str else {}
            except json.JSONDecodeError:
                mode_extra = {}
            try:
                schedule_config = json.loads(schedule_config_str) if schedule_config_str else {}
            except json.JSONDecodeError:
                schedule_config = {}

            try:
                await execute_broadcast(
                    bot,
                    text_final=text_final,
                    media_json=media_json,
                    mode=mode,
                    mode_extra=mode_extra,
                    created_by_user_id=0,
                    created_by_username=None,
                    text_original=text_final,
                    selected_variant="original",
                )
            except Exception as e:
                logger.exception("[SCHEDULED_BROADCASTS] Ошибка выполнения рассылки %s: %s", schedule_id, e)
                continue

            next_run_iso = compute_next_run_iso_after_execution(schedule_type, schedule_config)
            last_run_iso = _utc_now_iso()
            update_scheduled_broadcast_after_run(schedule_id, next_run_iso, last_run_iso)
            logger.info("[SCHEDULED_BROADCASTS] Выполнено: %s, следующий запуск: %s", schedule_id, next_run_iso)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
