import asyncio
from datetime import datetime, timezone

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.config import BOT_TOKEN, MANAGER_CHAT_ID
from app.services.report_service import build_monthly_report


def prev_month(y: int, m: int) -> tuple[int, int]:
    if m == 1:
        return (y - 1, 12)
    return (y, m - 1)


async def main():
    now = datetime.now(timezone.utc)
    y, m = prev_month(now.year, now.month)

    text = build_monthly_report(y, m)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    try:
        await bot.send_message(
            chat_id=int(MANAGER_CHAT_ID),
            text=text,
            disable_web_page_preview=True,
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())