import asyncio
from datetime import datetime, timezone, timedelta

from aiogram import Bot
from aiogram.enums import ParseMode

from app.config import BOT_TOKEN, MANAGER_CHAT_ID
from app.services.report_service import build_daily_report


async def main():
    # отчёт за вчера (UTC) — если хочешь МСК, скажи, переключим
    target = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    text = build_daily_report(target)

    bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
    try:
        await bot.send_message(chat_id=int(MANAGER_CHAT_ID), text=text, disable_web_page_preview=True)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

