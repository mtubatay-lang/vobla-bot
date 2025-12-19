import os
import logging
from aiogram import Router
from aiogram.types import CallbackQuery, Message
from aiogram.dispatcher.event.bases import SkipHandler

router = Router()
logger = logging.getLogger(__name__)

DEBUG_UPDATES = os.getenv("DEBUG_UPDATES", "0") == "1"


@router.callback_query()
async def dbg_cb(cb: CallbackQuery):
    if not DEBUG_UPDATES:
        raise SkipHandler()
    print(f"[DBG_CB] data={cb.data!r}")  # гарантированно видно в Railway
    raise SkipHandler()


@router.message()
async def dbg_msg(msg: Message):
    if not DEBUG_UPDATES:
        raise SkipHandler()
    print(f"[DBG_MSG] text={msg.text!r} type={msg.content_type}")  # на случай reply-кнопки
    raise SkipHandler()

