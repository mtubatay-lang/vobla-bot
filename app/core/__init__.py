"""
Платформо-независимое ядро: типы, транспорт, callbacks.
"""

from app.core.types import (
    CallbackEvent,
    IncomingMessage,
    InternalChat,
    InternalUser,
    KeyboardButton,
    KeyboardRow,
    MediaAttachment,
    OutgoingMessage,
    Platform,
    Recipient,
)
from app.core.transport import MessengerAdapter
from app.core.callbacks import (
    START_AUTH,
    QA_START,
    QA_EXIT,
    FB_HELPED_PREFIX,
    FB_COMP_PREFIX,
    FB_CLARITY_PREFIX,
    FB_SKIP_COMMENT,
    BROADCAST_START,
    KB_ADD,
    KB_CONFIRM_ADD_PREFIX,
    KB_EDIT_ANSWER_PREFIX,
    MGR_REPLY_PREFIX,
)

__all__ = [
    "CallbackEvent",
    "IncomingMessage",
    "InternalChat",
    "InternalUser",
    "KeyboardButton",
    "KeyboardRow",
    "MediaAttachment",
    "OutgoingMessage",
    "Platform",
    "Recipient",
    "MessengerAdapter",
    "START_AUTH",
    "QA_START",
    "QA_EXIT",
    "FB_HELPED_PREFIX",
    "FB_COMP_PREFIX",
    "FB_CLARITY_PREFIX",
    "FB_SKIP_COMMENT",
    "BROADCAST_START",
    "KB_ADD",
    "KB_CONFIRM_ADD_PREFIX",
    "KB_EDIT_ANSWER_PREFIX",
    "MGR_REPLY_PREFIX",
]
