"""Вложения голоса/аудио во входящих сообщениях MAX."""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.core.types import IncomingMessage
from app.platforms.max.adapter import message_attachments_from_incoming

MAX_VOICE_ATTACHMENT_TYPES = frozenset({"voice", "audio", "sound"})


def mime_from_max_voice_attachment(att: Dict[str, Any]) -> str:
    for k in ("mime_type", "media_type", "content_type", "mimetype"):
        v = att.get(k)
        if v and str(v).strip():
            return str(v).strip().lower()
    pl = att.get("payload")
    if isinstance(pl, dict):
        for k in ("mime_type", "media_type", "content_type"):
            v = pl.get(k)
            if v and str(v).strip():
                return str(v).strip().lower()
    return "audio/ogg"


def max_voice_attachment_declared_size(att: Dict[str, Any]) -> Optional[int]:
    for k in ("file_size", "size", "bytes"):
        v = att.get(k)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    pl = att.get("payload")
    if isinstance(pl, dict):
        for k in ("file_size", "size", "bytes"):
            v = pl.get(k)
            if v is not None:
                try:
                    return int(v)
                except (TypeError, ValueError):
                    pass
    return None


def first_max_voice_attachment(event: IncomingMessage) -> Optional[Dict[str, Any]]:
    atts: list[dict[str, Any]] = message_attachments_from_incoming(event)
    if not atts:
        raw = list(event.attachments or [])
        atts = [x for x in raw if isinstance(x, dict)]
    for a in atts:
        t = str(a.get("type", "")).lower()
        if t in MAX_VOICE_ATTACHMENT_TYPES:
            return a
    return None
