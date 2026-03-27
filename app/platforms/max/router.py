"""
Маршрутизация действий MAX (команды + callback_data).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from app.core.callbacks import (
    MAX_MENU_ADMIN,
    MAX_MENU_HELP,
    MAX_MENU_KILBIL,
    MAX_MENU_LOGIN,
    QA_EXIT,
    QA_START,
    START_AUTH,
)
from app.core.handlers import (
    _pending_auth_uid,
    handle_auth_code,
    handle_help,
    handle_start,
    handle_start_auth_callback,
    is_pending_auth,
)
from app.config import VOICE_STRUCTURE_OUTPUT, VOICE_TO_TEXT_ENABLED, WHISPER_MAX_FILE_BYTES
from app.core.types import CallbackEvent, IncomingMessage, OutgoingMessage
from app.services.auth_service import find_user_by_platform_id
from app.services.voice_transcription_service import structure_transcript, transcribe_audio_bytes
from app.services.broadcast_recipients_service import upsert_chat_recipient, upsert_user_recipient
from app.services.kilbil_service import find_kilbil_answer
from app.services.max_qa_simple import max_simple_rag_answer
from app.platforms.max import admin_flows
from app.platforms.max.adapter import _extract_chat_title_from_peer
from app.platforms.max.voice_attachment import (
    first_max_voice_attachment,
    max_voice_attachment_declared_size,
    mime_from_max_voice_attachment,
)
from app.platforms.max.client import MaxApiClientError
from app.ui.keyboards import max_main_menu_rows, qa_kb_rows

logger = logging.getLogger(__name__)

# Видимая обратная связь в MAX: индикатор набора часто не отображается в клиенте.
MAX_QA_SEARCHING_TEXT = "🔍 Ищу в базе знаний…"
MAX_KILBIL_SEARCHING_TEXT = "🔍 Ищу в базе Kilbil…"

_MAX_VTT_CONVERTING = "Преобразую в текст."
_MAX_VTT_ERROR = "Не удалось преобразовать голос в текст. Попробуйте ещё раз."
_MAX_VTT_TOO_LARGE = "Голосовое сообщение слишком длинное (лимит 25 МБ)."
_MAX_VTT_CHUNK_LEN = 4090


def _max_chat_id_for_sheets(chat_id: int | str) -> int | None:
    """Числовой chat_id для Google Sheets; None если не приводится к int."""
    if isinstance(chat_id, int):
        return chat_id
    s = str(chat_id).strip()
    try:
        return int(s)
    except ValueError:
        return None


def _max_group_chat_type_from_raw(raw: Any) -> str:
    """Тип чата для колонки chat_type (как в Telegram: group / supergroup / channel)."""
    if not isinstance(raw, dict):
        return "group"
    msg = raw.get("message")
    if not isinstance(msg, dict):
        return "group"
    rec = msg.get("recipient")
    if not isinstance(rec, dict):
        return "group"
    ct_flat = str(rec.get("chat_type") or "").lower()
    if ct_flat == "channel":
        return "channel"
    if ct_flat == "supergroup":
        return "supergroup"
    if ct_flat in ("chat", "group"):
        return "group"
    t = str(rec.get("type") or rec.get("recipient_type") or "").lower()
    nested = rec.get("chat") if isinstance(rec.get("chat"), dict) else {}
    nt = str(nested.get("type") or "").lower()
    for x in (t, nt):
        if x == "channel":
            return "channel"
        if x == "supergroup":
            return "supergroup"
        if x in ("chat", "group"):
            return "group"
    return "group"


def _schedule_collect_max_group_chat(
    event: IncomingMessage | CallbackEvent,
    max_client: Any = None,
) -> None:
    """
    Любое событие из группы MAX → upsert в recipients_chats (platform=max).
    Если в webhook нет названия — подгружаем GET /chats/{chatId} и повторный upsert.
    """
    if not event.chat.is_group or event.chat.platform != "max":
        return
    cid = _max_chat_id_for_sheets(event.chat.id)
    if cid is None:
        logger.debug("MAX collect chat: skip non-numeric chat_id=%r", event.chat.id)
        return
    import asyncio as _aio

    title = event.chat.title or ""
    username = event.chat.username
    chat_type = _max_group_chat_type_from_raw(getattr(event, "raw", None))
    logger.info(
        "MAX recipients_chats: upsert scheduled chat_id=%s title=%r platform=max",
        cid,
        title,
    )

    async def _run_collect() -> None:
        t = title or ""
        await _aio.to_thread(
            upsert_chat_recipient,
            cid,
            chat_type,
            t,
            username,
            platform="max",
        )
        if str(t).strip():
            return
        if max_client is None:
            logger.info(
                "MAX recipients_chats: title empty in webhook, no max_client for GET /chats/%s",
                cid,
            )
            return
        try:
            info = await max_client.get_group_chat(cid)
        except MaxApiClientError as e:
            logger.warning("MAX GET /chats/%s failed: %s", cid, e)
            return
        except Exception as e:
            logger.warning("MAX GET /chats/%s: %s", cid, e, exc_info=True)
            return
        if not isinstance(info, dict):
            return
        resolved = _extract_chat_title_from_peer({}, info)
        if not resolved:
            logger.info(
                "MAX GET /chats/%s: no title in response keys=%s",
                cid,
                sorted(info.keys()),
            )
            return
        await _aio.to_thread(
            upsert_chat_recipient,
            cid,
            chat_type,
            resolved,
            username,
            platform="max",
        )
        logger.info(
            "MAX recipients_chats: title updated via API chat_id=%s title=%r",
            cid,
            resolved,
        )

    _aio.create_task(_run_collect())


def _normalize_slash_command(text: str) -> str:
    t = (text or "").strip()
    if not t.startswith("/"):
        return t
    cmd = t.split(None, 1)[0]
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd


@dataclass
class MaxRouteResult:
    action: str
    authorized: bool
    role: str
    handled: bool


class MaxActionRouter:
    def __init__(self, max_client: Any = None) -> None:
        self._qa_sessions: set[int] = set()
        self._kilbil_sessions: set[int] = set()
        self._max_client = max_client

    def _user_int(self, user_id: int | str) -> int:
        return _pending_auth_uid(user_id)

    def _auth_context(self, user_id: int | str) -> tuple[bool, str]:
        user = find_user_by_platform_id("max", self._user_int(user_id))
        if not user:
            return False, ""
        return True, (user.role or "").strip().lower()

    async def _safe_answer_callback(self, adapter, callback_id: str | None) -> None:
        if not callback_id:
            return
        try:
            await adapter.answer_callback(str(callback_id))
        except Exception as e:
            logger.warning("MAX answer_callback failed: %s", e)

    async def _send_main_menu(self, adapter, event: IncomingMessage | CallbackEvent) -> None:
        ok, role = self._auth_context(event.user.id)
        rows = max_main_menu_rows(is_authorized=ok, is_admin=(role == "admin"))
        text = (
            "📋 Команды: /start /help /login /ask /kilbil /admin\n\n"
            "Или используйте кнопки ниже."
            if ok
            else "Сначала авторизуйтесь, затем будут доступны разделы."
        )
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=text,
                keyboard_rows=rows,
                is_group_chat=event.chat.is_group,
            )
        )

    async def _require_auth(self, adapter, event: IncomingMessage | CallbackEvent) -> bool:
        ok, role = self._auth_context(event.user.id)
        if ok:
            return True
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text="Сначала пройдите авторизацию: /start → кнопка «Авторизация».",
                keyboard_rows=max_main_menu_rows(is_authorized=False, is_admin=False),
                is_group_chat=event.chat.is_group,
            )
        )
        return False

    async def _require_admin(self, adapter, event: IncomingMessage | CallbackEvent) -> bool:
        ok, role = self._auth_context(event.user.id)
        if ok and role == "admin":
            return True
        if not ok:
            await self._require_auth(adapter, event)
            return False
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text="⛔ Раздел доступен только администраторам.",
                keyboard_rows=max_main_menu_rows(is_authorized=True, is_admin=False),
                is_group_chat=event.chat.is_group,
            )
        )
        return False

    async def _start_qa(self, adapter, event: IncomingMessage | CallbackEvent) -> bool:
        if not await self._require_auth(adapter, event):
            return False
        uid = self._user_int(event.user.id)
        self._qa_sessions.add(uid)
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=(
                    "🧠 <b>Навык: Ответы на вопросы</b>\n\n"
                    "Напишите вопрос одним сообщением.\n"
                    "Чтобы выйти — нажмите «✅ Завершить навык»."
                ),
                keyboard_rows=qa_kb_rows(),
                is_group_chat=event.chat.is_group,
            )
        )
        return True

    async def _exit_qa(self, adapter, event: IncomingMessage | CallbackEvent) -> bool:
        uid = self._user_int(event.user.id)
        self._qa_sessions.discard(uid)
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text="Вы вышли из режима «Задать вопрос».",
                keyboard_rows=max_main_menu_rows(
                    is_authorized=self._auth_context(event.user.id)[0],
                    is_admin=self._auth_context(event.user.id)[1] == "admin",
                ),
                is_group_chat=event.chat.is_group,
            )
        )
        return True

    async def _qa_turn(self, adapter, event: IncomingMessage) -> bool:
        uid = self._user_int(event.user.id)
        if uid not in self._qa_sessions:
            return False
        q = (event.text or "").strip()
        if not q:
            return True
        await adapter.send_typing(event.chat.id, is_group_chat=event.chat.is_group)
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=MAX_QA_SEARCHING_TEXT,
                keyboard_rows=qa_kb_rows(),
                is_group_chat=event.chat.is_group,
            )
        )
        user = find_user_by_platform_id("max", uid)
        user_name = user.name if user else (event.user.name or "друг")
        answer = await max_simple_rag_answer(q, user_name)
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=f"{answer}\n\nМожете задать ещё вопрос или нажать «✅ Завершить навык».",
                keyboard_rows=qa_kb_rows(),
                is_group_chat=event.chat.is_group,
            )
        )
        return True

    async def _enter_kilbil(self, adapter, event: IncomingMessage | CallbackEvent) -> bool:
        if not await self._require_auth(adapter, event):
            return False
        uid = self._user_int(event.user.id)
        self._kilbil_sessions.add(uid)
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=(
                    "🧾 Режим вопросов по Kilbil включён.\n"
                    "Напишите вопрос — отвечу по базе help.kilbil.ru"
                ),
                is_group_chat=event.chat.is_group,
            )
        )
        return True

    async def _kilbil_turn(self, adapter, event: IncomingMessage) -> bool:
        uid = self._user_int(event.user.id)
        if uid not in self._kilbil_sessions:
            return False
        self._kilbil_sessions.discard(uid)
        q = (event.text or "").strip()
        if not q:
            await adapter.send_message(
                OutgoingMessage(
                    chat_id=event.chat.id,
                    platform="max",
                    text="Напишите вопрос текстом 🙏",
                    is_group_chat=event.chat.is_group,
                )
            )
            return True
        await adapter.send_typing(event.chat.id, is_group_chat=event.chat.is_group)
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=MAX_KILBIL_SEARCHING_TEXT,
                is_group_chat=event.chat.is_group,
            )
        )
        match = await find_kilbil_answer(q)
        if not match:
            text = (
                "К сожалению, я не нашёл ответ на этот вопрос в базе знаний kilbil.\n"
                "Попробуйте переформулировать запрос."
            )
        else:
            text = match.get("answer", "")
            if match.get("url"):
                text += f"\n\n📎 Подробнее: {match['url']}"
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=text,
                keyboard_rows=max_main_menu_rows(
                    is_authorized=True,
                    is_admin=self._auth_context(event.user.id)[1] == "admin",
                ),
                is_group_chat=event.chat.is_group,
            )
        )
        return True

    async def _max_voice_to_text_turn(self, adapter, event: IncomingMessage) -> bool:
        """Голос/аудио → текст (как Telegram voice_to_text), до admin/QA."""
        if not VOICE_TO_TEXT_ENABLED:
            return False
        att = first_max_voice_attachment(event)
        if not att:
            return False
        fid = att.get("id_or_url") or att.get("file_id")
        if not fid:
            return False
        is_g = event.chat.is_group
        declared = max_voice_attachment_declared_size(att)
        if declared is not None and declared > WHISPER_MAX_FILE_BYTES:
            await adapter.send_message(
                OutgoingMessage(
                    chat_id=event.chat.id,
                    platform="max",
                    text=_MAX_VTT_TOO_LARGE,
                    is_group_chat=is_g,
                )
            )
            return True
        await adapter.send_message(
            OutgoingMessage(
                chat_id=event.chat.id,
                platform="max",
                text=_MAX_VTT_CONVERTING,
                is_group_chat=is_g,
            )
        )
        try:
            raw = await adapter.download_file(str(fid))
        except Exception as e:
            logger.exception("[MAX_VOICE_TO_TEXT] download: %s", e)
            await adapter.send_message(
                OutgoingMessage(
                    chat_id=event.chat.id,
                    platform="max",
                    text=_MAX_VTT_ERROR,
                    is_group_chat=is_g,
                )
            )
            return True
        if len(raw) > WHISPER_MAX_FILE_BYTES:
            await adapter.send_message(
                OutgoingMessage(
                    chat_id=event.chat.id,
                    platform="max",
                    text=_MAX_VTT_TOO_LARGE,
                    is_group_chat=is_g,
                )
            )
            return True
        mime = mime_from_max_voice_attachment(att)
        try:
            text = await asyncio.to_thread(transcribe_audio_bytes, raw, mime)
        except Exception as e:
            logger.exception("[MAX_VOICE_TO_TEXT] transcribe: %s", e)
            await adapter.send_message(
                OutgoingMessage(
                    chat_id=event.chat.id,
                    platform="max",
                    text=_MAX_VTT_ERROR,
                    is_group_chat=is_g,
                )
            )
            return True
        if not text:
            await adapter.send_message(
                OutgoingMessage(
                    chat_id=event.chat.id,
                    platform="max",
                    text="Текст не распознан.",
                    is_group_chat=is_g,
                )
            )
            return True
        if VOICE_STRUCTURE_OUTPUT:
            try:
                text = await asyncio.to_thread(structure_transcript, text)
            except Exception as e:
                logger.warning("[MAX_VOICE_TO_TEXT] structure: %s", e)
        for i in range(0, len(text), _MAX_VTT_CHUNK_LEN):
            chunk = text[i : i + _MAX_VTT_CHUNK_LEN]
            await adapter.send_message(
                OutgoingMessage(
                    chat_id=event.chat.id,
                    platform="max",
                    text=chunk,
                    is_group_chat=is_g,
                )
            )
        return True

    async def route(self, adapter, event: IncomingMessage | CallbackEvent) -> MaxRouteResult:
        update_type = None
        if getattr(event, "raw", None) and isinstance(event.raw, dict):
            update_type = event.raw.get("update_type") or event.raw.get("type")
        _schedule_collect_max_group_chat(event, self._max_client)
        authorized, role = self._auth_context(event.user.id)

        if isinstance(event, CallbackEvent):
            await self._safe_answer_callback(adapter, event.callback_id)
            data = (event.data or "").strip()
            if await admin_flows.handle_admin_callback(adapter, self._max_client, event, self._user_int(event.user.id)):
                result = MaxRouteResult(f"admin_cb:{data}", authorized, role, True)
                logger.info(
                    "MAX route trace update_type=%r action=%s authorized=%s role=%s result=%s",
                    update_type,
                    result.action,
                    result.authorized,
                    result.role or "-",
                    "handled" if result.handled else "ignored",
                )
                return result
            if data == START_AUTH:
                await handle_start_auth_callback(adapter, event)
                result = MaxRouteResult("start_auth", authorized, role, True)
            elif data == QA_START:
                ok = await self._start_qa(adapter, event)
                result = MaxRouteResult("qa_start", authorized, role, ok)
            elif data == QA_EXIT:
                ok = await self._exit_qa(adapter, event)
                result = MaxRouteResult("qa_exit", authorized, role, ok)
            elif data == MAX_MENU_HELP:
                await handle_help(adapter, IncomingMessage(user=event.user, chat=event.chat, text="/help", raw=event.raw))
                result = MaxRouteResult("menu_help", authorized, role, True)
            elif data == MAX_MENU_LOGIN:
                await handle_start_auth_callback(adapter, event)
                result = MaxRouteResult("menu_login", authorized, role, True)
            elif data == MAX_MENU_KILBIL:
                ok = await self._enter_kilbil(adapter, event)
                result = MaxRouteResult("menu_kilbil", authorized, role, ok)
            elif data == MAX_MENU_ADMIN:
                ok = await self._require_admin(adapter, event)
                if ok:
                    await admin_flows.show_admin_menu(adapter, event)
                result = MaxRouteResult("menu_admin", authorized, role, ok)
            else:
                result = MaxRouteResult(f"callback:{data}", authorized, role, False)
            logger.info(
                "MAX route trace update_type=%r action=%s authorized=%s role=%s result=%s",
                update_type,
                result.action,
                result.authorized,
                result.role or "-",
                "handled" if result.handled else "ignored",
            )
            return result

        text = (event.text or "").strip()
        norm = _normalize_slash_command(text)
        uid = self._user_int(event.user.id)
        handled = False
        action = "message"

        if norm == "/start":
            self._qa_sessions.discard(uid)
            self._kilbil_sessions.discard(uid)
            await handle_start(adapter, event)
            handled, action = True, "cmd_start"
            if self._auth_context(event.user.id)[0]:
                import asyncio as _aio

                fn = event.user.full_name or event.user.name
                _aio.create_task(
                    _aio.to_thread(
                        upsert_user_recipient,
                        uid,
                        event.user.username,
                        fn,
                        platform="max",
                    )
                )
        elif norm == "/help":
            await handle_help(adapter, event)
            handled, action = True, "cmd_help"
        elif norm == "/login":
            fake = CallbackEvent(user=event.user, chat=event.chat, data=START_AUTH, raw=event.raw)
            await handle_start_auth_callback(adapter, fake)
            handled, action = True, "cmd_login"
        elif norm == "/ask":
            handled = await self._start_qa(adapter, event)
            action = "cmd_ask"
        elif norm == "/kilbil":
            handled = await self._enter_kilbil(adapter, event)
            action = "cmd_kilbil"
        elif norm == "/admin":
            ok = await self._require_admin(adapter, event)
            if ok:
                await admin_flows.show_admin_menu(adapter, event)
            handled, action = ok, "cmd_admin"
        elif await self._max_voice_to_text_turn(adapter, event):
            handled, action = True, "voice_to_text"
        elif await admin_flows.handle_admin_message(adapter, self._max_client, event, uid):
            handled, action = True, "admin_msg"
        elif event.attachments and await admin_flows.handle_admin_doc_attachment(adapter, event, uid):
            handled, action = True, "admin_doc"
        elif is_pending_auth("max", event.user.id):
            await handle_auth_code(adapter, event)
            handled, action = True, "auth_code"
        elif await self._kilbil_turn(adapter, event):
            handled, action = True, "kilbil_turn"
        elif await self._qa_turn(adapter, event):
            handled, action = True, "qa_turn"

        result = MaxRouteResult(action, authorized, role, handled)
        logger.info(
            "MAX route trace update_type=%r action=%s authorized=%s role=%s result=%s",
            update_type,
            result.action,
            result.authorized,
            result.role or "-",
            "handled" if result.handled else "ignored",
        )
        return result
