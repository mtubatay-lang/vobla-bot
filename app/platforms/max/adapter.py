"""
MAX-адаптер: преобразование внутренних типов ↔ MAX API, реализация MessengerAdapter.
Спецификация webhook Update: https://dev.max.ru/docs-api/objects/Update
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.core.types import (
    CallbackEvent,
    IncomingMessage,
    InternalChat,
    InternalUser,
    KeyboardButton,
    KeyboardRow,
    OutgoingMessage,
    Platform,
)

from app.platforms.max.client import MaxApiClient, MaxApiClientError

logger = logging.getLogger(__name__)


def _extract_https_download_url(att: Any, depth: int = 0) -> Optional[str]:
    """Прямая ссылка на файл во вложении webhook MAX (GET /files/{id} часто отсутствует в API)."""
    if depth > 6 or not isinstance(att, dict):
        return None
    for k in ("url", "download_url", "file_url", "audio_url", "voice_url", "media_url"):
        v = att.get(k)
        if isinstance(v, str) and v.startswith(("http://", "https://")):
            return v
    pl = att.get("payload")
    if isinstance(pl, dict):
        u = _extract_https_download_url(pl, depth + 1)
        if u:
            return u
    for sk in ("audio", "voice", "file", "image", "document"):
        sub = att.get(sk)
        if isinstance(sub, dict):
            u = _extract_https_download_url(sub, depth + 1)
            if u:
                return u
    return None


def _find_attachment_download_url_in_api_message(
    data: Dict[str, Any], file_id: Optional[str] = None
) -> Optional[str]:
    """Ищет https URL в теле сообщения из GET /messages/{id}."""
    msg = data.get("message") if isinstance(data.get("message"), dict) else data
    if not isinstance(msg, dict):
        return None
    body = msg.get("body")
    if not isinstance(body, dict):
        body = msg
    atts = body.get("attachments")
    if not isinstance(atts, list):
        return None
    pairs: list[tuple[str, str]] = []
    for att in atts:
        if not isinstance(att, dict):
            continue
        pl = att.get("payload") if isinstance(att.get("payload"), dict) else att
        if not isinstance(pl, dict):
            continue
        fid = str(pl.get("file_id") or pl.get("id") or "")
        for key in ("url", "download_url", "file_url"):
            u = pl.get(key)
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                pairs.append((fid, u))
    if not pairs:
        return None
    if file_id:
        for fid, u in pairs:
            if fid == str(file_id):
                return u
    return pairs[0][1]


def max_attachment_https_url(att: Any) -> Optional[str]:
    """Прямая HTTPS-ссылка на файл: нормализованное поле или рекурсивный разбор вложения."""
    if isinstance(att, dict):
        for k in ("download_url", "url"):
            u = att.get(k)
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                return u
    return _extract_https_download_url(att)


def incoming_max_message_id(event: IncomingMessage) -> Optional[str]:
    """id входящего сообщения MAX из сырого webhook (для GET /messages/{id})."""
    raw = getattr(event, "raw", None)
    if not isinstance(raw, dict):
        return None
    msg = raw.get("message")
    if not isinstance(msg, dict):
        return None
    mid = msg.get("id") or msg.get("message_id")
    return str(mid) if mid is not None else None


def _norm_recipient_from_message(
    msg: Dict[str, Any],
    update: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Собрать единый dict recipient для разбора peer: иногда чат дублируется в
    ``message.chat`` или в ``body.recipient``, а не только в ``message.recipient``.
    Иногда объект чата лежит на верхнем уровне webhook (``update.chat``).
    """
    rec = msg.get("recipient")
    out: Dict[str, Any] = dict(rec) if isinstance(rec, dict) else {}

    body = msg.get("body")
    if isinstance(body, dict):
        br = body.get("recipient")
        if isinstance(br, dict):
            for k, v in br.items():
                if k not in out or out[k] in (None, "", {}):
                    out[k] = v
        # редко: вложенный chat в body
        bch = body.get("chat")
        if isinstance(bch, dict) and not isinstance(out.get("chat"), dict):
            out["chat"] = bch

    mch = msg.get("chat")
    if isinstance(mch, dict):
        existing = out.get("chat")
        if not isinstance(existing, dict):
            out["chat"] = dict(mch)
        else:
            merged = {**existing, **mch}
            out["chat"] = merged

    if isinstance(update, dict):
        uch = update.get("chat")
        if isinstance(uch, dict):
            existing = out.get("chat")
            if not isinstance(existing, dict):
                out["chat"] = dict(uch)
            else:
                out["chat"] = {**existing, **uch}

    return out


def _extract_chat_title_from_peer(recipient: Dict[str, Any], chat_nested: Dict[str, Any]) -> Optional[str]:
    """
    Название группы/канала для Sheets и UI.
    В доке MAX у Chat поле ``title``, в webhook часто приходит ``name`` или другие алиасы.
    """
    keys = (
        "title",
        "name",
        "chat_name",
        "chat_title",
        "subject",
        "display_name",
        "label",
    )
    for src in (chat_nested, recipient):
        if not isinstance(src, dict):
            continue
        for k in keys:
            v = src.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
    return None


def _user_from_max(obj: Optional[Dict[str, Any]]) -> Optional[InternalUser]:
    if not obj or not isinstance(obj, dict):
        return None
    uid = obj.get("user_id")
    if uid is None:
        uid = obj.get("id")
    if uid is None:
        return None
    return InternalUser(
        id=uid,
        platform="max",
        username=obj.get("username"),
        name=obj.get("name") or obj.get("first_name"),
        full_name=obj.get("full_name") or obj.get("name"),
    )


def _parse_recipient_peer(
    recipient: Optional[Dict[str, Any]],
    sender: InternalUser,
) -> tuple[Any, bool, InternalChat]:
    """
    peer_id для POST /messages (user_id или chat_id), is_group, InternalChat.

    В webhook MAX группа часто приходит как ``recipient.chat`` с полями
    ``chat_id`` и ``type: "chat"`` (см. объект Chat в API), без верхнеуровневого
    ``recipient.type`` — раньше такие сообщения ошибочно считались личкой.
    """
    if not recipient or not isinstance(recipient, dict):
        uid = sender.id
        chat = InternalChat(
            id=uid,
            platform="max",
            is_group=False,
            title=None,
            username=sender.username,
        )
        return uid, False, chat

    chat_nested = recipient.get("chat") if isinstance(recipient.get("chat"), dict) else {}
    rtype = str(recipient.get("type") or recipient.get("recipient_type") or "").lower()
    nested_type = str(chat_nested.get("type") or "").lower()
    # Плоский recipient MAX: { chat_id, chat_type, user_id } — группа даже при user_id (например бот).
    chat_type_flat = str(recipient.get("chat_type") or "").lower()

    participants_raw = chat_nested.get("participants_count")
    try:
        participants_n = int(participants_raw) if participants_raw is not None else None
    except (TypeError, ValueError):
        participants_n = None

    # Группа / канал: верхний уровень или вложенный Chat (type "chat" = группа в MAX API).
    # type "dialog" при participants_count > 2 — несоответствие схемы, но это явно не 1:1.
    # У вложенного Chat без type, но с chat_id и >2 участников — группа.
    is_group = (
        rtype in ("chat", "group", "channel")
        or recipient.get("is_group")
        or nested_type in ("chat", "channel")
        or chat_type_flat in ("chat", "channel", "group", "supergroup")
        or (nested_type == "dialog" and participants_n is not None and participants_n > 2)
        or (
            participants_n is not None
            and participants_n > 2
            and nested_type not in ("dialog",)
        )
    )

    # Webhook: только верхний chat_id без user_id и без вложенного chat — часто peer группы
    if not is_group:
        top_cid = recipient.get("chat_id")
        top_uid = recipient.get("user_id")
        if top_cid is not None and top_uid is None and rtype not in ("user",) and not chat_nested:
            is_group = True

    # Вложенный объект chat с chat_id и явным типом группы / канала
    if not is_group and chat_nested.get("chat_id") is not None:
        if nested_type in ("chat", "channel", "group", "supergroup"):
            is_group = True
        elif not nested_type and participants_n is not None and participants_n > 2:
            is_group = True

    if is_group:
        cid = (
            recipient.get("chat_id")
            or recipient.get("id")
            or chat_nested.get("chat_id")
            or chat_nested.get("id")
        )
        if cid is None:
            cid = sender.id
        title = _extract_chat_title_from_peer(recipient, chat_nested)
        chat = InternalChat(
            id=cid,
            platform="max",
            is_group=True,
            title=title,
            username=recipient.get("username") or chat_nested.get("username"),
        )
        return cid, True, chat

    # Личка с ботом: recipient почти всегда указывает на бота (его user_id).
    # POST /messages?user_id=… должен быть id пользователя-отправителя, иначе API отвечает 403.
    uid = sender.id
    chat = InternalChat(
        id=uid,
        platform="max",
        is_group=False,
        title=None,
        username=sender.username,
    )
    return uid, False, chat


def _message_body_text(msg: Dict[str, Any]) -> str:
    body = msg.get("body")
    if isinstance(body, dict):
        t = body.get("text")
        if t is not None:
            return str(t).strip()
    return str(msg.get("text") or msg.get("message") or "").strip()


def _attachment_file_ref(att: Any) -> tuple[Optional[str], str, str]:
    """Идентификатор вложения MAX: (value, media_type, source_key) — source_key нужен для исходящего payload (token vs file_id)."""
    if not isinstance(att, dict):
        return None, "file", ""
    att_type = str(att.get("type") or att.get("media_type") or "file").lower()

    def pick(d: dict) -> tuple[Optional[str], str]:
        for key in ("file_id", "id", "url", "token", "photo_id", "media_id"):
            v = d.get(key)
            if v is not None and str(v).strip():
                return str(v).strip(), key
        return None, ""

    val, rk = pick(att)
    if val:
        return val, att_type, rk
    pl = att.get("payload")
    if isinstance(pl, dict):
        val, rk = pick(pl)
        if val:
            return val, att_type, rk
    for subk in ("photo", "file", "image", "video", "document", "media", "sticker"):
        sub = att.get(subk)
        if isinstance(sub, dict):
            val, rk = pick(sub)
            if val:
                return val, att_type, rk
    return None, att_type, ""


def _max_outgoing_payload(att_type: str, ref_key: str, value: str) -> Dict[str, str]:
    """
    Поле payload для POST /messages (Platform API MAX).
    Для image/sticker сервер требует photos | url | token — не file_id (proto.payload).
    """
    t = att_type.lower()
    if t == "photo":
        t = "image"
    if ref_key == "url":
        return {"url": value}
    if t in ("video", "audio"):
        return {"token": value}
    if t in ("image", "sticker"):
        return {"token": value}
    if t in ("file", "document"):
        return {"file_id": value}
    if ref_key == "token":
        return {"token": value}
    return {"file_id": value}


def normalize_max_send_attachment_type(t: str) -> str:
    x = (t or "file").lower()
    return "image" if x == "photo" else x


def _normalize_max_send_payload(normalized_type: str, payload: Dict[str, str]) -> Dict[str, str]:
    """Старый media_json мог содержать max_payload с file_id для картинки — API ждёт token."""
    typ = normalize_max_send_attachment_type(normalized_type)
    if typ not in ("image", "sticker"):
        return payload
    if "token" in payload or "url" in payload or "photos" in payload:
        return payload
    fid = payload.get("file_id")
    if fid:
        return {"token": fid}
    return payload


def _incoming_attachments(msg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    body = msg.get("body")
    atts: List[Any] = []
    if isinstance(body, dict):
        atts = body.get("attachments") or []
    if not atts:
        atts = msg.get("attachments") or msg.get("media") or []
    if isinstance(body, dict) and not atts:
        for key in ("photo", "image", "video", "file", "document", "audio"):
            block = body.get(key)
            if isinstance(block, dict):
                merged = dict(block)
                merged.setdefault("type", key)
                atts = [merged]
                break
    for att in atts:
        if not isinstance(att, dict):
            continue
        file_id, att_type, ref_key = _attachment_file_ref(att)
        if file_id:
            item: Dict[str, Any] = {
                "type": att_type,
                "file_id": file_id,
                "id_or_url": file_id,
                "max_payload": _max_outgoing_payload(att_type, ref_key, file_id),
            }
            du = _extract_https_download_url(att)
            if du:
                item["download_url"] = du
            out.append(item)
    return out


def message_attachments_from_incoming(event: IncomingMessage) -> List[Dict[str, Any]]:
    """Вложения входящего сообщения: приоритет полного разбора raw.message (как в webhook)."""
    raw = getattr(event, "raw", None)
    if isinstance(raw, dict):
        msg = raw.get("message")
        if isinstance(msg, dict):
            parsed = _incoming_attachments(msg)
            if parsed:
                return parsed
    return list(event.attachments or [])


def _reply_to_mid(msg: Dict[str, Any]) -> Optional[str | int]:
    link = msg.get("link")
    if isinstance(link, dict):
        mid = link.get("mid") or link.get("message_id") or link.get("id")
        if mid is not None:
            return mid
    return msg.get("reply_to_message_id")


def parse_max_update(update: Dict[str, Any]) -> Optional[IncomingMessage | CallbackEvent]:
    """
    Разобрать входящий Update от MAX (webhook).
    https://dev.max.ru/docs-api/objects/Update — поле update_type.
    """
    if not update or not isinstance(update, dict):
        return None

    update_type = (
        update.get("update_type")
        or update.get("type")
        or update.get("event_type")
    )
    if not update_type:
        return None

    update_type = str(update_type)

    # --- message_callback ---
    if update_type in ("message_callback", "callback_query", "callback"):
        cb = update.get("callback") or update.get("callback_query") or {}
        if not isinstance(cb, dict):
            cb = {}
        callback_id = cb.get("callback_id") or cb.get("id")
        callback_id_str = str(callback_id) if callback_id is not None else ""
        payload = cb.get("payload") or cb.get("data") or update.get("payload") or ""

        user_dict = (
            update.get("user")
            or cb.get("user")
            or cb.get("from")
            or (cb.get("message") or {}).get("sender")
            or update.get("sender")
        )
        user = _user_from_max(user_dict if isinstance(user_dict, dict) else None)
        if user is None:
            return None

        msg = cb.get("message") or update.get("message") or {}
        if not isinstance(msg, dict):
            msg = {}
        recipient = _norm_recipient_from_message(msg, update)
        _, _, chat = _parse_recipient_peer(recipient, user)
        mid = msg.get("id") or msg.get("message_id")

        return CallbackEvent(
            user=user,
            chat=chat,
            data=str(payload),
            original_message_id=mid,
            raw=update,
            callback_id=callback_id_str or None,
        )

    # --- bot_started → как /start ---
    if update_type == "bot_started":
        user = _user_from_max(update.get("user") or update.get("sender"))
        if user is None:
            return None
        _, _, chat = _parse_recipient_peer(None, user)
        return IncomingMessage(
            user=user,
            chat=chat,
            text="/start",
            attachments=[],
            is_command=True,
            reply_to_message_id=None,
            raw=update,
        )

    # --- message_created и прочие с полем message ---
    if update_type not in ("message_created", "message_updated", "message_edited", "message_received"):
        if update.get("message") is None:
            return None

    msg = update.get("message")
    if not isinstance(msg, dict):
        return None

    sender = _user_from_max(msg.get("sender"))
    if sender is None:
        sender = _user_from_max(update.get("user") or update.get("sender"))
    if sender is None:
        return None

    recipient = _norm_recipient_from_message(msg, update)
    _, is_group, chat = _parse_recipient_peer(recipient, sender)

    if update_type == "message_created" and not is_group:
        cn = recipient.get("chat") if isinstance(recipient.get("chat"), dict) else {}
        logger.info(
            "MAX message_created parsed as direct chat (is_group=False): "
            "recipient_keys=%s chat_keys=%s chat.type=%r participants_count=%r top_chat_id=%r",
            sorted(recipient.keys()),
            sorted(cn.keys()) if cn else [],
            cn.get("type"),
            cn.get("participants_count"),
            recipient.get("chat_id"),
        )

    text = _message_body_text(msg)
    attachments = _incoming_attachments(msg)
    reply_mid = _reply_to_mid(msg)

    return IncomingMessage(
        user=sender,
        chat=chat,
        text=text,
        attachments=attachments,
        is_command=text.startswith("/"),
        reply_to_message_id=reply_mid,
        raw=update,
    )


def keyboard_rows_to_max_attachments(rows: Optional[List[KeyboardRow]]) -> Optional[List[Dict[str, Any]]]:
    """
    Вложения MAX: inline_keyboard с кнопками callback / link.
    https://dev.max.ru/docs-api — раздел «Клавиатура».
    """
    if not rows:
        return None
    button_rows: List[List[Dict[str, Any]]] = []
    for row in rows:
        r: List[Dict[str, Any]] = []
        for btn in row:
            if btn.url:
                r.append({"type": "link", "text": btn.text, "url": btn.url})
            elif btn.callback_data:
                r.append(
                    {
                        "type": "callback",
                        "text": btn.text,
                        "payload": btn.callback_data,
                    }
                )
        if r:
            button_rows.append(r)
    if not button_rows:
        return None
    return [{"type": "inline_keyboard", "payload": {"buttons": button_rows}}]


def _parse_mode_to_max_format(parse_mode: Optional[str]) -> Optional[str]:
    if not parse_mode:
        return None
    p = str(parse_mode).upper()
    if p in ("HTML", "PARSEMODE.HTML"):
        return "html"
    if p in ("MARKDOWN", "MD", "PARSEMODE.MARKDOWN"):
        return "markdown"
    if parse_mode.lower() == "html":
        return "html"
    if parse_mode.lower() == "markdown":
        return "markdown"
    return "html"


class MaxAdapter:
    """Адаптер мессенджера для MAX."""

    def __init__(self, client: MaxApiClient) -> None:
        self._client = client

    @property
    def platform(self) -> Platform:
        return "max"

    async def send_message(self, msg: OutgoingMessage) -> Optional[str]:
        """Отправить сообщение. Возвращает message_id (строка)."""
        attachments = keyboard_rows_to_max_attachments(msg.keyboard_rows)
        text_format = _parse_mode_to_max_format(msg.parse_mode) or "html"
        result = await self._client.send_message(
            chat_id=msg.chat_id,
            text=msg.text,
            is_group_chat=msg.is_group_chat,
            text_format=text_format,
            attachments=attachments,
        )
        return self._client.message_id_from_response(result)

    async def edit_message(
        self,
        chat_id: int | str,
        message_id: int | str,
        text: str,
        keyboard_rows: Optional[List[KeyboardRow]] = None,
        parse_mode: Optional[str] = None,
    ) -> None:
        """Редактировать сообщение (PUT /messages). chat_id в MAX для edit не используется в query."""
        attachments = keyboard_rows_to_max_attachments(keyboard_rows)
        text_format = _parse_mode_to_max_format(parse_mode)
        await self._client.edit_message(
            message_id=message_id,
            text=text,
            text_format=text_format,
            attachments=attachments,
        )

    async def answer_callback(
        self,
        callback_id_or_equivalent: str | int,
        text: Optional[str] = None,
    ) -> None:
        """Ответить на callback (POST /answers)."""
        await self._client.answer_callback(
            str(callback_id_or_equivalent),
            notification=text,
        )

    async def upload_document_bytes(self, data: bytes, filename: str, mime_type: str) -> str:
        """Загрузить файл в MAX, вернуть file_id для вложений."""
        return await self._client.upload_file(data, filename, mime_type)

    async def send_typing(self, chat_id: int | str, *, is_group_chat: bool = False) -> None:
        """Показать индикатор набора (лучшее усилие; эндпоинт может отличаться)."""
        await self._client.send_typing(str(chat_id), is_group_chat=is_group_chat)

    async def send_media(
        self,
        chat_id: int | str,
        attachments: List[Dict[str, Any]],
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int | str] = None,
        *,
        is_group_chat: bool = False,
    ) -> None:
        """Отправить медиа через POST /messages с вложениями."""
        if not attachments:
            return None
        max_atts: List[Dict[str, Any]] = []
        for att in attachments:
            file_id = att.get("id_or_url") or att.get("file_id")
            if not file_id:
                continue
            typ = normalize_max_send_attachment_type(str(att.get("type", "file")))
            mp = att.get("max_payload")
            if isinstance(mp, dict) and mp:
                payload = {k: str(v) for k, v in mp.items() if v is not None}
            else:
                payload = _max_outgoing_payload(
                    str(att.get("type", "file")), "file_id", str(file_id)
                )
            payload = _normalize_max_send_payload(typ, payload)
            max_atts.append({"type": typ, "payload": payload})
        if not max_atts:
            return None
        body_text = caption or ""
        await self._client.send_message(
            chat_id,
            body_text,
            is_group_chat=is_group_chat,
            attachments=max_atts,
        )

    @staticmethod
    def _http_content_type_mime(resp: Any) -> Optional[str]:
        h = resp.headers.get("Content-Type")
        if not h:
            return None
        base = h.split(";")[0].strip().lower()
        return base or None

    async def download_file(
        self,
        file_id_or_url: str,
        *,
        message_id: Optional[str | int] = None,
    ) -> tuple[bytes, Optional[str]]:
        """
        Скачать файл: прямой URL, GET /messages/{mid}, либо URL из /files/{id}.
        Возвращает (байты, MIME из Content-Type ответа скачивания или None).
        """
        import aiohttp

        fid = (file_id_or_url or "").strip()
        if fid.startswith("http://") or fid.startswith("https://"):
            async with aiohttp.ClientSession() as session:
                async with session.get(fid) as resp:
                    resp.raise_for_status()
                    data = await resp.read()
                    return data, self._http_content_type_mime(resp)

        mid = str(message_id).strip() if message_id is not None else ""
        if mid:
            try:
                api_msg = await self._client.get_message(mid)
                dl = _find_attachment_download_url_in_api_message(api_msg, file_id=fid)
                if dl:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(dl) as resp:
                            resp.raise_for_status()
                            data = await resp.read()
                            return data, self._http_content_type_mime(resp)
            except MaxApiClientError as e:
                logger.warning(
                    "MAX download_file: get_message mid=%s file_id=%s failed: %s",
                    mid,
                    fid,
                    e,
                )

        try:
            result = await self._client._request("GET", f"/files/{fid}")
            url = result.get("url") or result.get("file_url")
            if url:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        resp.raise_for_status()
                        data = await resp.read()
                        return data, self._http_content_type_mime(resp)
        except MaxApiClientError as e:
            if e.status != 404:
                raise
            logger.debug("MAX GET /files/%s -> 404 (ожидаемо для части сборок API)", fid)

        raise MaxApiClientError(
            "MAX: не удалось скачать вложение: нет прямого url, GET /messages не вернул ссылку, /files недоступен",
            status=404,
            body=f"file_id={fid!r} message_id={mid!r}",
        )
