"""
Админ-сценарии в MAX: рассылка, плановые, база знаний, документы.
Состояние в памяти процесса (как QA-сессии); при нескольких репликах — sticky session или одна реплика.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from app.config import BOT_TOKEN
from app.core.callbacks import (
    ADMIN_SCHEDULED,
    BROADCAST_CANCEL,
    BROADCAST_SCHEDULED_DISABLE_PREFIX,
    BROADCAST_SEND_PREFIX,
    BROADCAST_SKIP_MEDIA,
    BROADCAST_START,
    BROADCAST_VARIANT_PREFIX,
    DOC_GEN_CANCEL,
    DOC_GEN_CONFIRM,
    DOC_GEN_START,
    DOC_GEN_TEMPLATE_PREFIX,
    KB_ADD,
)
from app.core.types import CallbackEvent, IncomingMessage, KeyboardButton, KeyboardRow, OutgoingMessage
from app.platforms.max.adapter import incoming_max_message_id, max_attachment_https_url
from app.handlers.document_generator import (
    DOC_GEN_FIELD_ORDER,
    DOC_GEN_PROMPTS,
    REQUISITES_FIELD_SPECS,
    _format_fields_preview,
)
from app.services.admin_access import is_admin_for_platform
from app.services.broadcast_service import (
    execute_broadcast_multi,
    get_broadcast_recipients_list,
    parse_broadcast_media_for_platform,
    read_active_recipients_chats_with_names,
    read_active_regions,
)
from app.services.document_template_service import (
    fill_docx_template,
    get_template_by_id,
    get_template_path,
    get_templates_list,
    extract_fields_from_file,
    extract_fields_from_image,
    extract_fields_from_text,
)
from app.services.kb_upload_service import process_kb_document_pipeline
from app.services.metrics_service import log_event
from app.services.openai_client import improve_broadcast_text, generate_broadcast_title
from app.services.scheduled_broadcast_service import (
    compute_next_run_iso,
    create_scheduled_broadcast,
    read_active_scheduled_broadcasts_for_list,
    set_scheduled_broadcast_inactive,
)
from app.platforms.max.adapter import message_attachments_from_incoming
from app.ui.keyboards import max_admin_menu_rows, max_main_menu_rows

logger = logging.getLogger(__name__)

# uid -> сценарий
_sessions: Dict[int, Dict[str, Any]] = {}

MAX_ADMIN_HOME = "max:admin:home"

# Сегментация рассылки MAX (callback_data — короткие префиксы)
_MAXBC_SEG = "maxbc:seg"
_MAXBC_REG = "maxbc:reg"
_MAXBC_CHT = "maxbc:cht"
_MAXBC_BACK_SEG = "maxbc:bseg"
_MAXBC_RT = "maxbc:rt:"  # + индекс региона в полном списке
_MAXBC_RP = "maxbc:rp:"  # + страница
_MAXBC_RGO = "maxbc:rgo"
_MAXBC_CT = "maxbc:ct:"  # + chat_id (в т.ч. отрицательный)
_MAXBC_CP = "maxbc:cp:"
_MAXBC_CGO = "maxbc:cgo"
_MAX_REGIONS_PAGE = 8
_MAX_CHATS_PAGE = 6
_MAX_AUDIENCE_PLATFORM = "max"


def _clear(uid: int) -> None:
    _sessions.pop(uid, None)


def _busy(uid: int) -> bool:
    return uid in _sessions


def _rows(*pairs: Tuple[str, str]) -> list[KeyboardRow]:
    return [[KeyboardButton(text=t, callback_data=c)] for t, c in pairs]


def _max_audience_final_kb() -> list[KeyboardRow]:
    return _rows(
        ("👥 Пользователям", "broadcast:send:users"),
        ("💬 Во все чаты", "broadcast:send:chats"),
        ("👥💬 Бот и чаты", "broadcast:send:users_chats"),
        ("📋 Чаты и регионы", _MAXBC_SEG),
        ("❌ Отмена", "broadcast:cancel_send"),
    )


async def _send_maxbc_segmentation_menu(adapter, chat_id: Any, is_g: bool) -> None:
    await _send(
        adapter,
        chat_id,
        "📋 <b>Как выбрать получателей?</b>",
        _rows(
            ("🌍 По регионам", _MAXBC_REG),
            ("🏢 По чатам", _MAXBC_CHT),
            ("⬅️ Назад", _MAXBC_BACK_SEG),
        ),
        is_group=is_g,
    )


def _region_toggle_row(name: str, idx: int, selected: List[str]) -> KeyboardRow:
    mark = "☑" if name in selected else "☐"
    short = (name[:35] + "…") if len(name) > 35 else name
    return [KeyboardButton(text=f"{mark} {short}", callback_data=f"{_MAXBC_RT}{idx}")]


async def _render_max_regions(adapter, chat_id: Any, is_g: bool, s: Dict[str, Any]) -> None:
    regions: List[str] = s.get("available_regions") or []
    selected: List[str] = list(s.get("selected_regions") or [])
    page = int(s.get("regions_page") or 0)
    n = len(regions)
    per = _MAX_REGIONS_PAGE
    start = page * per
    chunk = regions[start : start + per]
    kb: list[KeyboardRow] = []
    for i, rname in enumerate(chunk):
        gidx = start + i
        kb.append(_region_toggle_row(rname, gidx, selected))
    nav: KeyboardRow = []
    if page > 0:
        nav.append(KeyboardButton(text="◀", callback_data=f"{_MAXBC_RP}{page - 1}"))
    if start + per < n:
        nav.append(KeyboardButton(text="▶", callback_data=f"{_MAXBC_RP}{page + 1}"))
    if nav:
        kb.append(nav)
    kb.append([KeyboardButton(text="✅ Далее (к отправке)", callback_data=_MAXBC_RGO)])
    kb.append([KeyboardButton(text="⬅️ Назад", callback_data=_MAXBC_BACK_SEG)])
    body = (
        "🌍 <b>Регионы</b> (строки с <code>platform=max</code> в recipients_chats)\n\n"
        f"Выбрано регионов: {len(selected)}. В списке: {n}.\n"
        "Отметьте регионы, затем «Далее»."
    )
    await _send(adapter, chat_id, body, kb, is_group=is_g)


async def _render_max_chats(adapter, chat_id: Any, is_g: bool, s: Dict[str, Any]) -> None:
    chats: List[Dict[str, Any]] = s.get("available_chats") or []
    sel_ids: List[int] = list(s.get("selected_chat_ids") or [])
    page = int(s.get("chats_page") or 0)
    n = len(chats)
    per = _MAX_CHATS_PAGE
    start = page * per
    chunk = chats[start : start + per]
    kb: list[KeyboardRow] = []
    for c in chunk:
        cid = int(c["chat_id"])
        nm = str(c.get("name") or cid)[:32]
        mark = "☑" if cid in sel_ids else "☐"
        kb.append([KeyboardButton(text=f"{mark} {nm}", callback_data=f"{_MAXBC_CT}{cid}")])
    nav: KeyboardRow = []
    if page > 0:
        nav.append(KeyboardButton(text="◀", callback_data=f"{_MAXBC_CP}{page - 1}"))
    if start + per < n:
        nav.append(KeyboardButton(text="▶", callback_data=f"{_MAXBC_CP}{page + 1}"))
    if nav:
        kb.append(nav)
    kb.append([KeyboardButton(text="✅ Далее (к отправке)", callback_data=_MAXBC_CGO)])
    kb.append([KeyboardButton(text="⬅️ Назад", callback_data=_MAXBC_BACK_SEG)])
    body = (
        "🏢 <b>Чаты</b> (platform=max)\n\n"
        f"Выбрано чатов: {len(sel_ids)} из {n}."
    )
    await _send(adapter, chat_id, body, kb, is_group=is_g)


async def _send_broadcast_send_type(adapter, chat_id: Any, is_g: bool) -> None:
    await _send(
        adapter,
        chat_id,
        "⏰ Как отправить?",
        _rows(
            ("📤 Сейчас", "broadcast:send_now"),
            ("📅 Раз в неделю (10:00)", "broadcast:schedule:weekly"),
            ("📆 Раз в месяц (10:00)", "broadcast:schedule:monthly"),
            ("❌ Отмена", "broadcast:cancel_send"),
        ),
        is_group=is_g,
    )


async def _try_maxbc_broadcast(
    adapter,
    event: CallbackEvent,
    uid: int,
    data: str,
    s: Dict[str, Any],
    chat_id: Any,
    is_g: bool,
) -> bool:
    if s.get("kind") != "broadcast" or not data.startswith("maxbc:"):
        return False

    if data == _MAXBC_SEG and s.get("step") == "audience_final":
        s["step"] = "choose_segmentation"
        await _send_maxbc_segmentation_menu(adapter, chat_id, is_g)
        return True

    if data == _MAXBC_BACK_SEG:
        if s.get("step") in ("choose_segmentation", "select_regions", "select_chats"):
            s["step"] = "audience_final"
            await _send(
                adapter,
                chat_id,
                "✅ Тест отправлен. Кому разослать?",
                _max_audience_final_kb(),
                is_group=is_g,
            )
            return True

    if data == _MAXBC_REG and s.get("step") == "choose_segmentation":
        regions = await asyncio.to_thread(read_active_regions, "max")
        if not regions:
            await _send(
                adapter,
                chat_id,
                "❌ Нет регионов для platform=max в recipients_chats (колонки region, platform).",
                [[KeyboardButton(text="⬅️ Назад", callback_data=_MAXBC_BACK_SEG)]],
                is_group=is_g,
            )
            return True
        s["step"] = "select_regions"
        s["available_regions"] = regions
        s["selected_regions"] = []
        s["regions_page"] = 0
        await _render_max_regions(adapter, chat_id, is_g, s)
        return True

    if data == _MAXBC_CHT and s.get("step") == "choose_segmentation":
        chats = await asyncio.to_thread(read_active_recipients_chats_with_names, "max")
        if not chats:
            await _send(
                adapter,
                chat_id,
                "❌ Нет чатов с platform=max в recipients_chats.",
                [[KeyboardButton(text="⬅️ Назад", callback_data=_MAXBC_BACK_SEG)]],
                is_group=is_g,
            )
            return True
        s["step"] = "select_chats"
        s["available_chats"] = chats
        s["selected_chat_ids"] = []
        s["chats_page"] = 0
        await _render_max_chats(adapter, chat_id, is_g, s)
        return True

    if data.startswith(_MAXBC_RT) and s.get("step") == "select_regions":
        try:
            idx = int(data[len(_MAXBC_RT) :])
        except ValueError:
            return True
        regions = s.get("available_regions") or []
        if idx < 0 or idx >= len(regions):
            return True
        sel = list(s.get("selected_regions") or [])
        name = regions[idx]
        if name in sel:
            sel.remove(name)
        else:
            sel.append(name)
        s["selected_regions"] = sel
        await _render_max_regions(adapter, chat_id, is_g, s)
        return True

    if data.startswith(_MAXBC_RP) and s.get("step") == "select_regions":
        try:
            page = int(data[len(_MAXBC_RP) :])
        except ValueError:
            return True
        s["regions_page"] = max(0, page)
        await _render_max_regions(adapter, chat_id, is_g, s)
        return True

    if data == _MAXBC_RGO and s.get("step") == "select_regions":
        sel = s.get("selected_regions") or []
        if not sel:
            await _send(adapter, chat_id, "Выберите хотя бы один регион.", None, is_group=is_g)
            return True
        s["pending_send_mode"] = "selected_regions"
        s["pending_mode_extra"] = {
            "selected_regions": list(sel),
            "audience_platform": _MAX_AUDIENCE_PLATFORM,
        }
        s["step"] = "send_type"
        await _send_broadcast_send_type(adapter, chat_id, is_g)
        return True

    if data.startswith(_MAXBC_CT) and s.get("step") == "select_chats":
        tail = data[len(_MAXBC_CT) :]
        try:
            cid = int(tail)
        except ValueError:
            return True
        sel_ids = list(s.get("selected_chat_ids") or [])
        if cid in sel_ids:
            sel_ids.remove(cid)
        else:
            sel_ids.append(cid)
        s["selected_chat_ids"] = sel_ids
        await _render_max_chats(adapter, chat_id, is_g, s)
        return True

    if data.startswith(_MAXBC_CP) and s.get("step") == "select_chats":
        try:
            page = int(data[len(_MAXBC_CP) :])
        except ValueError:
            return True
        s["chats_page"] = max(0, page)
        await _render_max_chats(adapter, chat_id, is_g, s)
        return True

    if data == _MAXBC_CGO and s.get("step") == "select_chats":
        sel_ids = s.get("selected_chat_ids") or []
        if not sel_ids:
            await _send(adapter, chat_id, "Выберите хотя бы один чат.", None, is_group=is_g)
            return True
        s["pending_send_mode"] = "selected_chats"
        s["pending_mode_extra"] = {
            "selected_chat_ids": list(sel_ids),
            "audience_platform": _MAX_AUDIENCE_PLATFORM,
        }
        s["step"] = "send_type"
        await _send_broadcast_send_type(adapter, chat_id, is_g)
        return True

    return False


def _max_broadcast_item_from_attachment(a: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fid = a.get("id_or_url") or a.get("file_id")
    if not fid:
        return None
    item: Dict[str, Any] = {
        "type": str(a.get("type", "file")).lower(),
        "file_id": str(fid),
        "id_or_url": str(fid),
    }
    mp = a.get("max_payload")
    if isinstance(mp, dict) and mp:
        item["max_payload"] = mp
    return item


# Лимит текста тела сообщения MAX (NewMessageBody.text)
_MAX_OUTGOING_MESSAGE_TEXT = 4000


def _max_variant_choice_message(improved_text: str, media_json: str, max_total: int = _MAX_OUTGOING_MESSAGE_TEXT) -> str:
    """Текст шага выбора оригинал/улучшенный — как в Telegram _process_broadcast_text, с усечением под лимит API."""
    head = "📋 <b>Превью (улучшенный вариант)</b>\n\n"
    suffix = ""
    if (media_json or "").strip():
        suffix += "\n\n📎 Медиа прикреплено"
    suffix += "\n\nВыберите вариант текста:"
    overhead = len(head) + len(suffix)
    budget = max_total - overhead
    if budget < 1:
        budget = 1
    imp = improved_text or ""
    if len(imp) <= budget:
        body_imp = imp
    else:
        body_imp = imp[: max(budget - 1, 0)] + "…"
    return head + body_imp + suffix


def _merge_max_media_json(existing: str, max_items: list) -> str:
    try:
        data = json.loads(existing) if existing.strip() else {}
    except json.JSONDecodeError:
        data = {}
    if isinstance(data, list):
        data = {"version": 2, "telegram": data, "max": []}
    if not isinstance(data, dict):
        data = {"version": 2, "telegram": [], "max": []}
    if data.get("version") != 2:
        tg = data if isinstance(data, list) else []
        data = {"version": 2, "telegram": tg if isinstance(tg, list) else [], "max": []}
    data["version"] = 2
    data["max"] = max_items
    return json.dumps(data, ensure_ascii=False)


async def _send(
    adapter,
    chat_id: Any,
    text: str,
    rows: Optional[list[KeyboardRow]] = None,
    *,
    is_group: bool = False,
) -> Optional[str]:
    return await adapter.send_message(
        OutgoingMessage(
            chat_id=chat_id,
            platform="max",
            text=text,
            keyboard_rows=rows,
            is_group_chat=is_group,
        )
    )


async def _run_broadcast_multi(
    adapter,
    recipients: list,
    text_final: str,
    media_json: str,
    created_by: int,
    username: Optional[str],
    text_original: str,
    variant: str,
    mode: str,
) -> Tuple[str, int, int]:
    from aiogram import Bot
    from aiogram.client.default import DefaultBotProperties
    from aiogram.enums import ParseMode
    from app.platforms.max import MaxAdapter, MaxApiClient
    from app.platforms.telegram import TelegramAdapter

    adapters: Dict[str, Any] = {"max": adapter}
    bot = None
    if BOT_TOKEN:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        adapters["telegram"] = TelegramAdapter(bot)
    try:
        return await execute_broadcast_multi(
            adapters,
            recipients,
            text_final,
            media_json,
            created_by_user_id=created_by,
            created_by_username=username,
            text_original=text_original,
            selected_variant=variant,
            mode=mode,
        )
    finally:
        if bot:
            await bot.session.close()


async def show_admin_menu(adapter, event: CallbackEvent | IncomingMessage) -> None:
    await _send(
        adapter,
        event.chat.id,
        "🛠 <b>Раздел администратора</b>\n\nВыберите функцию:",
        max_admin_menu_rows(),
        is_group=event.chat.is_group,
    )


async def handle_admin_callback(
    adapter,
    max_client: Any,
    event: CallbackEvent,
    uid: int,
) -> bool:
    if not is_admin_for_platform("max", uid):
        return False
    data = (event.data or "").strip()
    chat_id = event.chat.id
    is_g = event.chat.is_group

    if data == MAX_ADMIN_HOME:
        _clear(uid)
        ok, role = True, "admin"
        await _send(
            adapter,
            chat_id,
            "Главное меню.",
            max_main_menu_rows(is_authorized=True, is_admin=True),
            is_group=is_g,
        )
        return True

    if data == ADMIN_SCHEDULED:
        _clear(uid)
        rows = read_active_scheduled_broadcasts_for_list()
        if not rows:
            await _send(adapter, chat_id, "📅 Активных плановых рассылок нет.", max_admin_menu_rows(), is_group=is_g)
            return True
        lines = ["📅 <b>Плановые рассылки</b>\n"]
        kb: list[KeyboardRow] = []
        for r in rows[:15]:
            sid = r.get("schedule_id", "")
            title = (r.get("title") or sid)[:40]
            lines.append(f"• {title}")
            kb.append(
                [
                    KeyboardButton(
                        text=f"🔴 Выключить {sid[:8]}",
                        callback_data=f"{BROADCAST_SCHEDULED_DISABLE_PREFIX}{sid}",
                    )
                ]
            )
        kb.append([KeyboardButton(text="⬅️ Назад", callback_data=MAX_ADMIN_HOME)])
        await _send(adapter, chat_id, "\n".join(lines), kb, is_group=is_g)
        return True

    if data.startswith(BROADCAST_SCHEDULED_DISABLE_PREFIX):
        sid = data[len(BROADCAST_SCHEDULED_DISABLE_PREFIX) :].strip()
        ok = await asyncio.to_thread(set_scheduled_broadcast_inactive, sid)
        await _send(
            adapter,
            chat_id,
            "✅ Отключено." if ok else "❌ Не удалось отключить (нет в таблице?).",
            max_admin_menu_rows(),
            is_group=is_g,
        )
        return True

    if data == BROADCAST_START:
        _sessions[uid] = {
            "kind": "broadcast",
            "step": "wait_text",
            "text_original": "",
            "text_final": "",
            "improved_text": "",
            "media_json": "",
            "selected_variant": "original",
            "pending_send_mode": "",
            "pending_mode_extra": None,
        }
        await _send(
            adapter,
            chat_id,
            "📢 <b>Рассылка</b>\n\nВведите текст сообщения (или «-» если только медиа).\n\nДля отмены: /cancel",
            [[KeyboardButton(text="❌ Отмена", callback_data=BROADCAST_CANCEL)]],
            is_group=is_g,
        )
        return True

    if data == KB_ADD:
        _sessions[uid] = {"kind": "kb", "step": "wait_file"}
        await _send(
            adapter,
            chat_id,
            "📚 Отправьте файл (PDF, TXT, DOCX, MD, CSV).\n\n/cancel — отмена.",
            [[KeyboardButton(text="❌ Отмена", callback_data=MAX_ADMIN_HOME)]],
            is_group=is_g,
        )
        return True

    if data == DOC_GEN_START:
        templates = get_templates_list()
        if not templates:
            await _send(
                adapter,
                chat_id,
                "Нет шаблонов. Добавьте шаблон в Telegram: /admin → Создать документ → Добавить шаблон.",
                max_admin_menu_rows(),
                is_group=is_g,
            )
            return True
        kb = [
            [KeyboardButton(text=t["name"], callback_data=f"{DOC_GEN_TEMPLATE_PREFIX}{t['id']}")]
            for t in templates
        ]
        kb.append([KeyboardButton(text="⬅️ Назад", callback_data=MAX_ADMIN_HOME)])
        _sessions[uid] = {"kind": "doc", "step": "pick_template"}
        await _send(adapter, chat_id, "📝 Выберите шаблон:", kb, is_group=is_g)
        return True

    if data.startswith(DOC_GEN_TEMPLATE_PREFIX) and _sessions.get(uid, {}).get("kind") == "doc":
        tid = data[len(DOC_GEN_TEMPLATE_PREFIX) :].strip()
        tpl = get_template_by_id(tid)
        path = get_template_path(tpl) if tpl else None
        if not tpl or not path or not path.is_file():
            await _send(adapter, chat_id, "Шаблон не найден.", max_admin_menu_rows(), is_group=is_g)
            _clear(uid)
            return True
        _sessions[uid] = {
            "kind": "doc",
            "step": "field",
            "template_id": tid,
            "template": tpl,
            "collected": {},
            "next_field": 0,
        }
        await _send(
            adapter,
            chat_id,
            f"📄 <b>{tpl.get('name', tid)}</b>\n\n{DOC_GEN_PROMPTS[0]}",
            [[KeyboardButton(text="❌ Отмена", callback_data=DOC_GEN_CANCEL)]],
            is_group=is_g,
        )
        return True

    s = _sessions.get(uid)
    if not s or s.get("kind") != "broadcast":
        return False

    if await _try_maxbc_broadcast(adapter, event, uid, data, s, chat_id, is_g):
        return True

    if data == BROADCAST_CANCEL:
        _clear(uid)
        await _send(adapter, chat_id, "❌ Рассылка отменена.", max_admin_menu_rows(), is_group=is_g)
        return True

    if data == BROADCAST_SKIP_MEDIA:
        if s.get("step") != "wait_media":
            return True
        to = s.get("text_original", "")
        mj = s.get("media_json", "")
        if not to and not mj:
            await _send(adapter, chat_id, "Нужен текст или медиа.", None, is_group=is_g)
            return True
        await _after_text_media(adapter, chat_id, is_g, uid, s, to, mj)
        return True

    if data.startswith(BROADCAST_VARIANT_PREFIX):
        if s.get("step") != "pick_variant":
            return True
        choice = data.split(":")[-1]
        mj = s.get("media_json", "")
        if choice == "original":
            s["text_final"] = s.get("text_original", "")
            s["selected_variant"] = "original"
        else:
            s["text_final"] = s.get("improved_text", "")
            s["selected_variant"] = "improved"
        s["step"] = "preview"
        await _send_preview(adapter, chat_id, is_g, uid, s)
        return True

    if data == "broadcast:aud:test_self":
        if s.get("step") != "preview":
            return True
        tf = s.get("text_final", "")
        mj = s.get("media_json", "")
        atts = parse_broadcast_media_for_platform(mj, "max")
        try:
            if atts:
                await adapter.send_media(chat_id, atts, caption=tf, is_group_chat=is_g)
            elif tf:
                await _send(adapter, chat_id, tf, None, is_group=is_g)
            s["step"] = "audience_final"
            await _send(
                adapter,
                chat_id,
                "✅ Тест отправлен. Кому разослать?",
                _max_audience_final_kb(),
                is_group=is_g,
            )
        except Exception as e:
            logger.exception("[MAX_ADMIN] test self: %s", e)
            await _send(adapter, chat_id, f"❌ Ошибка теста: {e}"[:300], None, is_group=is_g)
        return True

    if data.startswith(BROADCAST_SEND_PREFIX) and s.get("step") == "audience_final":
        mode = data.rsplit(":", 1)[-1]
        if mode not in ("users", "chats", "users_chats"):
            return True
        s["pending_send_mode"] = mode
        s["pending_mode_extra"] = None
        s["step"] = "send_type"
        await _send_broadcast_send_type(adapter, chat_id, is_g)
        return True

    if data == "broadcast:cancel_send":
        _clear(uid)
        await _send(adapter, chat_id, "Отменено.", max_admin_menu_rows(), is_group=is_g)
        return True

    if data == "broadcast:send_now" and s.get("step") == "send_type":
        mode = s.get("pending_send_mode", "users")
        extra = (
            s.get("pending_mode_extra")
            if mode in ("selected_chats", "selected_regions")
            else None
        )
        pf = "max" if mode in ("selected_chats", "selected_regions") else None
        tf = s.get("text_final", "")
        mj = s.get("media_json", "")
        uname = event.user.username
        try:
            bid, okc, fl = await _run_broadcast_multi(
                adapter,
                await asyncio.to_thread(get_broadcast_recipients_list, mode, extra, pf),
                tf,
                mj,
                uid,
                uname,
                s.get("text_original", ""),
                s.get("selected_variant", "original"),
                mode,
            )
            await asyncio.to_thread(
                log_event,
                user_id=uid,
                username=uname,
                event="broadcast_sent",
                meta={"broadcast_id": bid, "mode": mode, "ok": okc, "fail": fl, "platform": "max"},
            )
            await _send(
                adapter,
                chat_id,
                f"✅ Рассылка завершена. Успешно: {okc}, ошибок: {fl}\nID: <code>{bid}</code>",
                max_admin_menu_rows(),
                is_group=is_g,
            )
        except Exception as e:
            logger.exception("[MAX_ADMIN] send: %s", e)
            await _send(adapter, chat_id, f"❌ {e}"[:400], max_admin_menu_rows(), is_group=is_g)
        _clear(uid)
        return True

    if data == "broadcast:schedule:weekly" and s.get("step") == "send_type":
        s["step"] = "pick_weekday"
        await _send(
            adapter,
            chat_id,
            "День недели (10:00):",
            [
                [
                    KeyboardButton(text="Пн", callback_data="broadcast:weekday:0"),
                    KeyboardButton(text="Вт", callback_data="broadcast:weekday:1"),
                    KeyboardButton(text="Ср", callback_data="broadcast:weekday:2"),
                ],
                [
                    KeyboardButton(text="Чт", callback_data="broadcast:weekday:3"),
                    KeyboardButton(text="Пт", callback_data="broadcast:weekday:4"),
                ],
                [
                    KeyboardButton(text="Сб", callback_data="broadcast:weekday:5"),
                    KeyboardButton(text="Вс", callback_data="broadcast:weekday:6"),
                ],
            ],
            is_group=is_g,
        )
        return True

    if data.startswith("broadcast:weekday:") and s.get("step") == "pick_weekday":
        wd = int(data.rsplit(":", 1)[-1])
        mode = s.get("pending_send_mode", "chats")
        tf = s.get("text_final", "")
        mj = s.get("media_json", "")
        extra = (
            s.get("pending_mode_extra")
            if mode in ("selected_chats", "selected_regions")
            else None
        )
        mode_extra_str = json.dumps(extra, ensure_ascii=False) if extra else "{}"
        title = await asyncio.to_thread(generate_broadcast_title, tf or "Рассылка")
        schedule_id = await asyncio.to_thread(
            create_scheduled_broadcast,
            created_by_user_id=uid,
            created_by_username=event.user.username,
            text_final=tf,
            media_json=mj,
            mode=mode,
            mode_extra=mode_extra_str,
            schedule_type="weekly",
            schedule_config={"weekday": wd},
            title=title,
        )
        nxt = compute_next_run_iso("weekly", {"weekday": wd})
        await _send(
            adapter,
            chat_id,
            f"✅ Плановая рассылка создана.\nСледующий запуск: {nxt[:16]} UTC\nID: <code>{schedule_id}</code>",
            max_admin_menu_rows(),
            is_group=is_g,
        )
        _clear(uid)
        return True

    if data == "broadcast:schedule:monthly" and s.get("step") == "send_type":
        s["step"] = "wait_month_day"
        await _send(
            adapter,
            chat_id,
            "Введите число месяца (1–31) для ежемесячной отправки (10:00):",
            [[KeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel_send")]],
            is_group=is_g,
        )
        return True

    if data == "broadcast:edit_text_max" and s and s.get("kind") == "broadcast":
        s["step"] = "wait_text"
        await _send(
            adapter,
            chat_id,
            "Введите новый текст рассылки (или «-»):",
            [[KeyboardButton(text="❌ Отмена", callback_data=BROADCAST_CANCEL)]],
            is_group=is_g,
        )
        return True

    if data == DOC_GEN_CANCEL:
        if _sessions.get(uid, {}).get("kind") != "doc":
            return False
        _clear(uid)
        await _send(adapter, chat_id, "Отменено.", max_admin_menu_rows(), is_group=is_g)
        return True

    if data == DOC_GEN_CONFIRM:
        s2 = _sessions.get(uid)
        if not (s2 and s2.get("kind") == "doc" and s2.get("step") == "confirm"):
            return False
        tpl = s2.get("template")
        collected = s2.get("collected") or {}
        path = get_template_path(tpl)
        if not path or not path.is_file():
            _clear(uid)
            return True
        data_fill = {**collected}
        for k in DOC_GEN_FIELD_ORDER:
            data_fill.setdefault(k, collected.get(k, ""))
        try:
            docx_bytes = await asyncio.to_thread(fill_docx_template, path, data_fill)
        except Exception as e:
            logger.exception("[MAX_ADMIN] fill docx: %s", e)
            await _send(
                adapter,
                chat_id,
                f"❌ Ошибка генерации: {e}"[:300],
                max_admin_menu_rows(),
                is_group=is_g,
            )
            _clear(uid)
            return True
        fname = f"{(tpl.get('id') or 'doc')}.docx"
        try:
            fid = await adapter.upload_document_bytes(
                docx_bytes,
                fname,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
            await adapter.send_media(
                event.chat.id,
                [{"type": "file", "id_or_url": fid, "file_id": fid}],
                caption="📎 Готовый документ",
                is_group_chat=is_g,
            )
        except Exception as e:
            logger.exception("[MAX_ADMIN] upload doc: %s", e)
            await _send(
                adapter,
                chat_id,
                f"❌ Не удалось отправить файл: {e}"[:300],
                max_admin_menu_rows(),
                is_group=is_g,
            )
        _clear(uid)
        await show_admin_menu(adapter, event)
        return True

    return False


async def _after_text_media(adapter, chat_id, is_g, uid: int, s: Dict, text_original: str, media_json: str) -> None:
    s["text_original"] = text_original
    s["media_json"] = media_json
    improved_text = ""
    if text_original:
        try:
            imp = await asyncio.to_thread(improve_broadcast_text, text_original)
            improved_text = imp.get("suggested", text_original) or imp.get("fixed", text_original) or text_original
        except Exception as e:
            logger.warning("[MAX_ADMIN] improve: %s", e)
            improved_text = text_original
    else:
        improved_text = ""
    need = bool(text_original) and bool(improved_text) and improved_text.strip() != text_original.strip()
    if need:
        s["improved_text"] = improved_text
        s["step"] = "pick_variant"
        variant_body = _max_variant_choice_message(improved_text, media_json)
        await _send(
            adapter,
            chat_id,
            variant_body,
            _rows(
                ("Оригинал", "broadcast:variant:original"),
                ("Улучшенный", "broadcast:variant:improved"),
            ),
            is_group=is_g,
        )
        return
    s["text_final"] = improved_text or text_original
    s["selected_variant"] = "improved" if improved_text else "original"
    s["step"] = "preview"
    await _send_preview(adapter, chat_id, is_g, uid, s)


async def _send_preview(adapter, chat_id, is_g, uid: int, s: Dict) -> None:
    tf = s.get("text_final", "")
    mj = s.get("media_json", "")
    rows = _rows(
        ("🧪 Тест себе", "broadcast:aud:test_self"),
        ("✏️ Изменить текст", "broadcast:edit_text_max"),
        ("❌ Отмена", BROADCAST_CANCEL),
    )
    body = "📋 <b>Превью</b>\n\n" + (tf or "_(только медиа)_")
    await _send(adapter, chat_id, body, rows, is_group=is_g)


async def handle_admin_message(adapter, max_client: Any, event: IncomingMessage, uid: int) -> bool:
    if not is_admin_for_platform("max", uid):
        return False
    text = (event.text or "").strip()
    is_g = event.chat.is_group

    if text == "/cancel":
        if _busy(uid):
            _clear(uid)
            await _send(
                adapter,
                event.chat.id,
                "Операция отменена.",
                max_admin_menu_rows(),
                is_group=is_g,
            )
            return True
        return False

    s = _sessions.get(uid)
    if not s:
        return False

    if s.get("kind") == "broadcast":
        if s.get("step") == "wait_text":
            to = "" if text == "-" else text
            atts = message_attachments_from_incoming(event)
            if atts:
                items = []
                for a in atts:
                    it = _max_broadcast_item_from_attachment(a)
                    if it:
                        items.append(it)
                if items:
                    mj = _merge_max_media_json(s.get("media_json", ""), items)
                    await _after_text_media(adapter, event.chat.id, is_g, uid, s, to, mj)
                    return True
            s["text_original"] = to
            s["step"] = "wait_media"
            await _send(
                adapter,
                event.chat.id,
                "📎 Пришлите фото/файл или нажмите «Пропустить медиа».",
                [[KeyboardButton(text="⏭ Пропустить медиа", callback_data=BROADCAST_SKIP_MEDIA)]],
                is_group=is_g,
            )
            return True
        if s.get("step") == "wait_media":
            atts = message_attachments_from_incoming(event)
            if atts:
                items = []
                for a in atts:
                    it = _max_broadcast_item_from_attachment(a)
                    if it:
                        items.append(it)
                if items:
                    mj = _merge_max_media_json(s.get("media_json", ""), items)
                    to = s.get("text_original", "")
                    await _after_text_media(adapter, event.chat.id, is_g, uid, s, to, mj)
                    return True
        if s.get("step") == "wait_month_day":
            try:
                day = int(text)
            except ValueError:
                await _send(adapter, event.chat.id, "Введите число 1–31.", None, is_group=is_g)
                return True
            if day < 1 or day > 31:
                await _send(adapter, event.chat.id, "Введите число 1–31.", None, is_group=is_g)
                return True
            mode = s.get("pending_send_mode", "chats")
            tf = s.get("text_final", "")
            mj = s.get("media_json", "")
            extra = (
                s.get("pending_mode_extra")
                if mode in ("selected_chats", "selected_regions")
                else None
            )
            mode_extra_str = json.dumps(extra, ensure_ascii=False) if extra else "{}"
            title = await asyncio.to_thread(generate_broadcast_title, tf or "Рассылка")
            schedule_id = await asyncio.to_thread(
                create_scheduled_broadcast,
                created_by_user_id=uid,
                created_by_username=event.user.username,
                text_final=tf,
                media_json=mj,
                mode=mode,
                mode_extra=mode_extra_str,
                schedule_type="monthly",
                schedule_config={"day": day},
                title=title,
            )
            nxt = compute_next_run_iso("monthly", {"day": day})
            await _send(
                adapter,
                event.chat.id,
                f"✅ Плановая рассылка создана.\nСледующий запуск: {nxt[:16]} UTC\nID: <code>{schedule_id}</code>",
                max_admin_menu_rows(),
                is_group=is_g,
            )
            _clear(uid)
            return True

    if s.get("kind") == "kb" and s.get("step") == "wait_file":
        if not event.attachments:
            await _send(adapter, event.chat.id, "Пришлите файл документа.", None, is_group=is_g)
            return True
        att = event.attachments[0]
        fid = att.get("id_or_url") or att.get("file_id")
        if not fid:
            return True
        du = max_attachment_https_url(att)
        mid = incoming_max_message_id(event)
        try:
            if du:
                content = await adapter.download_file(du)
            else:
                content = await adapter.download_file(str(fid), message_id=mid)
        except Exception as e:
            logger.exception("[MAX_ADMIN] kb download: %s", e)
            await _send(adapter, event.chat.id, "❌ Не удалось скачать файл.", max_admin_menu_rows(), is_group=is_g)
            _clear(uid)
            return True
        fn = "upload.bin"
        s["kb_bytes"] = content
        s["kb_filename"] = fn
        s["step"] = "wait_title"
        await _send(adapter, event.chat.id, "Введите название документа для базы знаний:", None, is_group=is_g)
        return True

    if s.get("kind") == "kb" and s.get("step") == "wait_title":
        title = text
        if not title:
            await _send(adapter, event.chat.id, "Название не может быть пустым.", None, is_group=is_g)
            return True
        b = s.get("kb_bytes")
        fn = s.get("kb_filename", "doc")
        mid = await _send(adapter, event.chat.id, "⏳ Обработка...", None, is_group=is_g)

        async def notify(html: str) -> None:
            if mid:
                try:
                    await adapter.edit_message(event.chat.id, mid, html)
                except Exception:
                    await _send(adapter, event.chat.id, html, None, is_group=is_g)
            else:
                await _send(adapter, event.chat.id, html, None, is_group=is_g)

        ok = await process_kb_document_pipeline(b, fn, title, uid, notify)
        _clear(uid)
        if ok:
            await _send(
                adapter,
                event.chat.id,
                "Готово. Выберите следующее действие:",
                max_admin_menu_rows(),
                is_group=is_g,
            )
        return True

    if s.get("kind") == "doc" and s.get("step") == "field":
        tpl = s.get("template")
        collected = s.get("collected") or {}
        nf = s.get("next_field", 0)
        if not tpl:
            _clear(uid)
            return True
        if nf < 3:
            key = DOC_GEN_FIELD_ORDER[nf]
            collected[key] = text
            s["collected"] = collected
            s["next_field"] = nf + 1
            await _send(
                adapter,
                event.chat.id,
                f"{DOC_GEN_PROMPTS[nf + 1]}\n\n/cancel — отмена.",
                [[KeyboardButton(text="❌ Отмена", callback_data=DOC_GEN_CANCEL)]],
                is_group=is_g,
            )
            return True
        await adapter.send_typing(event.chat.id, is_group_chat=is_g)
        try:
            extracted = await asyncio.to_thread(extract_fields_from_text, text, REQUISITES_FIELD_SPECS)
        except Exception as e:
            logger.exception("[MAX_ADMIN] extract: %s", e)
            await _send(adapter, event.chat.id, "Не удалось разобрать реквизиты. Попробуйте снова.", None, is_group=is_g)
            return True
        for k, v in (extracted or {}).items():
            if isinstance(v, str):
                collected[k] = v.strip()
            elif v is not None:
                collected[k] = v
        for spec in REQUISITES_FIELD_SPECS:
            k = spec["key"]
            collected.setdefault(k, "")
        s["collected"] = collected
        preview = _format_fields_preview(collected, REQUISITES_FIELD_SPECS)
        s["step"] = "confirm"
        await _send(
            adapter,
            event.chat.id,
            f"Проверьте данные:\n\n{preview}",
            _rows(("✅ Создать DOCX", DOC_GEN_CONFIRM), ("❌ Отмена", DOC_GEN_CANCEL)),
            is_group=is_g,
        )
        return True

    return False


async def handle_admin_doc_attachment(adapter, event: IncomingMessage, uid: int) -> bool:
    s = _sessions.get(uid)
    if not s or s.get("kind") != "doc" or s.get("step") != "field" or s.get("next_field") != 3:
        return False
    tpl = s.get("template")
    if not tpl:
        return False
    if not event.attachments:
        return False
    att = event.attachments[0]
    fid = att.get("id_or_url") or att.get("file_id")
    if not fid:
        return False
    du = max_attachment_https_url(att)
    mid = incoming_max_message_id(event)
    try:
        if du:
            raw = await adapter.download_file(du)
        else:
            raw = await adapter.download_file(str(fid), message_id=mid)
    except Exception as e:
        logger.exception("[MAX_ADMIN] doc att: %s", e)
        return True
    mime = str(att.get("type", "")).lower()
    collected = s.get("collected") or {}
    try:
        if "image" in mime or mime == "photo":
            ex = await asyncio.to_thread(
                extract_fields_from_image, raw, REQUISITES_FIELD_SPECS, "image/jpeg"
            )
        else:
            ex = await asyncio.to_thread(
                extract_fields_from_file, raw, "upload.bin", REQUISITES_FIELD_SPECS
            )
    except Exception as e:
        logger.exception("[MAX_ADMIN] vision: %s", e)
        await _send(adapter, event.chat.id, "Не удалось извлечь данные из файла.", None, is_group=event.chat.is_group)
        return True
    for k, v in (ex or {}).items():
        if isinstance(v, str):
            collected[k] = v.strip()
        elif v is not None:
            collected[k] = v
    for spec in REQUISITES_FIELD_SPECS:
        collected.setdefault(spec["key"], "")
    s["collected"] = collected
    s["step"] = "confirm"
    preview = _format_fields_preview(collected, REQUISITES_FIELD_SPECS)
    await _send(
        adapter,
        event.chat.id,
        f"Проверьте данные:\n\n{preview}",
        _rows(("✅ Создать DOCX", DOC_GEN_CONFIRM), ("❌ Отмена", DOC_GEN_CANCEL)),
        is_group=event.chat.is_group,
    )
    return True
