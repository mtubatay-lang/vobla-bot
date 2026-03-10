"""Хендлеры создания документов из шаблонов (раздел администратора)."""

import asyncio
import logging

from aiogram import Router, F
from aiogram.enums import ParseMode, ChatAction
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, BufferedInputFile

from app.core.callbacks import (
    DOC_GEN_START,
    DOC_GEN_TEMPLATE_PREFIX,
    DOC_GEN_CONFIRM,
    DOC_GEN_CANCEL,
    DOC_GEN_RETRY,
    DOC_GEN_UPLOAD,
)
from app.handlers.broadcast import _require_admin
from app.services.document_template_service import (
    get_templates_list,
    get_template_by_id,
    get_template_path,
    fill_docx_template,
    extract_fields_from_text,
    extract_fields_from_image,
    extract_fields_from_file,
    extract_placeholder_keys_from_docx,
    save_template_file,
    add_template_to_config,
    _slug,
)

logger = logging.getLogger(__name__)

router = Router()

# Порядок пошагового ввода полей при создании документа
DOC_GEN_FIELD_ORDER = ["contract_number", "contract_date", "contract_end_date", "customer_details"]

# Тексты запросов по шагам (индекс 0..3)
DOC_GEN_PROMPTS = [
    "Введите номер договора (например 015-2508).",
    "Введите дату договора (например 22 февраля 2026).",
    "Введите срок действия договора — до какой даты (например 31 декабря 2026).",
    "Загрузите реквизиты Заказчика (ФИО/название ИП, ИНН, адрес, р/с, БИК): отправьте текст, скриншот/фото или PDF/DOCX.",
]


class DocumentGenState(StatesGroup):
    choosing_template = State()
    waiting_data = State()
    confirming = State()


class TemplateUploadState(StatesGroup):
    waiting_file = State()
    waiting_name = State()


def _format_fields_preview(extracted: dict, field_specs: list) -> str:
    """Форматирует превью извлечённых полей для сообщения."""
    lines = []
    for spec in field_specs:
        key = spec.get("key", "")
        label = spec.get("label", key)
        value = (extracted.get(key) or "").strip()
        if len(value) > 200:
            value = value[:197] + "..."
        lines.append(f"<b>{label}</b>\n{value or '—'}")
    return "\n\n".join(lines)


@router.callback_query(F.data == DOC_GEN_START)
async def doc_gen_start_callback(cb: CallbackQuery, state: FSMContext) -> None:
    """Кнопка «Создать документ»: показать выбор шаблона."""
    if not await _require_admin(cb):
        return

    templates = get_templates_list()
    if not templates:
        await state.set_state(TemplateUploadState.waiting_file)
        await cb.message.answer(
            "📝 Нет доступных шаблонов. Отправьте DOCX-файл с плейсхолдерами в формате {{field_key}} (например {{contract_number}}, {{contract_date}}).\n\nДля отмены: /cancel",
            parse_mode=ParseMode.HTML,
        )
        await cb.answer()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t["name"], callback_data=f"{DOC_GEN_TEMPLATE_PREFIX}{t['id']}")]
        for t in templates
    ] + [[InlineKeyboardButton(text="➕ Добавить шаблон", callback_data=DOC_GEN_UPLOAD)]])
    await state.set_state(DocumentGenState.choosing_template)
    await cb.message.answer(
        "📝 <b>Создание документа</b>\n\nВыберите тип документа:",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.callback_query(F.data.startswith(DOC_GEN_TEMPLATE_PREFIX), DocumentGenState.choosing_template)
async def doc_gen_choose_template_callback(cb: CallbackQuery, state: FSMContext) -> None:
    """Выбор шаблона: сохраняем template_id, переходим в ожидание данных."""
    if not await _require_admin(cb):
        return

    template_id = cb.data[len(DOC_GEN_TEMPLATE_PREFIX):].strip()
    template = get_template_by_id(template_id)
    if not template:
        await cb.answer("Шаблон не найден.", show_alert=True)
        return

    template_path = get_template_path(template)
    if not template_path.is_file():
        await cb.answer(f"Файл шаблона не найден: {template_path.name}", show_alert=True)
        return

    await state.update_data(
        doc_gen_template_id=template_id,
        doc_gen_template=template,
        doc_gen_collected={},
        doc_gen_next_field=0,
    )
    await state.set_state(DocumentGenState.waiting_data)

    await cb.message.answer(
        f"📄 <b>{template.get('name', template_id)}</b>\n\n"
        f"{DOC_GEN_PROMPTS[0]}\n\nДля отмены отправьте /cancel",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.callback_query(F.data == DOC_GEN_UPLOAD)
async def doc_gen_upload_callback(cb: CallbackQuery, state: FSMContext) -> None:
    """Кнопка «Добавить шаблон»: переходим в ожидание DOCX-файла."""
    if not await _require_admin(cb):
        return
    await state.set_state(TemplateUploadState.waiting_file)
    await cb.message.answer(
        "📤 Отправьте DOCX-файл с плейсхолдерами в формате {{field_key}} (например {{contract_number}}, {{contract_date}}, {{customer_details}}).\n\nДля отмены: /cancel",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.message(Command("cancel"), DocumentGenState.waiting_data)
@router.message(Command("cancel"), DocumentGenState.confirming)
@router.message(Command("cancel"), TemplateUploadState.waiting_file)
@router.message(Command("cancel"), TemplateUploadState.waiting_name)
async def doc_gen_cancel(message: Message, state: FSMContext) -> None:
    """Отмена создания документа или загрузки шаблона."""
    if not await _require_admin(message):
        return
    await state.clear()
    await message.answer("Создание документа отменено.")


@router.message(TemplateUploadState.waiting_file, F.document)
async def template_upload_receive_file(message: Message, state: FSMContext) -> None:
    """Приём DOCX-файла для нового шаблона: извлекаем плейсхолдеры, запрашиваем название."""
    if not await _require_admin(message):
        return
    doc = message.document
    if not doc or not doc.file_name or not doc.file_name.lower().endswith(".docx"):
        await message.answer("Отправьте файл в формате DOCX.")
        return
    try:
        file = await message.bot.get_file(doc.file_id)
        file_bytes_io = await message.bot.download_file(file.file_path)
        file_bytes = file_bytes_io.read()
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка загрузки DOCX для шаблона: %s", e)
        await message.answer("Не удалось загрузить файл. Попробуйте ещё раз.")
        return
    try:
        keys = extract_placeholder_keys_from_docx(file_bytes)
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка разбора DOCX: %s", e)
        await message.answer("Не удалось прочитать файл. Убедитесь, что это DOCX с плейсхолдерами {{field_key}}.")
        return
    if not keys:
        await message.answer("В файле не найдены плейсхолдеры формата {{field_key}}. Добавьте их в документ и отправьте снова.")
        return
    await state.update_data(
        template_upload_bytes=file_bytes,
        template_upload_keys=keys,
    )
    await state.set_state(TemplateUploadState.waiting_name)
    await message.answer(
        f"Найдены поля: {', '.join(keys)}\n\nВведите название шаблона для списка (например: Договор поставки):",
        parse_mode=ParseMode.HTML,
    )


@router.message(TemplateUploadState.waiting_name, F.text)
async def template_upload_receive_name(message: Message, state: FSMContext) -> None:
    """Название шаблона: сохраняем файл и запись в config.json."""
    if not await _require_admin(message):
        return
    name = (message.text or "").strip()
    if not name:
        await message.answer("Введите название шаблона.")
        return
    data = await state.get_data()
    file_bytes = data.get("template_upload_bytes")
    keys = data.get("template_upload_keys")
    if not file_bytes or not keys:
        await state.clear()
        await message.answer("Данные утрачены. Начните заново: /admin → Создать документ → Добавить шаблон.")
        return
    template_id = _slug(name)
    filename = f"{template_id}.docx"
    try:
        save_template_file(filename, file_bytes)
        add_template_to_config(template_id, name, filename, keys)
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка сохранения шаблона: %s", e)
        await message.answer("Ошибка при сохранении шаблона. Проверьте права на папку templates/.")
        return
    await state.clear()
    await message.answer(f"✅ Шаблон «{name}» добавлен. Теперь его можно выбрать при создании документа.")


@router.message(DocumentGenState.waiting_data, F.text)
async def doc_gen_receive_text(message: Message, state: FSMContext) -> None:
    """Пошаговый ввод: номер → дата → срок → реквизиты (текст)."""
    if not await _require_admin(message):
        return

    data = await state.get_data()
    template = data.get("doc_gen_template")
    collected = data.get("doc_gen_collected") or {}
    next_field = data.get("doc_gen_next_field", 0)

    if not template:
        await state.clear()
        await message.answer("Ошибка: шаблон не выбран. Начните заново через /admin → Создать документ.")
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer(f"{DOC_GEN_PROMPTS[next_field]}\n\nДля отмены отправьте /cancel.")
        return

    key = DOC_GEN_FIELD_ORDER[next_field]
    collected[key] = text

    if next_field < 3:
        await state.update_data(doc_gen_collected=collected, doc_gen_next_field=next_field + 1)
        next_prompt = DOC_GEN_PROMPTS[next_field + 1]
        await message.answer(f"{next_prompt}\n\nДля отмены отправьте /cancel.")
        return

    # next_field == 3: реквизиты получены, показываем превью
    await state.update_data(doc_gen_collected=collected)
    await _show_preview_and_confirm(message, state, template, collected)


@router.message(DocumentGenState.waiting_data, F.photo)
async def doc_gen_receive_photo(message: Message, state: FSMContext) -> None:
    """Фото/скриншот реквизитов (только на шаге 4): извлекаем customer_details через Vision."""
    if not await _require_admin(message):
        return

    data = await state.get_data()
    template = data.get("doc_gen_template")
    collected = data.get("doc_gen_collected") or {}
    next_field = data.get("doc_gen_next_field", 0)

    if not template:
        await state.clear()
        await message.answer("Ошибка: шаблон не выбран. Начните заново через /admin → Создать документ.")
        return

    if next_field != 3:
        await message.answer(f"{DOC_GEN_PROMPTS[next_field]}\n\nДля отмены отправьте /cancel.")
        return

    photo = message.photo[-1] if message.photo else None
    if not photo:
        await message.answer("Не удалось получить изображение.")
        return

    await message.bot.send_chat_action(message.chat.id, action=ChatAction.TYPING)
    try:
        file = await message.bot.get_file(photo.file_id)
        file_bytes = await message.bot.download_file(file.file_path)
        image_bytes = file_bytes.read()
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка загрузки фото: %s", e)
        await message.answer("Не удалось загрузить изображение. Попробуйте ещё раз или отправьте текст.")
        return

    field_specs = [{"key": "customer_details", "label": "Реквизиты Заказчика"}]
    try:
        extracted = await asyncio.to_thread(
            extract_fields_from_image,
            image_bytes,
            field_specs,
            "image/jpeg",
        )
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка извлечения полей из фото: %s", e)
        await message.answer("Не удалось распознать реквизиты на изображении. Попробуйте отправить текст или документ.")
        return

    collected["customer_details"] = (extracted.get("customer_details") or "").strip()
    await state.update_data(doc_gen_collected=collected)
    await _show_preview_and_confirm(message, state, template, collected)


@router.message(DocumentGenState.waiting_data, F.document)
async def doc_gen_receive_document(message: Message, state: FSMContext) -> None:
    """Документ с реквизитами (только на шаге 4): извлекаем текст и customer_details через AI."""
    if not await _require_admin(message):
        return

    data = await state.get_data()
    template = data.get("doc_gen_template")
    collected = data.get("doc_gen_collected") or {}
    next_field = data.get("doc_gen_next_field", 0)

    if not template:
        await state.clear()
        await message.answer("Ошибка: шаблон не выбран. Начните заново через /admin → Создать документ.")
        return

    if next_field != 3:
        await message.answer(f"{DOC_GEN_PROMPTS[next_field]}\n\nДля отмены отправьте /cancel.")
        return

    doc = message.document
    if not doc or not doc.file_name:
        await message.answer("Отправьте файл с именем (PDF или DOCX).")
        return

    filename_lower = doc.file_name.lower()
    if not (filename_lower.endswith(".pdf") or filename_lower.endswith(".docx")):
        await message.answer("Поддерживаются только PDF и DOCX. Отправьте файл нужного формата или текст.")
        return

    await message.bot.send_chat_action(message.chat.id, action=ChatAction.TYPING)
    try:
        file = await message.bot.get_file(doc.file_id)
        file_bytes_io = await message.bot.download_file(file.file_path)
        file_bytes = file_bytes_io.read()
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка загрузки документа: %s", e)
        await message.answer("Не удалось загрузить файл. Попробуйте ещё раз.")
        return

    field_specs = [{"key": "customer_details", "label": "Реквизиты Заказчика"}]
    try:
        extracted = await asyncio.to_thread(
            extract_fields_from_file,
            file_bytes,
            doc.file_name,
            field_specs,
        )
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка извлечения полей из файла: %s", e)
        await message.answer("Не удалось извлечь реквизиты из файла. Попробуйте отправить текст.")
        return

    collected["customer_details"] = (extracted.get("customer_details") or "").strip()
    await state.update_data(doc_gen_collected=collected)
    await _show_preview_and_confirm(message, state, template, collected)


async def _show_preview_and_confirm(
    message: Message,
    state: FSMContext,
    template: dict,
    extracted: dict,
) -> None:
    """Показать превью извлечённых полей и кнопки Подтвердить / Отмена / Повторить."""
    await state.update_data(doc_gen_extracted=extracted)
    await state.set_state(DocumentGenState.confirming)

    field_specs = template.get("fields", [])
    preview_text = _format_fields_preview(extracted, field_specs)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить и получить документ", callback_data=DOC_GEN_CONFIRM)],
        [
            InlineKeyboardButton(text="🔄 Ввести данные заново", callback_data=DOC_GEN_RETRY),
            InlineKeyboardButton(text="❌ Отмена", callback_data=DOC_GEN_CANCEL),
        ],
    ])
    await message.answer(
        "📋 <b>Проверьте данные</b>\n\n" + preview_text + "\n\nПодтвердите или введите данные заново.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data == DOC_GEN_CONFIRM, DocumentGenState.confirming)
async def doc_gen_confirm_callback(cb: CallbackQuery, state: FSMContext) -> None:
    """Подтверждение: заполняем шаблон и отправляем DOCX."""
    if not await _require_admin(cb):
        return

    data = await state.get_data()
    template = data.get("doc_gen_template")
    extracted = data.get("doc_gen_extracted")
    if not template or not extracted:
        await cb.answer("Данные утрачены. Начните заново.", show_alert=True)
        await state.clear()
        return

    template_path = get_template_path(template)
    if not template_path.is_file():
        await cb.answer("Файл шаблона не найден.", show_alert=True)
        await state.clear()
        return

    try:
        docx_bytes = fill_docx_template(template_path, extracted)
    except Exception as e:
        logger.exception("[DOC_GEN] Ошибка заполнения шаблона: %s", e)
        await cb.answer("Ошибка при заполнении шаблона.", show_alert=True)
        return

    template_name = template.get("name", "document")
    safe_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in template_name).strip() or "document"
    filename = f"{safe_name}.docx"

    chat_id = cb.message.chat.id if cb.message else 0
    doc_file = BufferedInputFile(docx_bytes, filename=filename)
    await cb.message.bot.send_document(
        chat_id=chat_id,
        document=doc_file,
        caption="📄 Документ готов.",
        parse_mode=ParseMode.HTML,
    )

    await state.clear()
    await cb.answer("Документ отправлен.")


@router.callback_query(F.data == DOC_GEN_CANCEL, DocumentGenState.confirming)
async def doc_gen_cancel_callback(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена из экрана подтверждения."""
    if not await _require_admin(cb):
        return
    await state.clear()
    if cb.message:
        await cb.message.answer("Создание документа отменено.")
    await cb.answer()


@router.callback_query(F.data == DOC_GEN_RETRY, DocumentGenState.confirming)
async def doc_gen_retry_callback(cb: CallbackQuery, state: FSMContext) -> None:
    """Ввести данные заново: сбрасываем к первому шагу."""
    if not await _require_admin(cb):
        return

    data = await state.get_data()
    template = data.get("doc_gen_template")
    if not template:
        await cb.answer("Шаблон не найден.", show_alert=True)
        await state.clear()
        return

    await state.update_data(doc_gen_collected={}, doc_gen_next_field=0)
    await state.set_state(DocumentGenState.waiting_data)
    await cb.message.answer(
        f"📄 {DOC_GEN_PROMPTS[0]}\n\nДля отмены отправьте /cancel",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()
