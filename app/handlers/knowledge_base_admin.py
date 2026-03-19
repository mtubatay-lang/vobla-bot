"""Админ-панель для пополнения базы знаний через загрузку документов."""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery

from app.services.auth_service import find_user_by_telegram_id
from app.services.metrics_service import alog_event
from app.services.faq_migration import migrate_faq_to_qdrant
from app.services.kb_upload_service import process_kb_document_pipeline
from app.handlers.broadcast import _check_admin, _require_admin

logger = logging.getLogger(__name__)

router = Router()


class KnowledgeBaseState(StatesGroup):
    waiting_document = State()
    waiting_title = State()
    processing = State()


@router.message(Command("kb_add"))
async def cmd_kb_add(message: Message, state: FSMContext):
    """Команда /kb_add для начала загрузки документа."""
    if not await _require_admin(message):
        return
    
    await state.set_state(KnowledgeBaseState.waiting_document)
    await message.answer(
        "📚 <b>Пополнение базы знаний</b>\n\n"
        "Отправьте документ (PDF, TXT, DOCX, MD, CSV).\n"
        "Бот предподготовит файл при необходимости, извлечет текст, разобьет на чанки и загрузит в Qdrant.\n\n"
        "Для отмены отправьте /cancel",
        parse_mode="HTML",
    )


@router.callback_query(F.data == "kb_add")
async def kb_add_callback(cb: CallbackQuery, state: FSMContext):
    """Кнопка для начала загрузки документа."""
    if not await _require_admin(cb):
        return
    
    await state.set_state(KnowledgeBaseState.waiting_document)
    await cb.message.answer(
        "📚 <b>Пополнение базы знаний</b>\n\n"
        "Отправьте документ (PDF, TXT, DOCX, MD, CSV).\n"
        "Бот предподготовит файл при необходимости, извлечет текст, разобьет на чанки и загрузит в Qdrant.\n\n"
        "Для отмены отправьте /cancel",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(Command("kb_migrate"))
async def cmd_kb_migrate(message: Message):
    """Команда /kb_migrate для миграции FAQ из Google Sheets в Qdrant."""
    try:
        logger.info(f"[KB_ADMIN] Получена команда /kb_migrate от пользователя {message.from_user.id if message.from_user else 'unknown'}")
        
        if not await _require_admin(message):
            logger.warning(f"[KB_ADMIN] Пользователь {message.from_user.id if message.from_user else 'unknown'} не имеет прав админа")
            return
        
        logger.info(f"[KB_ADMIN] Начинаю миграцию FAQ для админа {message.from_user.id}")
        
        # Отправляем сообщение о начале миграции
        status_msg = await message.answer("⏳ Начинаю миграцию FAQ из Google Sheets в Qdrant...")
        
        # Запускаем асинхронную миграцию
        asyncio.create_task(
            migrate_faq_async(
                message.bot,
                message.chat.id,
                status_msg.message_id,
                message.from_user.id if message.from_user else None,
            )
        )
    except Exception as e:
        logger.exception(f"[KB_ADMIN] Ошибка в обработчике команды /kb_migrate: {e}")
        try:
            await message.answer(f"❌ Произошла ошибка: {str(e)}")
        except:
            pass


async def migrate_faq_async(
    bot,
    chat_id: int,
    status_msg_id: int,
    user_id: Optional[int],
):
    """Асинхронная миграция FAQ из Google Sheets в Qdrant."""
    try:
        async def progress(stage: str, detail: str) -> None:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"⏳ {stage}: {detail}",
            )

        # Обновляем статус
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text="⏳ Читаю FAQ из Google Sheets...",
        )

        result = await migrate_faq_to_qdrant(progress_callback=progress)

        if result["success"]:
            dedup = result.get("deduplicated_groups", 0)
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=(
                    f"✅ <b>Миграция завершена успешно</b>\n\n"
                    f"📊 Обработано FAQ: {result['total_faqs']}\n"
                    f"📋 После дедупликации групп: {dedup}\n"
                    f"📦 Создано чанков в RAG: {result['total_chunks']}"
                ),
                parse_mode="HTML",
            )

            await alog_event(
                user_id=user_id,
                username=None,
                event="kb_migrate_completed",
                meta={
                    "total_faqs": result["total_faqs"],
                    "total_chunks": result["total_chunks"],
                    "deduplicated_groups": dedup,
                },
            )
        else:
            # Ошибка миграции
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=(
                    f"❌ <b>Ошибка миграции</b>\n\n"
                    f"Ошибка: {result.get('error', 'Неизвестная ошибка')}"
                ),
                parse_mode="HTML",
            )
    except Exception as e:
        logger.exception(f"[KB_ADMIN] Ошибка миграции FAQ: {e}")
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"❌ Произошла ошибка при миграции: {str(e)}",
            )
        except:
            pass


@router.message(Command("kb_ingest_kilbil"))
async def cmd_kb_ingest_kilbil(message: Message):
    """Команда /kb_ingest_kilbil — индексация базы знаний help.kilbil.ru в Qdrant."""
    if not await _require_admin(message):
        return

    status_msg = await message.answer("⏳ Запускаю индексацию kilbil (help.kilbil.ru) в Qdrant...")

    asyncio.create_task(
        _run_kilbil_ingest_async(
            message.bot,
            message.chat.id,
            status_msg.message_id,
        )
    )


async def _run_kilbil_ingest_async(bot, chat_id: int, status_msg_id: int):
    """Фоновая индексация kilbil в Qdrant."""
    try:
        async def progress(stage: str, detail: str) -> None:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_msg_id,
                    text=f"⏳ <b>kilbil индексация</b>\n\n{stage}: {detail}",
                )
            except Exception:
                pass

        from app.services.kilbil_ingest_service import run_ingestion

        result = await run_ingestion(fresh=False, progress_callback=progress)

        if result["success"]:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=(
                    f"✅ <b>Индексация kilbil завершена</b>\n\n"
                    f"📊 Статей: {result['articles']}\n"
                    f"📦 Чанков в Qdrant: {result['chunks']}"
                ),
            )
        else:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"❌ <b>Ошибка</b>\n\n{result.get('error', 'Неизвестная ошибка')}",
            )
    except Exception as e:
        logger.exception(f"[KB_ADMIN] Ошибка индексации kilbil: {e}")
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"❌ Ошибка: {str(e)}",
            )
        except Exception:
            pass


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Отмена текущей операции."""
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("❌ Операция отменена")


@router.message(KnowledgeBaseState.waiting_document, F.document)
async def handle_document_upload(message: Message, state: FSMContext):
    """Обработка загруженного документа."""
    if not await _require_admin(message):
        return
    
    document = message.document
    if not document:
        await message.answer("❌ Не удалось получить информацию о документе")
        return
    
    filename = document.file_name or "document"
    
    # Проверяем формат
    filename_lower = filename.lower()
    supported_formats = ['.pdf', '.txt', '.docx', '.md', '.markdown', '.csv']
    if not any(filename_lower.endswith(ext) for ext in supported_formats):
        await message.answer(
            f"❌ Неподдерживаемый формат файла.\n"
            f"Поддерживаются: PDF, TXT, DOCX, MD, CSV"
        )
        return
    
    # Скачиваем файл
    try:
        file = await message.bot.get_file(document.file_id)
        file_bytes = await message.bot.download_file(file.file_path)
        file_content = file_bytes.read()
    except Exception as e:
        logger.exception(f"[KB_ADMIN] Ошибка скачивания файла: {e}")
        await message.answer("❌ Ошибка при скачивании файла")
        return
    
    # Сохраняем данные в state
    await state.update_data(
        file_content=file_content,
        filename=filename,
    )
    
    # Переходим к запросу названия документа
    await state.set_state(KnowledgeBaseState.waiting_title)
    await message.answer(
        f"✅ Файл получен: <b>{filename}</b>\n\n"
        "Введите название документа для базы знаний (например: 'Регламент для франчайзи'):",
        parse_mode="HTML",
    )


@router.message(KnowledgeBaseState.waiting_title, F.text)
async def handle_document_title(message: Message, state: FSMContext):
    """Обработка названия документа."""
    if not await _require_admin(message):
        return
    
    document_title = message.text.strip()
    if not document_title:
        await message.answer("❌ Название не может быть пустым. Введите название документа:")
        return
    
    data = await state.get_data()
    file_content = data.get("file_content")
    filename = data.get("filename")
    
    if not file_content:
        await message.answer("❌ Ошибка: файл не найден. Начните заново: /kb_add")
        await state.clear()
        return
    
    # Переходим в состояние обработки
    await state.set_state(KnowledgeBaseState.processing)
    
    # Отправляем сообщение о начале обработки
    status_msg = await message.answer("⏳ Начинаю обработку документа...")
    
    # Запускаем асинхронную обработку
    asyncio.create_task(
        process_document_async(
            message.bot,
            message.chat.id,
            status_msg.message_id,
            file_content,
            filename,
            document_title,
            message.from_user.id if message.from_user else None,
            state,
        )
    )


async def process_document_async(
    bot,
    chat_id: int,
    status_msg_id: int,
    file_content: bytes,
    filename: str,
    document_title: str,
    user_id: Optional[int],
    state: FSMContext,
):
    """Асинхронная обработка документа: предподготовка (если применимо), извлечение текста, чанкинг, обогащение, загрузка в Qdrant."""
    try:

        async def notify(html: str) -> None:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=html,
                parse_mode="HTML",
            )

        ok = await process_kb_document_pipeline(
            file_content,
            filename,
            document_title,
            user_id,
            notify,
        )
        if ok:
            await state.clear()
    except Exception as e:
        logger.exception(f"[KB_ADMIN] Неожиданная ошибка при обработке документа: {e}")
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"❌ Произошла ошибка при обработке документа: {str(e)}",
            )
        except Exception:
            pass


async def save_manager_answer_to_qdrant(
    question: str,
    answer: str,
    manager_user_id: int,
    chat_id: int,
    media_json: str = "",
) -> None:
    """Сохраняет ответ менеджера в Qdrant с автоматической обработкой.
    
    Args:
        question: Вопрос пользователя
        answer: Ответ менеджера
        manager_user_id: ID менеджера
        chat_id: ID чата
        media_json: JSON строка с медиа-вложениями (опционально)
    """
    try:
        from app.services.chunking_service import semantic_chunk_text, extract_metadata_from_text
        from app.services.context_enrichment import enrich_chunks_batch
        from app.services.openai_client import create_embedding
        from app.services.qdrant_service import get_qdrant_service
        from datetime import datetime
        
        # 1. Создаем текст: вопрос + ответ
        full_text = f"Вопрос: {question}\nОтвет: {answer}"
        
        # 2. Разбиваем на чанки семантически
        chunks = semantic_chunk_text(full_text)
        if not chunks:
            chunks = [{
                "text": full_text,
                "chunk_index": 0,
                "total_chunks": 1,
                "start_char": 0,
                "end_char": len(full_text),
            }]
        
        # 3. Обогащаем контекстом
        document_title = f"Ответ менеджера на вопрос"
        enriched_chunks = await enrich_chunks_batch(chunks, document_title)
        
        # 4. Извлекаем метаданные из текста
        extracted_metadata = extract_metadata_from_text(full_text, source="manager_answer")
        
        # 5. Создаем эмбеддинги
        embeddings = []
        for chunk in enriched_chunks:
            embedding = await asyncio.to_thread(
                create_embedding,
                chunk.get("text", ""),
            )
            embeddings.append(embedding)
        
        # 6. Подготавливаем метаданные с расширенными полями
        timestamp = datetime.now().isoformat()
        chunks_with_metadata = []
        for chunk in enriched_chunks:
            chunks_with_metadata.append({
                "text": chunk.get("text", ""),
                "metadata": {
                    "source": "manager_answer",
                    "document_type": extracted_metadata.get("document_type", "faq"),
                    "category": extracted_metadata.get("category", "общее"),
                    "tags": extracted_metadata.get("tags", []),
                    "keywords": extracted_metadata.get("keywords", []),
                    "question": question,
                    "answer": answer,
                    "manager_id": manager_user_id,
                    "chat_id": chat_id,
                    "answered_at": timestamp,
                    "media_json": media_json,
                },
            })
        
        # 6. Загружаем в Qdrant
        qdrant_service = get_qdrant_service()
        qdrant_service.add_documents(chunks_with_metadata, embeddings)
        
        logger.info(f"[KB_ADMIN] Ответ менеджера сохранен в Qdrant: question={question[:50]}...")
    except Exception as e:
        logger.exception(f"[KB_ADMIN] Ошибка сохранения ответа менеджера в Qdrant: {e}")
        raise
        await state.clear()


# Логирование при импорте модуля
logger.info("[KB_ADMIN] Модуль загружен, обработчики команд: kb_add, kb_migrate, kb_ingest_kilbil, cancel")
