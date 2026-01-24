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
from app.services.qdrant_service import get_qdrant_service
from app.services.document_processor import extract_text
from app.services.chunking_service import chunk_text
from app.services.context_enrichment import enrich_chunks_batch
from app.services.openai_client import create_embedding
from app.services.metrics_service import alog_event
from app.services.faq_migration import migrate_faq_to_qdrant
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
        "Отправьте документ (PDF, TXT, DOCX, MD).\n"
        "Бот автоматически извлечет текст, разобьет на чанки, обогатит контекстом и загрузит в Qdrant.\n\n"
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
        "Отправьте документ (PDF, TXT, DOCX, MD).\n"
        "Бот автоматически извлечет текст, разобьет на чанки, обогатит контекстом и загрузит в Qdrant.\n\n"
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
        # Обновляем статус
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text="⏳ Читаю FAQ из Google Sheets...",
        )
        
        # Выполняем миграцию
        result = await migrate_faq_to_qdrant()
        
        if result["success"]:
            # Успешная миграция
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=(
                    f"✅ <b>Миграция завершена успешно</b>\n\n"
                    f"📊 Обработано FAQ: {result['total_faqs']}\n"
                    f"📦 Создано чанков: {result['total_chunks']}"
                ),
                parse_mode="HTML",
            )
            
            # Логируем событие
            await alog_event(
                user_id=user_id,
                username=None,
                event="kb_migrate_completed",
                meta={
                    "total_faqs": result["total_faqs"],
                    "total_chunks": result["total_chunks"],
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
    supported_formats = ['.pdf', '.txt', '.docx', '.md', '.markdown']
    if not any(filename_lower.endswith(ext) for ext in supported_formats):
        await message.answer(
            f"❌ Неподдерживаемый формат файла.\n"
            f"Поддерживаются: PDF, TXT, DOCX, MD"
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
    """Асинхронная обработка документа: извлечение текста, чанкинг, обогащение, загрузка в Qdrant."""
    try:
        # Обновляем статус
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text="⏳ Извлекаю текст из документа...",
        )
        
        # 1. Извлечение текста
        try:
            text = extract_text(file_content, filename)
            if not text or not text.strip():
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_msg_id,
                    text="❌ Не удалось извлечь текст из документа",
                )
                await state.clear()
                return
        except Exception as e:
            logger.exception(f"[KB_ADMIN] Ошибка извлечения текста: {e}")
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"❌ Ошибка извлечения текста: {str(e)}",
            )
            await state.clear()
            return
        
        # 2. Разбивка на чанки
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text="⏳ Разбиваю документ на чанки...",
        )
        
        chunks = chunk_text(text)
        if not chunks:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text="❌ Не удалось разбить документ на чанки",
            )
            await state.clear()
            return
        
        # 3. Обогащение контекстом
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text=f"⏳ Обогащаю {len(chunks)} чанков контекстом...",
        )
        
        try:
            enriched_chunks = await enrich_chunks_batch(chunks, document_title)
        except Exception as e:
            logger.exception(f"[KB_ADMIN] Ошибка обогащения: {e}")
            # Продолжаем с оригинальными чанками
            enriched_chunks = chunks
        
        # 4. Создание эмбеддингов
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text=f"⏳ Создаю эмбеддинги для {len(enriched_chunks)} чанков...",
        )
        
        embeddings = []
        for chunk in enriched_chunks:
            try:
                embedding = await asyncio.to_thread(
                    create_embedding,
                    chunk.get("text", ""),
                )
                embeddings.append(embedding)
            except Exception as e:
                logger.exception(f"[KB_ADMIN] Ошибка создания эмбеддинга: {e}")
                # Создаем нулевой эмбеддинг как fallback
                embeddings.append([0.0] * 1536)
        
        # 5. Подготовка метаданных
        timestamp = datetime.now().isoformat()
        chunks_with_metadata = []
        for chunk in enriched_chunks:
            chunks_with_metadata.append({
                "text": chunk.get("text", ""),
                "metadata": {
                    "source": "manual_upload",
                    "document_title": document_title,
                    "filename": filename,
                    "chunk_index": chunk.get("chunk_index", 0),
                    "total_chunks": chunk.get("total_chunks", len(enriched_chunks)),
                    "uploaded_by": user_id,
                    "uploaded_at": timestamp,
                },
            })
        
        # 6. Загрузка в Qdrant
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text="⏳ Загружаю в Qdrant...",
        )
        
        try:
            qdrant_service = get_qdrant_service()
            qdrant_service.add_documents(chunks_with_metadata, embeddings)
        except Exception as e:
            logger.exception(f"[KB_ADMIN] Ошибка загрузки в Qdrant: {e}")
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"❌ Ошибка загрузки в Qdrant: {str(e)}",
            )
            await state.clear()
            return
        
        # 7. Финальное сообщение
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg_id,
            text=(
                f"✅ <b>Документ успешно добавлен в базу знаний</b>\n\n"
                f"📄 Название: {document_title}\n"
                f"📊 Создано чанков: {len(enriched_chunks)}\n"
                f"📁 Файл: {filename}"
            ),
            parse_mode="HTML",
        )
        
        # Логируем событие
        await alog_event(
            user_id=user_id,
            username=None,
            event="kb_document_uploaded",
            meta={
                "document_title": document_title,
                "filename": filename,
                "chunks_count": len(enriched_chunks),
            },
        )
        
        await state.clear()
        
    except Exception as e:
        logger.exception(f"[KB_ADMIN] Неожиданная ошибка при обработке документа: {e}")
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f"❌ Произошла ошибка при обработке документа: {str(e)}",
            )
        except:
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
        from app.services.chunking_service import chunk_text
        from app.services.context_enrichment import enrich_chunks_batch
        from app.services.openai_client import create_embedding
        from app.services.qdrant_service import get_qdrant_service
        from datetime import datetime
        
        # 1. Создаем текст: вопрос + ответ
        full_text = f"Вопрос: {question}\nОтвет: {answer}"
        
        # 2. Разбиваем на чанки (если длинный)
        chunks = chunk_text(full_text)
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
        
        # 4. Создаем эмбеддинги
        embeddings = []
        for chunk in enriched_chunks:
            embedding = await asyncio.to_thread(
                create_embedding,
                chunk.get("text", ""),
            )
            embeddings.append(embedding)
        
        # 5. Подготавливаем метаданные
        timestamp = datetime.now().isoformat()
        chunks_with_metadata = []
        for chunk in enriched_chunks:
            chunks_with_metadata.append({
                "text": chunk.get("text", ""),
                "metadata": {
                    "source": "manager_answer",
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
logger.info("[KB_ADMIN] Модуль загружен, обработчики команд: kb_add, kb_migrate, cancel")
