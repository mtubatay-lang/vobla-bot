"""Unit tests for group chat QA (RAG) handler."""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from app.handlers.group_chat_qa import (
    _get_question_text_from_message,
    _is_top_score_sufficient_for_answer,
    _is_question,
)


# --- _get_question_text_from_message ---


@pytest.mark.asyncio
async def test_get_question_text_from_text_message():
    """Текст в сообщении возвращается без пробелов по краям."""
    message = MagicMock()
    message.text = "  Какой документ подписать?  "
    message.voice = None
    message.photo = None
    result = await _get_question_text_from_message(message)
    assert result == "Какой документ подписать?"


@pytest.mark.asyncio
async def test_get_question_text_from_photo_without_caption_returns_none():
    """Фото без подписи — не извлекаем текст (None)."""
    message = MagicMock()
    message.text = None
    message.voice = None
    message.photo = [MagicMock()]
    message.caption = None
    result = await _get_question_text_from_message(message)
    assert result is None


@pytest.mark.asyncio
async def test_get_question_text_from_photo_with_caption_returns_caption():
    """Фото с подписью — возвращается подпись как текст вопроса."""
    message = MagicMock()
    message.text = None
    message.voice = None
    message.photo = [MagicMock()]
    message.caption = "  Регламент по открытию магазина  "
    result = await _get_question_text_from_message(message)
    assert result == "Регламент по открытию магазина"


@pytest.mark.asyncio
async def test_get_question_text_empty_caption_returns_none():
    """Фото с пустой подписью (пробелы) — None."""
    message = MagicMock()
    message.text = None
    message.voice = None
    message.photo = [MagicMock()]
    message.caption = "   "
    result = await _get_question_text_from_message(message)
    assert result is None


@pytest.mark.asyncio
async def test_get_question_text_no_text_no_media_returns_none():
    """Нет текста, голосового, фото — None."""
    message = MagicMock()
    message.text = None
    message.voice = None
    message.photo = None
    result = await _get_question_text_from_message(message)
    assert result is None


# --- _is_top_score_sufficient_for_answer ---


def test_is_top_score_sufficient_empty_chunks_returns_false():
    """Пустой список чанков — ответ не разрешён."""
    assert _is_top_score_sufficient_for_answer([], 0.45) is False


def test_is_top_score_sufficient_above_threshold_returns_true():
    """Лучший score выше порога — ответ разрешён."""
    chunks = [{"score": 0.5}, {"score": 0.3}]
    assert _is_top_score_sufficient_for_answer(chunks, 0.45) is True


def test_is_top_score_sufficient_below_threshold_returns_false():
    """Лучший score ниже порога — ответ не разрешён."""
    chunks = [{"score": 0.4}, {"score": 0.35}]
    assert _is_top_score_sufficient_for_answer(chunks, 0.45) is False


def test_is_top_score_sufficient_equal_to_threshold_returns_true():
    """Лучший score равен порогу — ответ разрешён."""
    chunks = [{"score": 0.45}]
    assert _is_top_score_sufficient_for_answer(chunks, 0.45) is True


def test_is_top_score_sufficient_missing_score_treated_as_zero():
    """Чанк без поля score считается 0."""
    chunks = [{"text": "foo"}, {"score": 0.5}]
    assert _is_top_score_sufficient_for_answer(chunks, 0.45) is True


# --- _is_question (with mocked OpenAI) ---


@pytest.mark.asyncio
async def test_is_question_yes_from_model_returns_true():
    """Модель ответила yes — считаем вопросом."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="  yes  "))]
    with patch("app.handlers.group_chat_qa.client") as mock_client:
        mock_client.chat.completions.create = MagicMock(return_value=mock_response)
        result = await _is_question("Какой документ подписать для начала работы?")
    assert result is True


@pytest.mark.asyncio
async def test_is_question_no_from_model_returns_false():
    """Модель ответила no — не вопрос."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="no"))]
    with patch("app.handlers.group_chat_qa.client") as mock_client:
        mock_client.chat.completions.create = MagicMock(return_value=mock_response)
        result = await _is_question("Привет")
    assert result is False


@pytest.mark.asyncio
async def test_is_question_short_question_without_mark_returns_true_when_model_yes():
    """Формулировка без «?», модель yes — вопрос."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="yes"))]
    with patch("app.handlers.group_chat_qa.client") as mock_client:
        mock_client.chat.completions.create = MagicMock(return_value=mock_response)
        result = await _is_question("Регламент по открытию магазина")
    assert result is True


@pytest.mark.asyncio
async def test_is_question_needs_info_phrase_returns_true_when_model_yes():
    """«Нужна информация когда…» при yes от модели — вопрос."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="yes"))]
    with patch("app.handlers.group_chat_qa.client") as mock_client:
        mock_client.chat.completions.create = MagicMock(return_value=mock_response)
        result = await _is_question("Нужна информация когда открывать ИП и счет")
    assert result is True


@pytest.mark.asyncio
async def test_is_question_on_exception_fallback_to_question_mark():
    """При ошибке API возвращаем True только если в тексте есть «?»."""
    with patch("app.handlers.group_chat_qa.client") as mock_client:
        mock_client.chat.completions.create = MagicMock(side_effect=Exception("API error"))
        assert await _is_question("Какой документ?") is True
        assert await _is_question("Привет") is False
