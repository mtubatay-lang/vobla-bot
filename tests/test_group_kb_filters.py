"""Фильтры группового RAG (Telegram / MAX)."""

from app.handlers.group_chat_qa import passes_group_kb_content_filters, rag_chat_matches_restriction


def test_passes_group_kb_rejects_link_only() -> None:
    assert not passes_group_kb_content_filters("https://example.com/path")


def test_passes_group_kb_rejects_too_short() -> None:
    assert not passes_group_kb_content_filters("а б")


def test_passes_group_kb_accepts_question() -> None:
    assert passes_group_kb_content_filters("Какой документ нужен для открытия точки?")


def test_rag_chat_matches_none_means_all() -> None:
    assert rag_chat_matches_restriction(-100123, None)
    assert rag_chat_matches_restriction("-72257432704463", None)


def test_rag_chat_matches_numeric() -> None:
    assert rag_chat_matches_restriction(-1003377597100, -1003377597100)
    assert rag_chat_matches_restriction("-1003377597100", -1003377597100)
    assert not rag_chat_matches_restriction(-1, -1003377597100)
