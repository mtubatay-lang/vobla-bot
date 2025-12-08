"""Пакет обработчиков."""
from .start import router as start_router
from .echo import router as echo_router

__all__ = ["start_router", "echo_router"]

