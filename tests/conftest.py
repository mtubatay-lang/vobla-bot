"""Pytest configuration and shared fixtures."""
import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark test as async (pytest-asyncio).")


@pytest.fixture
def anyio_backend():
    return "asyncio"
