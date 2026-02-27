"""MAX-платформа: клиент API и адаптер."""

from app.platforms.max.client import MaxApiClient, MaxApiClientError
from app.platforms.max.adapter import MaxAdapter, parse_max_update

__all__ = [
    "MaxApiClient",
    "MaxApiClientError",
    "MaxAdapter",
    "parse_max_update",
]
