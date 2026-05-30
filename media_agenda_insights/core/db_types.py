
from __future__ import annotations

from typing import Any, Mapping, Sequence

try:
    from psycopg2.extensions import connection as PGConnection
    from psycopg2.extensions import cursor as PGCursor
except ImportError:
    # Fallback mypy / Windows / CI
    class PGConnection:  # type: ignore
        ...
    class PGCursor:  # type: ignore
        ...

JsonDict = dict[str, Any]
JsonList = list[JsonDict]

__all__ = [
    "PGConnection",
    "PGCursor",
    "JsonDict",
    "JsonList",
    "Mapping",
    "Sequence",
]
