"""Database backends.

``base`` defines the generic :class:`DataBackend` protocol; concrete backends
(SQLite now, Redis later) implement it so the server tools and query pipeline
never depend on a specific storage engine.
"""

import os
from .base import (
    ColumnInfo,
    DataBackend,
    DataSink,
    QueryError,
    TableSchema,
    is_read_only_sql,
)

from .sqlite_backend import SQLiteSource
from .duckdb_backend import DuckDBSource
from dotenv import load_dotenv
from pathlib import Path
from typing import Literal

BackendType = Literal["sqlite", "duckdb"]

__all__ = [
    "ColumnInfo",
    "DataBackend",
    "DataSink",
    "QueryError",
    "TableSchema",
    "SQLiteSource",
    "DuckDBSource",
    "create_backend",
    "is_read_only_sql",
]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)


def create_backend(backend_type: BackendType = "sqlite") -> DataBackend:
    """Construct the configured backend (read-only, for the serving path).

    Today only SQLite is supported. A future Redis cache backend would be
    selected here (e.g. via a ``settings.backend`` field) without changing any
    server or client code, since both satisfy the :class:`DataBackend` protocol.
    """
    if backend_type == "sqlite":
        return SQLiteSource(os.getenv("SQLITEDB_PATH", None), read_only=True)
    elif backend_type == "duckdb":
        return DuckDBSource(os.getenv("DUCKDB_PATH", None), read_only=True)
    else:
        raise ValueError(f"Invalid backend type: {backend_type}")
