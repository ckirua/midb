"""
Type stubs for PostgreSQL connection utilities.
"""

from typing import Any, List, Optional, TypeVar, Union

import asyncpg
from asyncpg import Record

from .parameters import PGConnectionParameters

T = TypeVar("T")

async def connect(
    params: Union[PGConnectionParameters, str], **kwargs: Any
) -> asyncpg.Connection: ...
async def execute_query(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    timeout: Optional[float] = None,
) -> str: ...
async def fetch_all(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    timeout: Optional[float] = None,
) -> List[Record]: ...
async def fetch_row(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    timeout: Optional[float] = None,
) -> Optional[Record]: ...
async def fetch_val(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    column: int = 0,
    timeout: Optional[float] = None,
) -> Any: ...

class Transaction:
    """A wrapper around asyncpg.Transaction that provides a context manager."""

    connection: asyncpg.Connection
    transaction_kwargs: dict
    transaction: Optional[asyncpg.transaction.Transaction]

    def __init__(
        self, connection: asyncpg.Connection, **kwargs: Any
    ) -> None: ...
    async def __aenter__(self) -> "Transaction": ...
    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None: ...
    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str: ...
    async def fetch(
        self, query: str, *args: Any, **kwargs: Any
    ) -> List[Record]: ...
    async def fetchrow(
        self, query: str, *args: Any, **kwargs: Any
    ) -> Optional[Record]: ...
    async def fetchval(
        self, query: str, *args: Any, column: int = 0, **kwargs: Any
    ) -> Any: ...
