"""
Type stubs for PostgreSQL connection pool.
"""

from contextlib import AbstractAsyncContextManager
from typing import Any, Dict, List, Optional, TypeVar, Union

import asyncpg
from asyncpg import Record

from .connection import Transaction
from .dtypes import PGTypes
from .parameters import PGConnectionParameters

T = TypeVar("T")

_pools: Dict[str, "Pool"]
_current_pool: Optional["Pool"]

def get_pool(name: str = "default") -> Optional["Pool"]: ...
def set_current_pool(pool: "Pool") -> None: ...
def get_current_pool() -> Optional["Pool"]: ...
def connection(
    pool_name: Optional[str] = None,
) -> AbstractAsyncContextManager[asyncpg.Connection]: ...

class Pool:
    """A connection pool for PostgreSQL databases."""

    min_size: int
    max_size: int
    name: str
    pool_kwargs: Dict[str, Any]
    dsn: str
    pool: Optional[asyncpg.pool.Pool]
    types: PGTypes

    def __init__(
        self,
        dsn_or_params: Union[str, PGConnectionParameters],
        min_size: int = 10,
        max_size: int = 10,
        name: str = "default",
        **kwargs: Any,
    ) -> None: ...
    async def initialize(self) -> None: ...
    async def close(self) -> None: ...
    async def acquire(self) -> asyncpg.Connection: ...
    async def release(self, conn: asyncpg.Connection) -> None: ...
    async def execute(
        self, query: str, *args: Any, timeout: Optional[float] = None
    ) -> str: ...
    async def fetch(
        self, query: str, *args: Any, timeout: Optional[float] = None
    ) -> List[Record]: ...
    async def fetchrow(
        self, query: str, *args: Any, timeout: Optional[float] = None
    ) -> Optional[Record]: ...
    async def fetchval(
        self,
        query: str,
        *args: Any,
        column: int = 0,
        timeout: Optional[float] = None,
    ) -> Any: ...
    def transaction(
        self, **kwargs: Any
    ) -> AbstractAsyncContextManager[Transaction]: ...
    async def __aenter__(self) -> "Pool": ...
    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None: ...
