"""Type stubs for the asyncpg pool wrapper."""

import asyncio
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Type

import asyncpg
from asyncpg.pool import Pool as AsyncpgPool
from asyncpg.transaction import Transaction

from .dtypes import PGTypes

_pools: Dict[str, "Pool"]
_current_pool: ContextVar[Optional["Pool"]]

class Pool:
    dsn: str
    min_size: int
    max_size: int
    max_queries: int
    max_inactive_connection_lifetime: float
    setup: Optional[Callable[[asyncpg.Connection], Any]]
    init: Optional[Callable[[asyncpg.Connection], Any]]
    name: str
    connect_kwargs: Dict[str, Any]
    types: PGTypes

    _pool: Optional[AsyncpgPool]
    _closed: bool
    _creating_pool: bool
    _creation_lock: asyncio.Lock
    _token: Token

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = ...,
        max_size: int = ...,
        max_queries: int = ...,
        max_inactive_connection_lifetime: float = ...,
        setup: Optional[Callable[[asyncpg.Connection], Any]] = ...,
        init: Optional[Callable[[asyncpg.Connection], Any]] = ...,
        name: Optional[str] = ...,
        **connect_kwargs: Any,
    ) -> None: ...
    @property
    def pool(self) -> AsyncpgPool: ...
    @classmethod
    def get(cls, name: str) -> "Pool": ...
    @classmethod
    def set_current(cls, pool: "Pool") -> None: ...
    @classmethod
    def current(cls) -> "Pool": ...
    async def _create_pool(self) -> AsyncpgPool: ...
    async def initialize(self) -> None: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> "Pool": ...
    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...
    async def acquire(self) -> asyncpg.Connection: ...
    async def release(self, conn: asyncpg.Connection) -> None: ...

    # Connection and transaction context managers
    async def connection(
        self,
    ) -> asyncio.ContextManager[asyncpg.Connection]: ...
    async def transaction(self) -> asyncio.ContextManager[Transaction]: ...

    # Convenience methods
    async def execute(
        self, query: str, *args: Any, timeout: Optional[float] = ...
    ) -> str: ...
    async def executemany(
        self,
        query: str,
        args: List[List[Any]],
        *,
        timeout: Optional[float] = ...,
    ) -> None: ...
    async def fetch(
        self, query: str, *args: Any, timeout: Optional[float] = ...
    ) -> List[asyncpg.Record]: ...
    async def fetchrow(
        self, query: str, *args: Any, timeout: Optional[float] = ...
    ) -> Optional[asyncpg.Record]: ...
    async def fetchval(
        self,
        query: str,
        *args: Any,
        column: int = ...,
        timeout: Optional[float] = ...,
    ) -> Any: ...

# Helper function
async def connection() -> asyncpg.Connection: ...
