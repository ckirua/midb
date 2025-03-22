"""
PostgreSQL connection pool using asyncpg.

This module provides a connection pool implementation for PostgreSQL databases
using asyncpg with the PGConnectionParameters class.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union

try:
    import asyncpg
except ImportError:
    raise ImportError(
        "asyncpg is required for database connections. "
        "Please install it with 'pip install asyncpg'."
    )

from .connection import Transaction
from .dtypes import PGTypes
from .parameters import PGConnectionParameters

# Global registry for connection pools
_pools: Dict[str, "Pool"] = {}
_current_pool: Optional["Pool"] = None
_logger = logging.getLogger(__name__)


def get_pool(name: str = "default") -> Optional["Pool"]:
    """
    Get a connection pool by name from the global registry.

    Args:
        name: The name of the pool to retrieve

    Returns:
        The pool, or None if no pool with that name exists
    """
    return _pools.get(name)


def set_current_pool(pool: "Pool") -> None:
    """
    Set the current pool for the application.

    Args:
        pool: The pool to set as current
    """
    global _current_pool
    _current_pool = pool


def get_current_pool() -> Optional["Pool"]:
    """
    Get the current pool for the application.

    Returns:
        The current pool, or None if no pool is set
    """
    return _current_pool


@asynccontextmanager
async def connection(pool_name: Optional[str] = None):
    """
    Get a connection from a pool.

    If pool_name is provided, gets a connection from that pool.
    Otherwise, gets a connection from the current pool.

    Args:
        pool_name: Optional name of the pool to get a connection from

    Returns:
        A connection from the pool

    Raises:
        ValueError: If no pool is specified and no current pool is set
    """
    pool = get_pool(pool_name) if pool_name else get_current_pool()

    if not pool:
        raise ValueError(
            f"No pool found with name {pool_name}"
            if pool_name
            else "No current pool set. Use set_current_pool() first."
        )

    conn = await pool.acquire()
    try:
        yield conn
    finally:
        await pool.release(conn)


class Pool:
    """
    A connection pool for PostgreSQL databases.

    This class provides a wrapper around asyncpg.Pool with additional
    features like a global registry, connection acquisition with context
    manager support, and transaction support.
    """

    def __init__(
        self,
        dsn_or_params: Union[str, PGConnectionParameters],
        min_size: int = 10,
        max_size: int = 10,
        name: str = "default",
        **kwargs: Any,
    ):
        """
        Initialize a new connection pool.

        Args:
            dsn_or_params: Either a connection string or PGConnectionParameters
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
            name: The name of the pool for the global registry
            **kwargs: Additional parameters to pass to asyncpg.create_pool()
        """
        self.min_size = min_size
        self.max_size = max_size
        self.name = name
        self.pool_kwargs = kwargs

        if isinstance(dsn_or_params, PGConnectionParameters):
            self.dsn = dsn_or_params.to_url()
        else:
            self.dsn = dsn_or_params

        self.pool: Optional[asyncpg.Pool] = None
        self.types = PGTypes()

        # Register this pool in the global registry
        _pools[name] = self

    async def initialize(self) -> None:
        """
        Initialize the connection pool.

        This method must be called before using the pool.
        """
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                self.dsn,
                min_size=self.min_size,
                max_size=self.max_size,
                **self.pool_kwargs,
            )
            _logger.info(
                f"Initialized pool '{self.name}' with {self.min_size}-{self.max_size} connections"
            )

    async def close(self) -> None:
        """
        Close the connection pool.

        This method should be called when the pool is no longer needed.
        """
        if self.pool:
            await self.pool.close()
            self.pool = None
            _logger.info(f"Closed pool '{self.name}'")

            # Remove from the global registry
            if self.name in _pools:
                del _pools[self.name]

            # Clear current pool if it's this one
            global _current_pool
            if _current_pool is self:
                _current_pool = None

    async def acquire(self) -> asyncpg.Connection:
        """
        Acquire a connection from the pool.

        Returns:
            A connection from the pool

        Raises:
            RuntimeError: If the pool has not been initialized
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized. Call initialize() first.")
        return await self.pool.acquire()

    async def release(self, conn: asyncpg.Connection) -> None:
        """
        Release a connection back to the pool.

        Args:
            conn: The connection to release

        Raises:
            RuntimeError: If the pool has not been initialized
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized. Call initialize() first.")
        await self.pool.release(conn)

    async def execute(
        self, query: str, *args: Any, timeout: Optional[float] = None
    ) -> str:
        """
        Execute a query on a connection from the pool.

        Args:
            query: The SQL query to execute
            *args: Parameters for the SQL query
            timeout: Optional timeout in seconds

        Returns:
            The command completion tag

        Raises:
            RuntimeError: If the pool has not been initialized
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized. Call initialize() first.")
        return await self.pool.execute(query, *args, timeout=timeout)

    async def fetch(
        self, query: str, *args: Any, timeout: Optional[float] = None
    ) -> List[asyncpg.Record]:
        """
        Execute a query on a connection from the pool and return all rows.

        Args:
            query: The SQL query to execute
            *args: Parameters for the SQL query
            timeout: Optional timeout in seconds

        Returns:
            A list of Record objects

        Raises:
            RuntimeError: If the pool has not been initialized
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized. Call initialize() first.")
        return await self.pool.fetch(query, *args, timeout=timeout)

    async def fetchrow(
        self, query: str, *args: Any, timeout: Optional[float] = None
    ) -> Optional[asyncpg.Record]:
        """
        Execute a query on a connection from the pool and return the first row.

        Args:
            query: The SQL query to execute
            *args: Parameters for the SQL query
            timeout: Optional timeout in seconds

        Returns:
            The first row as a Record object or None if no rows returned

        Raises:
            RuntimeError: If the pool has not been initialized
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized. Call initialize() first.")
        return await self.pool.fetchrow(query, *args, timeout=timeout)

    async def fetchval(
        self,
        query: str,
        *args: Any,
        column: int = 0,
        timeout: Optional[float] = None,
    ) -> Any:
        """
        Execute a query on a connection from the pool and return a single value.

        Args:
            query: The SQL query to execute
            *args: Parameters for the SQL query
            column: The column index to retrieve
            timeout: Optional timeout in seconds

        Returns:
            A single value from the first row or None if no rows returned

        Raises:
            RuntimeError: If the pool has not been initialized
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized. Call initialize() first.")
        return await self.pool.fetchval(
            query, *args, column=column, timeout=timeout
        )

    @asynccontextmanager
    async def transaction(self, **kwargs: Any):
        """
        Start a transaction on a connection from the pool.

        Args:
            **kwargs: Transaction options to pass to the transaction

        Returns:
            A Transaction object that can be used as a context manager

        Raises:
            RuntimeError: If the pool has not been initialized
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized. Call initialize() first.")

        conn = await self.acquire()
        try:
            async with Transaction(conn, **kwargs) as tx:
                yield tx
        finally:
            await self.release(conn)

    async def __aenter__(self) -> "Pool":
        """
        Enter the context manager.

        If the pool has not been initialized, it will be initialized.
        The pool will be set as the current pool.

        Returns:
            The pool
        """
        if not self.pool:
            await self.initialize()

        # Set as current pool
        set_current_pool(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the context manager.

        If an error occurred, it will be logged.
        The pool will be closed.
        """
        if exc_type:
            _logger.error(f"Error in pool: {exc_val}")

        await self.close()
