"""
PostgreSQL connection utilities using asyncpg.

This module provides utilities for connecting to PostgreSQL databases
using asyncpg with the PGConnectionParameters class.
"""

from typing import Any, List, Optional, Union

try:
    import asyncpg
except ImportError:
    raise ImportError(
        "asyncpg is required for database connections. "
        "Please install it with 'pip install asyncpg'."
    )

from .parameters import PGConnectionParameters


async def connect(
    params: Union[PGConnectionParameters, str], **kwargs: Any
) -> asyncpg.Connection:
    """
    Create a connection to a PostgreSQL database using asyncpg.

    Args:
        params: Either a PGConnectionParameters instance or a connection URL string
        **kwargs: Additional parameters to pass to asyncpg.connect()

    Returns:
        A connected asyncpg Connection object
    """
    if isinstance(params, PGConnectionParameters):
        connection_string = params.to_url()
    else:
        connection_string = params

    return await asyncpg.connect(connection_string, **kwargs)


async def execute_query(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    timeout: Optional[float] = None,
) -> str:
    """
    Execute a query that doesn't return any rows.

    Args:
        conn: The asyncpg connection
        query: The SQL query to execute
        *args: Parameters for the SQL query
        timeout: Optional timeout in seconds

    Returns:
        The command completion tag
    """
    return await conn.execute(query, *args, timeout=timeout)


async def fetch_all(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    timeout: Optional[float] = None,
) -> List[asyncpg.Record]:
    """
    Execute a query and return all resulting rows.

    Args:
        conn: The asyncpg connection
        query: The SQL query to execute
        *args: Parameters for the SQL query
        timeout: Optional timeout in seconds

    Returns:
        A list of Record objects
    """
    return await conn.fetch(query, *args, timeout=timeout)


async def fetch_row(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    timeout: Optional[float] = None,
) -> Optional[asyncpg.Record]:
    """
    Execute a query and return the first row.

    Args:
        conn: The asyncpg connection
        query: The SQL query to execute
        *args: Parameters for the SQL query
        timeout: Optional timeout in seconds

    Returns:
        The first row as a Record object or None if no rows returned
    """
    return await conn.fetchrow(query, *args, timeout=timeout)


async def fetch_val(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    column: int = 0,
    timeout: Optional[float] = None,
) -> Any:
    """
    Execute a query and return a single value from the first row.

    Args:
        conn: The asyncpg connection
        query: The SQL query to execute
        *args: Parameters for the SQL query
        column: The column index to retrieve
        timeout: Optional timeout in seconds

    Returns:
        A single value from the first row or None if no rows returned
    """
    return await conn.fetchval(query, *args, column=column, timeout=timeout)


class Transaction:
    """
    A wrapper around asyncpg.Transaction that provides a context manager.

    Usage:
        async with Transaction(connection) as tx:
            await tx.execute("INSERT INTO ...")
    """

    def __init__(self, connection: asyncpg.Connection, **kwargs: Any):
        self.connection = connection
        self.transaction_kwargs = kwargs
        self.transaction = None

    async def __aenter__(self) -> "Transaction":
        self.transaction = await self.connection.transaction(
            **self.transaction_kwargs
        )
        await self.transaction.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.transaction.rollback()
        else:
            await self.transaction.commit()
        self.transaction = None

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str:
        """Execute a query within this transaction."""
        return await execute_query(self.connection, query, *args, **kwargs)

    async def fetch(
        self, query: str, *args: Any, **kwargs: Any
    ) -> List[asyncpg.Record]:
        """Fetch all rows from a query within this transaction."""
        return await fetch_all(self.connection, query, *args, **kwargs)

    async def fetchrow(
        self, query: str, *args: Any, **kwargs: Any
    ) -> Optional[asyncpg.Record]:
        """Fetch a single row from a query within this transaction."""
        return await fetch_row(self.connection, query, *args, **kwargs)

    async def fetchval(
        self, query: str, *args: Any, column: int = 0, **kwargs: Any
    ) -> Any:
        """Fetch a single value from a query within this transaction."""
        return await fetch_val(
            self.connection, query, *args, column=column, **kwargs
        )
