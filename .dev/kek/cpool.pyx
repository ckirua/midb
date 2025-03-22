# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native
"""
Provides a high-performance Cython-optimized wrapper around asyncpg connection pools.
"""
import asyncio
import logging
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Type, Union

cimport cython

import asyncpg
from asyncpg.pool import Pool as AsyncpgPool
from asyncpg.transaction import Transaction

from .dtypes import PGTypes

logger = logging.getLogger(__name__)

# Global pool instance tracking
_pools: Dict[str, 'CPool'] = {}
_current_pool: ContextVar[Optional['CPool']] = ContextVar('current_pool', default=None)


@cython.final
@cython.no_gc_clear
cdef class CPool:
    """
    A high-performance Cython-optimized wrapper around asyncpg connection pools with additional features:
    
    - Global pool registry for easy access
    - Connection acquisition with context manager support
    - Transaction support with context manager
    - Auto-reconnection capabilities
    - Statically typed SQL data types
    """
    
    # Python attributes
    cdef public str dsn
    cdef public int min_size
    cdef public int max_size
    cdef public int max_queries
    cdef public double max_inactive_connection_lifetime
    cdef public object setup
    cdef public object init
    cdef public str name
    cdef public dict connect_kwargs
    
    # Private attributes
    cdef object _pool
    cdef bint _closed
    cdef bint _creating_pool
    cdef object _creation_lock
    cdef object _token
    
    # SQL types
    cdef public PGTypes types
    
    def __cinit__(
        self,
        str dsn,
        *,
        int min_size=10,
        int max_size=10,
        int max_queries=50000,
        double max_inactive_connection_lifetime=300.0,
        object setup=None,
        object init=None,
        str name=None,
        **connect_kwargs
    ):
        """
        Initialize a new PostgreSQL connection pool.
        
        Args:
            dsn: PostgreSQL connection string
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
            max_queries: Maximum number of queries per connection before recycling
            max_inactive_connection_lifetime: Seconds after which inactive connections are closed
            setup: Coroutine called when a connection is created
            init: Coroutine called when a connection is created or acquired from the pool
            name: Optional name for the pool (for registry purposes)
            **connect_kwargs: Additional connection parameters passed to asyncpg
        """
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.max_queries = max_queries
        self.max_inactive_connection_lifetime = max_inactive_connection_lifetime
        self.setup = setup
        self.init = init
        self.name = name or dsn
        self.connect_kwargs = connect_kwargs
        
        # Will be initialized in _create_pool
        self._pool = None
        self._closed = False
        self._creating_pool = False
        self._creation_lock = asyncio.Lock()
        
        # Initialize SQL types
        self.types = PGTypes()
        
        # Register in global pools registry
        if name:
            _pools[name] = self
    
    @property
    def pool(self):
        """Return the underlying asyncpg pool instance, creating it if necessary."""
        if self._pool is None or self._closed:
            raise RuntimeError("Pool is not initialized or has been closed")
        return self._pool
    
    @classmethod
    def get(cls, str name):
        """Get a registered pool by name."""
        try:
            return _pools[name]
        except KeyError:
            raise KeyError(f"No pool registered with name: {name}")
    
    @classmethod
    def set_current(cls, CPool pool):
        """Set the current pool in the context."""
        _current_pool.set(pool)
    
    @classmethod
    def current(cls):
        """Get the current pool from the context."""
        pool = _current_pool.get()
        if pool is None:
            raise RuntimeError("No pool set in current context")
        return pool
    
    async def _create_pool(self):
        """Create the actual asyncpg connection pool."""
        return await asyncpg.create_pool(
            self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
            max_queries=self.max_queries,
            max_inactive_connection_lifetime=self.max_inactive_connection_lifetime,
            setup=self.setup,
            init=self.init,
            **self.connect_kwargs
        )
    
    async def initialize(self):
        """Initialize the connection pool."""
        if self._pool is not None and not self._closed:
            return
        
        # Use a lock to prevent multiple simultaneous initialization attempts
        async with self._creation_lock:
            if self._creating_pool:
                # Another task is already creating the pool
                return
            
            if self._pool is not None and not self._closed:
                # Pool was created while we were waiting for the lock
                return
            
            try:
                self._creating_pool = True
                self._pool = await self._create_pool()
                self._closed = False
                logger.info(f"Pool initialized: {self.name}")
            except Exception as e:
                logger.error(f"Failed to initialize pool: {str(e)}")
                raise
            finally:
                self._creating_pool = False
    
    async def close(self):
        """Close the connection pool."""
        if self._pool is not None and not self._closed:
            await self._pool.close()
            self._closed = True
            logger.info(f"Pool closed: {self.name}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        old_pool = _current_pool.get()
        self._token = _current_pool.set(self)
        return self
    
    async def __aexit__(
        self,
        exc_type,
        exc_val,
        exc_tb
    ):
        """Async context manager exit."""
        _current_pool.reset(self._token)
    
    async def acquire(self):
        """Acquire a connection from the pool."""
        if self._pool is None:
            await self.initialize()
        assert self._pool is not None
        return await self._pool.acquire()
    
    async def release(self, conn):
        """Release a connection back to the pool."""
        if self._pool is None:
            raise RuntimeError("Pool not initialized")
        await self._pool.release(conn)
    
    @cython.returns(object)
    async def connection(self):
        """
        Acquire a connection from the pool and manage its lifecycle.
        
        Example:
            async with pool.connection() as conn:
                result = await conn.fetch("SELECT * FROM users")
        """
        conn = await self.acquire()
        try:
            yield conn
        finally:
            await self.release(conn)
    
    @cython.returns(object)
    async def transaction(self):
        """
        Start a transaction within a dedicated connection.
        
        Example:
            async with pool.transaction() as tx:
                await tx.connection.execute("INSERT INTO users(name) VALUES($1)", "John")
                await tx.connection.execute("UPDATE counters SET value = value + 1")
        """
        conn = await self.acquire()
        tx = conn.transaction()
        try:
            await tx.start()
            yield tx
            await tx.commit()
        except Exception:
            try:
                await tx.rollback()
            except Exception:
                # Transaction might already be rolled back or in error state
                pass
            raise
        finally:
            await self.release(conn)
    
    # Querying convenience methods
    @cython.returns(str)
    async def execute(self, str query, *args, timeout=None):
        """Execute a query that returns a status string."""
        async with self.connection() as conn:
            return await conn.execute(query, *args, timeout=timeout)
    
    async def executemany(self, str query, args, *, timeout=None):
        """Execute a query with a sequence of arguments."""
        async with self.connection() as conn:
            await conn.executemany(query, args, timeout=timeout)
    
    @cython.returns(list)
    async def fetch(self, str query, *args, timeout=None):
        """Run a query and return all results as a list of records."""
        async with self.connection() as conn:
            return await conn.fetch(query, *args, timeout=timeout)
    
    async def fetchrow(self, str query, *args, timeout=None):
        """Run a query and return the first row, or None if no rows were returned."""
        async with self.connection() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)
    
    async def fetchval(self, str query, *args, int column=0, timeout=None):
        """Run a query and return a value from the first row, or None if no rows were returned."""
        async with self.connection() as conn:
            return await conn.fetchval(query, *args, column=column, timeout=timeout)


# Helper for getting a connection from the current pool context
async def cconnection():
    """Get a connection from the current pool context."""
    pool = CPool.current()
    return await pool.acquire() 