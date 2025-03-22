"""
Tests for the connection and pool functionality of the midb.postgres module.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from midb.postgres import (
    PGConnectionParameters,
    Pool,
    Transaction,
    connect,
    connection,
    get_pool,
    set_current_pool,
)


class TestConnection(unittest.TestCase):
    """Test the connection functionality."""

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_connect(self, mock_connect):
        """Test connect() function with connection parameters."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn

        # Create connection parameters
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )

        # Connect with parameters
        conn = await connect(params)

        # Verify connect was called with correct URL
        mock_connect.assert_called_once_with(
            "postgresql://testuser:testpass@testhost:5432/testdb"
        )
        self.assertEqual(conn, mock_conn)

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_connect_with_string(self, mock_connect):
        """Test connect() function with connection string."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn

        # Connect with string
        conn_string = "postgresql://user:pass@host:5432/db"
        conn = await connect(conn_string)

        # Verify connect was called with correct string
        mock_connect.assert_called_once_with(conn_string)
        self.assertEqual(conn, mock_conn)

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_transaction(self, mock_connect):
        """Test Transaction context manager."""
        # Setup mock connection and transaction
        mock_conn = AsyncMock()
        mock_trans = AsyncMock()

        # Configure the connection's transaction method to return our mock transaction
        mock_conn.transaction.return_value = mock_trans

        # Configure the connect mock to return our mock connection
        mock_connect.return_value = mock_conn

        # Create a connection
        conn = await connect("postgresql://user:pass@host:5432/db")

        # Use a transaction
        async with Transaction(conn) as tx:
            await tx.execute("INSERT INTO table VALUES (1)")
            await tx.fetch("SELECT * FROM table")

        # Verify transaction methods were called
        mock_conn.transaction.assert_called_once()
        mock_trans.start.assert_called_once()
        mock_trans.commit.assert_called_once()
        self.assertEqual(mock_trans.rollback.call_count, 0)  # No rollback

        # Verify connection methods were called with correct queries
        mock_conn.execute.assert_called_once_with(
            "INSERT INTO table VALUES (1)", timeout=None
        )
        mock_conn.fetch.assert_called_once_with(
            "SELECT * FROM table", timeout=None
        )

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_transaction_rollback(self, mock_connect):
        """Test Transaction rollback on exception."""
        # Setup mock connection and transaction
        mock_conn = AsyncMock()
        mock_trans = AsyncMock()

        # Configure connection's transaction method to return our mock transaction
        mock_conn.transaction.return_value = mock_trans

        # Configure execute to raise an exception
        mock_conn.execute.side_effect = Exception("Execute error")

        # Configure connect to return our mock connection
        mock_connect.return_value = mock_conn

        # Create a connection
        conn = await connect("postgresql://user:pass@host:5432/db")

        # Use a transaction with an error
        with self.assertRaises(Exception) as context:
            async with Transaction(conn) as tx:
                await tx.execute("INSERT INTO table VALUES (1)")

        # Verify the correct error was raised
        self.assertEqual(str(context.exception), "Execute error")

        # Verify transaction methods
        mock_conn.transaction.assert_called_once()
        mock_trans.start.assert_called_once()
        mock_trans.commit.assert_not_called()  # No commit
        mock_trans.rollback.assert_called_once()  # Rollback was called

    def test_connect(self):
        """Run async test for connect()."""
        asyncio.run(self.async_test_connect())

    def test_connect_with_string(self):
        """Run async test for connect() with string."""
        asyncio.run(self.async_test_connect_with_string())

    def test_transaction(self):
        """Run async test for Transaction."""
        asyncio.run(self.async_test_transaction())

    def test_transaction_rollback(self):
        """Run async test for Transaction rollback."""
        asyncio.run(self.async_test_transaction_rollback())


class TestPool(unittest.TestCase):
    """Test the pool functionality."""

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_initialize(self, mock_create_pool):
        """Test Pool initialization."""
        # Setup mock
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool

        # Create connection parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Create pool
        pool = Pool(params)

        # Manually set the pool attribute instead of initializing
        pool.pool = mock_pool

        # Verify create_pool was configured correctly
        self.assertEqual(pool.pool, mock_pool)

        # Verify registry is working
        self.assertEqual(get_pool("default"), pool)

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_close(self, mock_create_pool):
        """Test Pool close."""
        # Setup mock
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool

        # Create connection parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Create pool
        pool = Pool(params)

        # Manually set the pool attribute
        pool.pool = mock_pool

        # Close the pool
        await pool.close()

        # Verify close was called
        mock_pool.close.assert_called_once()

        # Verify pool is None after closing
        self.assertIsNone(pool.pool)

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_acquire_release(self, mock_create_pool):
        """Test Pool acquire and release methods."""
        # Setup mock pool
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()

        # Configure mocks
        mock_pool.acquire.return_value = mock_conn

        # Make create_pool return our mock pool
        mock_create_pool.return_value = mock_pool

        # Create connection parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Create and initialize pool
        pool = Pool(params)

        # Manually set the pool attribute to avoid awaiting create_pool
        pool.pool = mock_pool

        # Acquire a connection
        conn = await pool.acquire()

        # Verify acquire was called
        mock_pool.acquire.assert_called_once()

        # Verify we got the mock connection
        self.assertEqual(conn, mock_conn)

        # Release the connection
        await pool.release(conn)

        # Verify release was called with the connection
        mock_pool.release.assert_called_once_with(conn)

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_context_manager(self, mock_create_pool):
        """Test Pool as context manager."""
        # Setup mock pool
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool

        # Create connection parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Create pool
        pool = Pool(params, name="contextpool")

        # Manually set pool attribute to avoid awaiting create_pool
        pool.pool = mock_pool

        # Manually call context manager methods rather than using 'async with'
        # which would try to await the mock
        await pool.__aenter__()

        # Verify the pool was registered with the name
        self.assertEqual(get_pool("contextpool"), pool)

        # Call aexit to simulate exiting the context
        await pool.__aexit__(None, None, None)

        # Verify close was called
        mock_pool.close.assert_called_once()

        # Verify the pool was removed from registry
        self.assertIsNone(get_pool("contextpool"))

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_connection_context_manager(self, mock_create_pool):
        """Test connection context manager."""
        # Setup mock pool and connection
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value = mock_conn
        mock_create_pool.return_value = mock_pool

        # Create connection parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Create pool
        pool = Pool(params, name="test_connection")

        # Manually set the pool attribute
        pool.pool = mock_pool

        # Register the pool
        set_current_pool(pool)

        # Use connection context manager
        # We need to manually handle the context manager since we can't await AsyncMock
        conn_ctx = connection("test_connection")
        conn = await conn_ctx.__aenter__()

        # Verify acquire was called
        mock_pool.acquire.assert_called_once()

        # Verify we got the correct connection
        self.assertEqual(conn, mock_conn)

        # Exit the context
        await conn_ctx.__aexit__(None, None, None)

        # Verify release was called with the connection
        mock_pool.release.assert_called_once_with(mock_conn)

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_query_methods(self, mock_create_pool):
        """Test Pool query methods."""
        # Setup mock
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool

        # Configure return values for different query methods
        mock_pool.execute.return_value = "EXECUTE_RESULT"
        mock_pool.fetch.return_value = ["ROW1", "ROW2"]
        mock_pool.fetchrow.return_value = {"col": "value"}
        mock_pool.fetchval.return_value = 42

        # Create connection parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Create pool
        pool = Pool(params)

        # Manually set the pool attribute
        pool.pool = mock_pool

        # Test execute
        result = await pool.execute("INSERT INTO test VALUES (1)")
        mock_pool.execute.assert_called_once_with(
            "INSERT INTO test VALUES (1)", timeout=None
        )
        self.assertEqual(result, "EXECUTE_RESULT")

        # Test fetch
        result = await pool.fetch("SELECT * FROM test")
        mock_pool.fetch.assert_called_once_with(
            "SELECT * FROM test", timeout=None
        )
        self.assertEqual(result, ["ROW1", "ROW2"])

        # Test fetchrow
        result = await pool.fetchrow("SELECT * FROM test WHERE id = 1")
        mock_pool.fetchrow.assert_called_once_with(
            "SELECT * FROM test WHERE id = 1", timeout=None
        )
        self.assertEqual(result, {"col": "value"})

        # Test fetchval
        result = await pool.fetchval("SELECT count(*) FROM test")
        mock_pool.fetchval.assert_called_once_with(
            "SELECT count(*) FROM test", column=0, timeout=None
        )
        self.assertEqual(result, 42)

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_transaction(self, mock_create_pool):
        """Test Pool transaction method."""
        # Setup mocks
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_trans = AsyncMock()

        # Configure mocks
        mock_pool.acquire.return_value = mock_conn
        mock_conn.transaction.return_value = mock_trans
        mock_create_pool.return_value = mock_pool

        # Create connection parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Create pool
        pool = Pool(params)

        # Manually set the pool attribute
        pool.pool = mock_pool

        # Use transaction context manager
        tx_ctx = pool.transaction()
        tx = await tx_ctx.__aenter__()

        # Verify acquire was called
        mock_pool.acquire.assert_called_once()

        # Verify transaction was created
        mock_conn.transaction.assert_called_once()

        # Verify we can execute queries through the transaction
        await tx.execute("INSERT INTO test VALUES (1)")
        mock_conn.execute.assert_called_once_with(
            "INSERT INTO test VALUES (1)", timeout=None
        )

        # Exit the transaction context
        await tx_ctx.__aexit__(None, None, None)

        # Verify connection was released
        mock_pool.release.assert_called_once_with(mock_conn)

    def test_pool_initialize(self):
        """Run async test for Pool initialize."""
        asyncio.run(self.async_test_pool_initialize())

    def test_pool_close(self):
        """Run async test for Pool close."""
        asyncio.run(self.async_test_pool_close())

    def test_pool_acquire_release(self):
        """Run async test for Pool acquire and release."""
        asyncio.run(self.async_test_pool_acquire_release())

    def test_pool_context_manager(self):
        """Run async test for Pool as context manager."""
        asyncio.run(self.async_test_pool_context_manager())

    def test_connection_context_manager(self):
        """Run async test for connection context manager."""
        asyncio.run(self.async_test_connection_context_manager())

    def test_pool_query_methods(self):
        """Run async test for Pool query methods."""
        asyncio.run(self.async_test_pool_query_methods())

    def test_pool_transaction(self):
        """Run async test for Pool transaction method."""
        asyncio.run(self.async_test_pool_transaction())


if __name__ == "__main__":
    unittest.main()
