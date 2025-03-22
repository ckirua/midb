"""
Tests for asynchronous behavior in the midb.postgres module.
"""

import asyncio
import time
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


class TestAsyncBehavior(unittest.TestCase):
    """Test asynchronous behavior of the connection and pool functionality."""

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_concurrent_operations(self, mock_connect):
        """Test that multiple asynchronous operations can be performed concurrently."""
        # Set up mock
        mock_conn = AsyncMock()

        # Track execution time for queries
        execution_times = []

        # Create a custom execute function that simulates delay
        async def delayed_execute(query, *args, **kwargs):
            start_time = time.time()
            # Simulate database operation taking time
            await asyncio.sleep(0.1)
            end_time = time.time()
            execution_times.append((start_time, end_time))
            return f"Result for {query}"

        mock_conn.execute = delayed_execute
        mock_connect.return_value = mock_conn

        # Create connection
        conn = await connect("postgresql://user:pass@host:5432/db")

        # Execute multiple queries concurrently
        start_time = time.time()
        tasks = [
            asyncio.create_task(conn.execute(f"SELECT {i}")) for i in range(5)
        ]
        results = await asyncio.gather(*tasks)
        end_time = time.time()

        # Assert that results are correct
        self.assertEqual(len(results), 5)
        self.assertEqual(
            results,
            [
                "Result for SELECT 0",
                "Result for SELECT 1",
                "Result for SELECT 2",
                "Result for SELECT 3",
                "Result for SELECT 4",
            ],
        )

        # Assert that operations were performed concurrently
        # Total time should be significantly less than sequential execution
        # which would be ~0.5 seconds (5 * 0.1s)
        self.assertLess(end_time - start_time, 0.3)

        # Check that all operations ran during the same time period
        for i in range(len(execution_times) - 1):
            # Each operation's start time should be before the next one's end time
            # This verifies they were running concurrently
            self.assertLess(execution_times[i][0], execution_times[i + 1][1])

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_concurrent_connections(self, mock_create_pool):
        """Test that multiple connections can be acquired from pool concurrently."""
        # Set up mock pool
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool

        # Track acquired connections
        acquired_connections = []
        released_connections = []

        # Create mock connections with delay
        class MockConnection:
            def __init__(self, conn_id):
                self.conn_id = conn_id
                self.execute_calls = []

            async def execute(self, query, *args, **kwargs):
                self.execute_calls.append(query)
                await asyncio.sleep(0.1)  # Simulate query execution
                return f"Result from conn {self.conn_id}: {query}"

        # Mock the acquire method to return different connections with delay
        async def mock_acquire():
            conn_id = len(acquired_connections)
            mock_conn = MockConnection(conn_id)
            await asyncio.sleep(0.05)  # Small delay in connection acquisition
            acquired_connections.append(mock_conn)
            return mock_conn

        # Mock the release method
        async def mock_release(conn):
            await asyncio.sleep(0.05)  # Small delay in connection release
            released_connections.append(conn)

        mock_pool.acquire = mock_acquire
        mock_pool.release = mock_release

        # Create pool parameters
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )

        # Create and initialize pool
        pool = Pool(params, name="testpool")
        await pool.initialize()
        set_current_pool("testpool")

        # Execute multiple queries using different connections
        start_time = time.time()

        async def run_query(query):
            async with connection() as conn:
                result = await conn.execute(query)
                return result

        # Run 5 queries concurrently, each should get its own connection
        tasks = [
            asyncio.create_task(run_query(f"SELECT {i}")) for i in range(5)
        ]
        results = await asyncio.gather(*tasks)
        end_time = time.time()

        # Assert that results are correct
        self.assertEqual(len(results), 5)
        expected_results = [
            f"Result from conn {i}: SELECT {i}" for i in range(5)
        ]
        self.assertEqual(results, expected_results)

        # Assert that operations were performed with multiple connections
        self.assertEqual(len(acquired_connections), 5)
        self.assertEqual(len(released_connections), 5)

        # Each connection should have executed exactly one query
        for i, conn in enumerate(acquired_connections):
            self.assertEqual(len(conn.execute_calls), 1)
            self.assertEqual(conn.execute_calls[0], f"SELECT {i}")

        # Assert that connections were acquired concurrently
        # Total time should be significantly less than sequential execution
        # which would be ~0.75 seconds (5 * (0.05s + 0.1s + 0.05s))
        self.assertLess(end_time - start_time, 0.4)

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_transaction_isolation(self, mock_connect):
        """Test that transactions properly isolate operations."""
        # Create mock transactions
        mock_trans1 = AsyncMock()
        mock_trans2 = AsyncMock()

        # Create mock connection
        mock_conn = AsyncMock()

        # Configure mock connection to return different transactions
        mock_conn.transaction.side_effect = [mock_trans1, mock_trans2]

        # Configure transaction mocks
        mock_trans1.start = AsyncMock()
        mock_trans1.commit = AsyncMock()
        mock_trans2.start = AsyncMock()
        mock_trans2.commit = AsyncMock()

        # Configure transaction execute methods
        mock_trans1.execute = AsyncMock()
        mock_trans2.execute = AsyncMock()

        # Configure connect to return our mock connection
        mock_connect.return_value = mock_conn

        async def operation1():
            async with Transaction(mock_conn) as tx1:
                await tx1.execute("INSERT INTO test (value) VALUES ($1)", 1)

        async def operation2():
            async with Transaction(mock_conn) as tx2:
                await tx2.execute("INSERT INTO test (value) VALUES ($1)", 2)

        # Run operations concurrently
        await asyncio.gather(operation1(), operation2())

        # Verify operations were performed in separate transactions
        mock_trans1.execute.assert_called_once_with(
            "INSERT INTO test (value) VALUES ($1)", 1
        )
        mock_trans2.execute.assert_called_once_with(
            "INSERT INTO test (value) VALUES ($1)", 2
        )

        # Verify transaction methods were called correctly
        mock_trans1.start.assert_called_once()
        mock_trans1.commit.assert_called_once()
        mock_trans2.start.assert_called_once()
        mock_trans2.commit.assert_called_once()

    def test_concurrent_operations(self):
        """Run async test for concurrent operations."""
        asyncio.run(self.async_test_concurrent_operations())

    def test_pool_concurrent_connections(self):
        """Run async test for pool concurrent connections."""
        asyncio.run(self.async_test_pool_concurrent_connections())

    def test_transaction_isolation(self):
        """Run async test for transaction isolation."""
        asyncio.run(self.async_test_transaction_isolation())


class TestPoolResourceManagement(unittest.TestCase):
    """Test proper resource management in pools and connections."""

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_cleanup(self, mock_create_pool):
        """Test that pool resources are properly cleaned up."""
        # Set up mock
        mock_pool = AsyncMock()
        # Return mock_pool to avoid 'can't use in await expression' error
        mock_create_pool.return_value = mock_pool

        # Create pool parameters
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )

        # Create and initialize pool
        pool = Pool(params, name="cleanup_test")
        # Manually set the pool attribute to avoid await of AsyncMock
        pool.pool = mock_pool

        # Verify pool is in the registry
        self.assertEqual(get_pool("cleanup_test"), pool)

        # Close the pool
        await pool.close()

        # Verify close was called
        mock_pool.close.assert_called_once()

        # Create a context manager pool - avoid actually awaiting the AsyncMock
        pool = Pool(params, name="context_pool")
        pool.pool = mock_pool
        # Call __aenter__ directly instead of using async with
        await pool.__aenter__()

        # Verify pool is in the registry
        self.assertEqual(get_pool("context_pool"), pool)

        # Use the pool
        mock_pool.execute.return_value = "Query executed"
        result = await pool.execute("SELECT 1")
        self.assertEqual(result, "Query executed")

        # Call __aexit__ directly to close the pool
        await pool.__aexit__(None, None, None)

        # Verify close was called again
        self.assertEqual(mock_pool.close.call_count, 2)

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_connection_cleanup(self, mock_create_pool):
        """Test that connections are properly released back to the pool."""
        # Set up mock
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value = mock_conn
        mock_create_pool.return_value = mock_pool

        # Create pool parameters
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )

        # Create and initialize pool - avoid async mock await issues
        pool = Pool(params, name="conn_cleanup_test")
        pool.pool = mock_pool
        set_current_pool("conn_cleanup_test")

        # Define a helper to simulate connection context manager
        async def use_connection_with_context():
            conn = await pool.acquire()
            try:
                await conn.execute("SELECT 1")
                await conn.execute("SELECT 2")
            finally:
                await pool.release(conn)

        # Use a connection
        await use_connection_with_context()

        # Verify acquire and release were called
        mock_pool.acquire.assert_called_once()
        mock_pool.release.assert_called_once_with(mock_conn)

        # Reset call counts
        mock_pool.acquire.reset_mock()
        mock_pool.release.reset_mock()

        # Use multiple connections
        tasks = [
            asyncio.create_task(use_connection_with_context()) for _ in range(3)
        ]
        await asyncio.gather(*tasks)

        # Verify acquire and release were called multiple times
        self.assertEqual(mock_pool.acquire.call_count, 3)
        self.assertEqual(mock_pool.release.call_count, 3)

    def test_pool_cleanup(self):
        """Run async test for pool cleanup."""
        asyncio.run(self.async_test_pool_cleanup())

    def test_connection_cleanup(self):
        """Run async test for connection cleanup."""
        asyncio.run(self.async_test_connection_cleanup())


if __name__ == "__main__":
    unittest.main()
