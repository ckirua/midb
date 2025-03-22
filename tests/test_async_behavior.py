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
        """Test that pool can handle multiple concurrent connections."""
        # Create mock objects
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool

        # Create a list to track created connections
        mock_connections = []

        # Setup a mock connection class
        class MockConnection:
            def __init__(self, conn_id):
                self.conn_id = conn_id
                self.queries = []

            async def execute(self, query, *args, **kwargs):
                self.queries.append(query)
                return f"EXECUTED_{self.conn_id}"

        # Configure mock_acquire to create and return different connections
        counter = 0

        async def mock_acquire():
            nonlocal counter
            counter += 1
            conn = MockConnection(f"CONN_{counter}")
            mock_connections.append(conn)
            return conn

        mock_pool.acquire = mock_acquire

        # Configure mock_release to track releases
        released_connections = []

        async def mock_release(conn):
            released_connections.append(conn)

        mock_pool.release = mock_release

        # Create pool
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        pool = Pool(params)
        # Manually set the pool attribute instead of awaiting initialize
        pool.pool = mock_pool

        # Define a helper function to execute a query
        async def run_query(query):
            conn = await pool.acquire()
            try:
                result = await conn.execute(query)
                return result
            finally:
                await pool.release(conn)

        # Run multiple queries concurrently
        queries = [
            "SELECT * FROM table1",
            "INSERT INTO table2 VALUES (1, 2, 3)",
            "UPDATE table3 SET col = 'value'",
            "DELETE FROM table4 WHERE id = 42",
            "SELECT * FROM table5 WHERE name = 'test'",
        ]

        results = await asyncio.gather(*[run_query(q) for q in queries])

        # Verify the right number of connections were acquired
        self.assertEqual(len(mock_connections), len(queries))

        # Verify all connections were released
        self.assertEqual(len(released_connections), len(queries))

        # Verify each connection executed exactly one query
        for conn in mock_connections:
            self.assertEqual(len(conn.queries), 1)

        # Verify each query was executed
        for query in queries:
            self.assertTrue(
                any(query in conn.queries for conn in mock_connections)
            )

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_transaction_isolation(self, mock_connect):
        """Test transaction isolation with concurrent operations."""
        # Set up mock connection
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn

        # Track operations in each transaction
        trans1_operations = []
        trans2_operations = []

        # Mock execute function for transaction 1
        async def trans1_execute(query, *args, **kwargs):
            trans1_operations.append(query)
            return "EXECUTED_TRANS1"

        # Mock execute function for transaction 2
        async def trans2_execute(query, *args, **kwargs):
            trans2_operations.append(query)
            return "EXECUTED_TRANS2"

        # Set up mock transactions
        mock_trans1 = AsyncMock()
        mock_trans2 = AsyncMock()

        # Configure different mock connections for each transaction
        mock_conn_trans1 = AsyncMock()
        mock_conn_trans1.execute.side_effect = trans1_execute

        mock_conn_trans2 = AsyncMock()
        mock_conn_trans2.execute.side_effect = trans2_execute

        # Configure transaction context managers to return transaction objects
        tx1_conn_ctx = (
            mock_connect.return_value
        )  # First call returns first mock
        tx2_conn_ctx = AsyncMock()  # Second call returns second mock

        # Mock connect to return different connections for each call
        mock_connect.side_effect = [tx1_conn_ctx, tx2_conn_ctx]

        # Make the connections return their transaction objects
        tx1_conn_ctx.transaction.return_value = mock_trans1
        tx2_conn_ctx.transaction.return_value = mock_trans2

        # Connect for two separate transactions
        conn1 = await connect("postgresql://user:pass@host/db1")
        conn2 = await connect("postgresql://user:pass@host/db2")

        # Create transactions with the connections
        tx1 = Transaction(conn1)
        tx2 = Transaction(conn2)

        # Start the first transaction and execute queries
        async with tx1:
            # Store the transaction's connection execute method before replacing it
            tx1.connection.execute = mock_conn_trans1.execute

            # Run queries in transaction 1
            await tx1.execute("INSERT INTO table1 VALUES (1, 2, 3)")
            await tx1.execute("UPDATE table1 SET col = 'new_value'")

        # Start the second transaction and execute queries
        async with tx2:
            # Store the transaction's connection execute method before replacing it
            tx2.connection.execute = mock_conn_trans2.execute

            # Run queries in transaction 2
            await tx2.execute("SELECT * FROM table2")
            await tx2.execute("DELETE FROM table2 WHERE id = 5")

        # Verify both transactions executed their own queries
        self.assertEqual(len(trans1_operations), 2)
        self.assertEqual(len(trans2_operations), 2)

        # Verify transaction 1 operations
        self.assertIn("INSERT INTO table1 VALUES (1, 2, 3)", trans1_operations)
        self.assertIn("UPDATE table1 SET col = 'new_value'", trans1_operations)

        # Verify transaction 2 operations
        self.assertIn("SELECT * FROM table2", trans2_operations)
        self.assertIn("DELETE FROM table2 WHERE id = 5", trans2_operations)

        # Verify transactions were committed correctly
        mock_trans1.commit.assert_called_once()
        mock_trans2.commit.assert_called_once()

        # Verify transactions were not rolled back
        mock_trans1.rollback.assert_not_called()
        mock_trans2.rollback.assert_not_called()

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
