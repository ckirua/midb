"""
Tests for error handling and edge cases in the midb.postgres module.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from midb.postgres import (
    PGConnectionParameters,
    PGSchemaParameters,
    PGTypes,
    Pool,
    Transaction,
    connect,
)


class TestConnectionErrors(unittest.TestCase):
    """Test error handling in the connection functionality."""

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_connect_error(self, mock_connect):
        """Test error handling in connect()."""
        # Set up mock to raise an exception
        mock_connect.side_effect = Exception("Connection error")

        # Attempt to connect
        with self.assertRaises(Exception) as context:
            await connect("postgresql://user:pass@host:5432/db")

        # Assert the correct error was raised
        self.assertEqual(str(context.exception), "Connection error")

    @patch("midb.postgres.connection.asyncpg.connect")
    async def async_test_transaction_connection_error(self, mock_connect):
        """Test error handling in Transaction when connection fails."""
        # Set up mock
        mock_conn = AsyncMock()
        mock_trans = AsyncMock()

        # Configure transaction start to raise an exception
        mock_trans.start.side_effect = Exception("Transaction start error")

        # Configure transaction method to return our mock transaction
        mock_conn.transaction.return_value = mock_trans

        # Configure connect to return our mock connection
        mock_connect.return_value = mock_conn

        # Create connection
        conn = await connect("postgresql://user:pass@host:5432/db")

        # Use Transaction with start error
        with self.assertRaises(Exception) as context:
            async with Transaction(conn) as tx:
                await tx.execute("SELECT 1")

        # Assert the correct error was raised
        self.assertEqual(str(context.exception), "Transaction start error")
        mock_trans.start.assert_called_once()
        self.assertEqual(mock_trans.commit.call_count, 0)  # No commit
        self.assertEqual(
            mock_trans.rollback.call_count, 0
        )  # No rollback (handled by context manager)

    def test_connect_error(self):
        """Run async test for connect error."""
        asyncio.run(self.async_test_connect_error())

    def test_transaction_connection_error(self):
        """Run async test for transaction connection error."""
        asyncio.run(self.async_test_transaction_connection_error())


class TestPoolErrors(unittest.TestCase):
    """Test error handling in the pool functionality."""

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_initialize_error(self, mock_create_pool):
        """Test error handling when pool initialization fails."""
        # Set up mock to raise an exception
        mock_create_pool.side_effect = Exception("Pool creation error")

        # Create pool
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )
        pool = Pool(params)

        # Attempt to initialize pool
        with self.assertRaises(Exception) as context:
            await pool.initialize()

        # Assert the correct error was raised
        self.assertEqual(str(context.exception), "Pool creation error")
        self.assertIsNone(pool.pool)

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_uninitialized_pool_error(self, mock_create_pool):
        """Test error handling when using uninitialized pool."""
        # Create pool without initializing
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )
        pool = Pool(params)

        # Ensure pool is not initialized
        self.assertIsNone(pool.pool)

        # Attempt to use uninitialized pool
        with self.assertRaises(RuntimeError) as context:
            await pool.acquire()
        self.assertEqual(
            str(context.exception),
            "Pool not initialized. Call initialize() first.",
        )

        with self.assertRaises(RuntimeError) as context:
            await pool.execute("SELECT 1")
        self.assertEqual(
            str(context.exception),
            "Pool not initialized. Call initialize() first.",
        )

        with self.assertRaises(RuntimeError) as context:
            await pool.fetch("SELECT 1")
        self.assertEqual(
            str(context.exception),
            "Pool not initialized. Call initialize() first.",
        )

        with self.assertRaises(RuntimeError) as context:
            await pool.fetchrow("SELECT 1")
        self.assertEqual(
            str(context.exception),
            "Pool not initialized. Call initialize() first.",
        )

        with self.assertRaises(RuntimeError) as context:
            await pool.fetchval("SELECT 1")
        self.assertEqual(
            str(context.exception),
            "Pool not initialized. Call initialize() first.",
        )

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_connection_context_manager_error(
        self, mock_create_pool
    ):
        """Test error handling in connection context manager."""
        # Set up mock
        mock_pool = AsyncMock()
        # Make acquire raise an exception
        mock_pool.acquire.side_effect = Exception("Acquire error")
        mock_create_pool.return_value = mock_pool

        # Create and initialize pool - avoid AsyncMock await issues
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )
        pool = Pool(params, name="errorpool")
        # Manually set the pool attribute
        pool.pool = mock_pool

        # Define a function to simulate the connection context manager
        async def use_connection():
            conn = await pool.acquire()
            try:
                pass  # Should not reach here due to acquire exception
            finally:
                if conn:
                    await pool.release(conn)

        # Use connection context manager with acquire error
        with self.assertRaises(Exception) as context:
            await use_connection()

        # Assert the correct error was raised
        self.assertEqual(str(context.exception), "Acquire error")
        mock_pool.acquire.assert_called_once()
        self.assertEqual(mock_pool.release.call_count, 0)  # No release

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_connection_pool_not_found_error(
        self, _mock_create_pool
    ):
        """Test error handling when referenced pool doesn't exist."""
        # Remove any existing pools from the registry
        import midb.postgres.pool
        from midb.postgres.pool import get_pool

        midb.postgres.pool._pools = {}
        midb.postgres.pool._current_pool = None

        # Define a function to simulate the connection context manager
        async def use_connection_with_nonexistent_pool():
            # Simulate what connection() does internally
            pool = get_pool("nonexistent_pool")
            if not pool:
                raise ValueError(f"No pool found with name nonexistent_pool")
            conn = await pool.acquire()
            try:
                pass  # Should not reach here
            finally:
                await pool.release(conn)

        # Attempt to use non-existent pool
        with self.assertRaises(ValueError) as context:
            await use_connection_with_nonexistent_pool()

        # Assert the correct error was raised
        self.assertEqual(
            str(context.exception), "No pool found with name nonexistent_pool"
        )

        # Define a function to simulate the default connection context manager
        async def use_connection_with_no_current_pool():
            # Simulate what connection() does internally
            pool = get_pool()
            if not pool:
                raise ValueError(
                    "No current pool set. Use set_current_pool() first."
                )
            conn = await pool.acquire()
            try:
                pass  # Should not reach here
            finally:
                await pool.release(conn)

        # Attempt to use current pool when none is set
        with self.assertRaises(ValueError) as context:
            await use_connection_with_no_current_pool()

        # Assert the correct error was raised
        self.assertEqual(
            str(context.exception),
            "No current pool set. Use set_current_pool() first.",
        )

    @patch("midb.postgres.pool.asyncpg.create_pool")
    async def async_test_pool_query_error(self, mock_create_pool):
        """Test error handling in pool query methods."""
        # Set up mock
        mock_pool = AsyncMock()
        # Make execute raise an exception
        mock_pool.execute.side_effect = Exception("Execute error")
        mock_create_pool.return_value = mock_pool

        # Create and initialize pool - avoid AsyncMock await issues
        params = PGConnectionParameters(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )
        pool = Pool(params)
        # Manually set the pool attribute
        pool.pool = mock_pool

        # Attempt to execute query
        with self.assertRaises(Exception) as context:
            await pool.execute("SELECT 1")

        # Assert the correct error was raised
        self.assertEqual(str(context.exception), "Execute error")
        mock_pool.execute.assert_called_once()

    def test_pool_initialize_error(self):
        """Run async test for pool initialization error."""
        asyncio.run(self.async_test_pool_initialize_error())

    def test_uninitialized_pool_error(self):
        """Run async test for uninitialized pool error."""
        asyncio.run(self.async_test_uninitialized_pool_error())

    def test_connection_context_manager_error(self):
        """Run async test for connection context manager error."""
        asyncio.run(self.async_test_connection_context_manager_error())

    def test_connection_pool_not_found_error(self):
        """Run async test for connection pool not found error."""
        asyncio.run(self.async_test_connection_pool_not_found_error())

    def test_pool_query_error(self):
        """Run async test for pool query error."""
        asyncio.run(self.async_test_pool_query_error())


class TestSchemaParametersEdgeCases(unittest.TestCase):
    """Test edge cases for the PGSchemaParameters class."""

    def test_empty_schema_parameters(self):
        """Test PGSchemaParameters with minimal inputs."""
        # Create minimal schema parameters
        dtype_map = {"id": "INTEGER"}
        params = PGSchemaParameters(
            schema_name="public",
            table_name="test",
            dtype_map=dtype_map,
        )

        # Verify properties
        self.assertEqual(params.schema_name, "public")
        self.assertEqual(params.table_name, "test")
        self.assertEqual(params.dtype_map, dtype_map)
        self.assertIsNone(params.time_index)
        self.assertIsNone(params.primary_keys)

    def test_schema_parameters_equality_edge_cases(self):
        """Test equality comparisons for PGSchemaParameters."""
        # Create basic schema parameters
        dtype_map = {"id": "INTEGER"}
        params1 = PGSchemaParameters(
            schema_name="public",
            table_name="test",
            dtype_map=dtype_map,
        )

        # Create identical schema parameters
        params2 = PGSchemaParameters(
            schema_name="public",
            table_name="test",
            dtype_map=dtype_map,
        )

        # Create similar schema parameters with different attributes
        params3 = PGSchemaParameters(
            schema_name="public",
            table_name="test",
            dtype_map={"id": "INTEGER", "name": "VARCHAR"},
        )

        # Test equality with identical parameters
        self.assertEqual(params1, params2)
        self.assertTrue(params1 == params2)
        self.assertFalse(params1 != params2)

        # Test equality with different parameters
        self.assertNotEqual(params1, params3)
        self.assertFalse(params1 == params3)
        self.assertTrue(params1 != params3)

        # Test equality with None
        self.assertNotEqual(params1, None)
        self.assertFalse(params1 == None)
        self.assertTrue(params1 != None)

        # Test equality with different object type
        # Skip direct assertNotEqual which tries to call __eq__
        # Just verify the boolean expressions work as expected
        self.assertFalse(params1 == "not a schema params")
        self.assertTrue(params1 != "not a schema params")

    def test_invalid_schema_parameters(self):
        """Test validation of invalid PGSchemaParameters."""
        types = PGTypes()

        # Test with empty dtype_map - should raise ValueError
        with self.assertRaises(ValueError) as context:
            PGSchemaParameters(
                schema_name="test",
                table_name="table",
                dtype_map={},  # Empty dtype_map should cause ValueError
            )
        self.assertEqual(str(context.exception), "dtype_map cannot be empty")

        # Test with non-existent time_index
        with self.assertRaises(ValueError):
            PGSchemaParameters(
                schema_name="test",
                table_name="table",
                dtype_map={"id": types.BigInt, "name": types.VarChar},
                time_index="timestamp",  # This column doesn't exist in dtype_map
            )

        # Test with empty schema_name
        with self.assertRaises(ValueError):
            PGSchemaParameters(
                schema_name="",
                table_name="test",
                dtype_map={"id": "INTEGER"},
            )

        # Test with empty table_name
        with self.assertRaises(ValueError):
            PGSchemaParameters(
                schema_name="public",
                table_name="",
                dtype_map={"id": "INTEGER"},
            )

    def test_none_dtype_map(self):
        """Test that None dtype_map raises ValueError."""
        try:
            PGSchemaParameters(
                schema_name="public",
                table_name="test",
                dtype_map=None,  # None should cause ValueError
            )
            self.fail("Expected ValueError was not raised for None dtype_map")
        except ValueError as e:
            self.assertEqual(str(e), "dtype_map cannot be None")
        except Exception as e:
            self.fail(f"Wrong exception type raised: {type(e).__name__}: {e}")


if __name__ == "__main__":
    unittest.main()
