"""
Tests for SQL query building functionality in the midb.postgres module.
"""

import unittest

from midb.postgres import PGSchemaParameters
from midb.postgres.timescale import TSDBSql


class TestSchemaSQL(unittest.TestCase):
    """Test the SQL generation for schema and table creation."""

    def setUp(self):
        self.sql_builder = TSDBSql()

    def test_create_schema_sql(self):
        """Test creating SQL for schema creation."""
        # Create a basic schema
        sql = self.sql_builder.create_schema("test_schema")

        # Verify the SQL
        self.assertEqual(sql, "CREATE SCHEMA IF NOT EXISTS test_schema;")

    def test_create_table_sql_simple(self):
        """Test creating SQL for a simple table."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map={
                "id": "SERIAL PRIMARY KEY",
                "name": "VARCHAR(100)",
                "created_at": "TIMESTAMP WITH TIME ZONE",
            },
        )
        sql = self.sql_builder.create_table(
            schema=params.schema_name,
            table=params.table_name,
            columns=[
                "id SERIAL PRIMARY KEY",
                "name VARCHAR(100)",
                "created_at TIMESTAMP WITH TIME ZONE",
            ],
        )
        self.assertIn("CREATE TABLE", sql)
        self.assertIn("public.test_table", sql)

    def test_create_table_sql_complex(self):
        """Test creating SQL for a complex table with time index."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="metrics",
            dtype_map={
                "time": "TIMESTAMPTZ",
                "device_id": "VARCHAR(50)",
                "value": "DOUBLE PRECISION",
                "tags": "JSONB",
            },
            time_index="time",
            primary_keys=["time", "device_id"],
        )
        sql = self.sql_builder.create_table(
            schema=params.schema_name,
            table=params.table_name,
            columns=[
                "time TIMESTAMPTZ",
                "device_id VARCHAR(50)",
                "value DOUBLE PRECISION",
                "tags JSONB",
            ],
            constraints=["PRIMARY KEY (time, device_id)"],
        )
        self.assertIn("CREATE TABLE", sql)
        self.assertIn("public.metrics", sql)
        self.assertIn("PRIMARY KEY (time, device_id)", sql)

    def test_create_hypertable_sql(self):
        """Test creating SQL for hypertable creation."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="metrics",
            dtype_map={
                "time": "TIMESTAMPTZ",
                "device_id": "VARCHAR(50)",
                "value": "DOUBLE PRECISION",
            },
            time_index="time",
        )
        sql = self.sql_builder.create_hypertable(
            schema=params.schema_name,
            table=params.table_name,
            time_column=params.time_index,
        )
        self.assertIn("SELECT create_hypertable", sql)
        self.assertIn("public.metrics", sql)
        self.assertIn("time", sql)

    def test_create_hypertable_sql_with_chunks(self):
        """Test creating SQL for hypertable creation with chunk parameters."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="metrics",
            dtype_map={
                "time": "TIMESTAMPTZ",
                "device_id": "VARCHAR(50)",
                "value": "DOUBLE PRECISION",
            },
            time_index="time",
        )
        sql = self.sql_builder.create_hypertable(
            schema=params.schema_name,
            table=params.table_name,
            time_column=params.time_index,
            interval="1 day",
            if_not_exists=True,
        )
        self.assertIn("SELECT create_hypertable", sql)
        self.assertIn("public.metrics", sql)
        self.assertIn("time", sql)
        self.assertIn("chunk_time_interval => interval '1 day'", sql)


class TestSelectSQL(unittest.TestCase):
    """Test the SQL generation for SELECT queries."""

    def test_select_all_sql(self):
        """Test creating a basic SELECT * query."""
        sql_builder = TSDBSql()
        sql = sql_builder.select(table_name="test_table")

        # Verify the SQL
        self.assertEqual(sql, "SELECT * FROM test_table;")

    def test_select_columns_sql(self):
        """Test creating a SELECT query with specific columns."""
        sql_builder = TSDBSql()
        sql = sql_builder.select(
            table_name="users", columns=["id", "name", "email"]
        )

        # Verify the SQL
        self.assertEqual(sql, "SELECT id, name, email FROM users;")

    def test_select_with_schema_sql(self):
        """Test creating a SELECT query with a schema name."""
        sql_builder = TSDBSql()
        sql = sql_builder.select(
            schema_name="public",
            table_name="products",
            columns=["id", "name", "price"],
        )

        # Verify the SQL
        self.assertEqual(sql, "SELECT id, name, price FROM public.products;")

    def test_select_with_where_sql(self):
        """Test creating a SELECT query with WHERE clause."""
        sql_builder = TSDBSql()
        sql = sql_builder.select(
            table_name="orders",
            columns=["id", "customer_id", "total"],
            where="total > 100",
        )

        # Verify the SQL
        self.assertEqual(
            sql, "SELECT id, customer_id, total FROM orders WHERE total > 100;"
        )

    def test_select_with_order_by_sql(self):
        """Test creating a SELECT query with ORDER BY clause."""
        sql_builder = TSDBSql()
        sql = sql_builder.select(
            table_name="products",
            columns=["id", "name", "price"],
            order_by="price DESC",
        )

        # Verify the SQL
        self.assertEqual(
            sql, "SELECT id, name, price FROM products ORDER BY price DESC;"
        )

    def test_select_with_limit_sql(self):
        """Test creating a SELECT query with LIMIT clause."""
        sql_builder = TSDBSql()
        sql = sql_builder.select(table_name="logs", limit=10)

        # Verify the SQL
        self.assertEqual(sql, "SELECT * FROM logs LIMIT 10;")

    def test_select_complex_sql(self):
        """Test creating a complex SELECT query with multiple clauses."""
        sql_builder = TSDBSql()
        sql = sql_builder.select(
            schema_name="shop",
            table_name="products",
            columns=["id", "name", "price", "category"],
            where="price > 50 AND category = 'electronics'",
            order_by="price ASC",
            limit=5,
        )

        # Verify the SQL
        expected_sql = (
            "SELECT id, name, price, category FROM shop.products "
            "WHERE price > 50 AND category = 'electronics' "
            "ORDER BY price ASC LIMIT 5;"
        )
        self.assertEqual(sql, expected_sql)


class TestInsertSQL(unittest.TestCase):
    """Test the SQL generation for INSERT queries."""

    def setUp(self):
        self.sql_builder = TSDBSql()

    def test_insert_single_row_sql(self):
        """Test creating an INSERT query for a single row."""
        values = {
            "device_id": "device1",
            "value": 42.5,
            "tags": {"location": "room1", "type": "temperature"},
        }
        sql, params = self.sql_builder.insert_many(
            table_name="metrics",
            values=[values],
            schema_name="public",
        )
        self.assertIn("INSERT INTO", sql)
        self.assertIn("public.metrics", sql)
        self.assertEqual(len(params), 3)

    def test_insert_with_schema_sql(self):
        """Test creating an INSERT query with a schema name."""
        sql_builder = TSDBSql()
        values = {"id": 1, "name": "John Doe", "email": "john@example.com"}
        sql, params = sql_builder.insert(
            schema_name="public", table_name="users", values=values
        )

        # Verify the SQL and parameters
        expected_sql = (
            "INSERT INTO public.users (id, name, email) VALUES ($1, $2, $3);"
        )
        expected_params = [1, "John Doe", "john@example.com"]
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)

    def test_insert_with_returning_sql(self):
        """Test creating an INSERT query with RETURNING clause."""
        sql_builder = TSDBSql()
        values = {"name": "Product 2", "price": 29.99}
        sql, params = sql_builder.insert(
            table_name="products", values=values, returning="id"
        )

        # Verify the SQL and parameters
        expected_sql = (
            "INSERT INTO products (name, price) VALUES ($1, $2) RETURNING id;"
        )
        expected_params = ["Product 2", 29.99]
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)

    def test_insert_multiple_rows_sql(self):
        """Test creating an INSERT query for multiple rows."""
        values_list = [
            {
                "device_id": "device1",
                "value": 42.5,
                "tags": {"location": "room1", "type": "temperature"},
            },
            {
                "device_id": "device2",
                "value": 43.0,
                "tags": {"location": "room2", "type": "humidity"},
            },
        ]
        sql, params = self.sql_builder.insert_many(
            table_name="metrics",
            values=values_list,
            schema_name="public",
        )
        self.assertIn("INSERT INTO", sql)
        self.assertIn("public.metrics", sql)
        self.assertEqual(len(params), 6)

    def test_insert_many_with_returning_sql(self):
        """Test creating an INSERT MANY query with RETURNING clause."""
        values = {
            "device_id": "device1",
            "value": 42.5,
            "tags": {"location": "room1", "type": "temperature"},
        }
        sql, params = self.sql_builder.insert_many(
            table_name="metrics",
            values=[values],
            schema_name="public",
            returning="id, created_at",
        )
        self.assertIn("INSERT INTO", sql)
        self.assertIn("public.metrics", sql)
        self.assertIn("RETURNING", sql)
        self.assertEqual(len(params), 3)


class TestUpdateSQL(unittest.TestCase):
    """Test the SQL generation for UPDATE queries."""

    def test_update_sql(self):
        """Test creating a basic UPDATE query."""
        sql_builder = TSDBSql()
        values = {"name": "Updated Product", "price": 24.99}
        sql, params = sql_builder.update(
            table_name="products", values=values, where="id = 1"
        )

        # Verify the SQL and parameters
        expected_sql = "UPDATE products SET name = $1, price = $2 WHERE id = 1;"
        expected_params = ["Updated Product", 24.99]
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)

    def test_update_with_schema_sql(self):
        """Test creating an UPDATE query with a schema name."""
        sql_builder = TSDBSql()
        values = {"email": "newemail@example.com", "is_active": True}
        sql, params = sql_builder.update(
            schema_name="public",
            table_name="users",
            values=values,
            where="id = 5",
        )

        # Verify the SQL and parameters
        expected_sql = (
            "UPDATE public.users SET email = $1, is_active = $2 WHERE id = 5;"
        )
        expected_params = ["newemail@example.com", True]
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)

    def test_update_with_returning_sql(self):
        """Test creating an UPDATE query with RETURNING clause."""
        sql_builder = TSDBSql()
        values = {"price": 34.99, "stock": 50}
        sql, params = sql_builder.update(
            table_name="products",
            values=values,
            where="id = 3",
            returning="id, price, stock",
        )

        # Verify the SQL and parameters
        expected_sql = (
            "UPDATE products SET price = $1, stock = $2 "
            "WHERE id = 3 RETURNING id, price, stock;"
        )
        expected_params = [34.99, 50]
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)

    def test_update_without_where_sql(self):
        """Test creating an UPDATE query without WHERE clause (should warn)."""
        sql_builder = TSDBSql()
        values = {"status": "archived"}

        # This should raise a warning about updating all rows
        with self.assertWarns(UserWarning):
            sql, params = sql_builder.update(table_name="orders", values=values)

        # Verify the SQL and parameters
        expected_sql = "UPDATE orders SET status = $1;"
        expected_params = ["archived"]
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)


class TestDeleteSQL(unittest.TestCase):
    """Test the SQL generation for DELETE queries."""

    def test_delete_sql(self):
        """Test creating a basic DELETE query."""
        sql_builder = TSDBSql()
        sql = sql_builder.delete(table_name="products", where="id = 1")

        # Verify the SQL
        expected_sql = "DELETE FROM products WHERE id = 1;"
        self.assertEqual(sql, expected_sql)

    def test_delete_with_schema_sql(self):
        """Test creating a DELETE query with a schema name."""
        sql_builder = TSDBSql()
        sql = sql_builder.delete(
            schema_name="public",
            table_name="users",
            where="email = 'old@example.com'",
        )

        # Verify the SQL
        expected_sql = (
            "DELETE FROM public.users WHERE email = 'old@example.com';"
        )
        self.assertEqual(sql, expected_sql)

    def test_delete_with_returning_sql(self):
        """Test creating a DELETE query with RETURNING clause."""
        sql_builder = TSDBSql()
        sql = sql_builder.delete(
            table_name="orders",
            where="status = 'cancelled'",
            returning="id, customer_id",
        )

        # Verify the SQL
        expected_sql = "DELETE FROM orders WHERE status = 'cancelled' RETURNING id, customer_id;"
        self.assertEqual(sql, expected_sql)

    def test_delete_without_where_sql(self):
        """Test creating a DELETE query without WHERE clause (should warn)."""
        sql_builder = TSDBSql()

        # This should raise a warning about deleting all rows
        with self.assertWarns(UserWarning):
            sql = sql_builder.delete(table_name="temp_logs")

        # Verify the SQL
        expected_sql = "DELETE FROM temp_logs;"
        self.assertEqual(sql, expected_sql)


if __name__ == "__main__":
    unittest.main()
