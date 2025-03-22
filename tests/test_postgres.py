"""
Tests for the midb.postgres module.
"""

import unittest

from midb.postgres import (
    PGConnectionParameters,
    PGSchemaParameters,
    PGTypes,
    TSDBSql,
)


class TestPGConnectionParameters(unittest.TestCase):
    """Test the PGConnectionParameters class."""

    def test_init(self):
        """Test initialization and property access."""
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Get the parameters as a dictionary
        params_dict = params.to_dict()

        # Check the values in the dictionary
        self.assertEqual(params_dict["host"], "localhost")
        self.assertEqual(params_dict["port"], 5432)
        self.assertEqual(params_dict["user"], "postgres")
        self.assertEqual(params_dict["password"], "password")
        self.assertEqual(params_dict["dbname"], "test")

    def test_to_url(self):
        """Test URL generation."""
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Test the URL generation
        expected_url = "postgresql://postgres:password@localhost:5432/test"
        self.assertEqual(params.to_url(), expected_url)

    def test_string_representation(self):
        """Test string representation (should not include password)."""
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="secret",
            dbname="test",
        )

        # Test string representation (should not expose password)
        str_repr = str(params)
        self.assertIn("localhost", str_repr)
        self.assertIn("postgres", str_repr)
        self.assertIn("test", str_repr)
        self.assertNotIn("secret", str_repr)  # Password should not be included


class TestPGTypes(unittest.TestCase):
    """Test the PGTypes class."""

    def test_basic_types(self):
        """Test basic data type constants."""
        types = PGTypes()

        # Check standard types (PEP-8 style)
        self.assertEqual(types.VARCHAR, "VARCHAR")
        self.assertEqual(types.BIGINT, "BIGINT")
        self.assertEqual(types.INTEGER, "INTEGER")
        self.assertEqual(types.REAL, "REAL")
        self.assertEqual(types.DOUBLE_PRECISION, "DOUBLE PRECISION")
        self.assertEqual(types.TIMESTAMPTZ, "TIMESTAMPTZ")
        self.assertEqual(types.TIMESTAMP, "TIMESTAMP")
        self.assertEqual(types.FLOAT, "FLOAT")
        self.assertEqual(types.JSONB, "JSONB")
        self.assertEqual(types.BOOLEAN, "BOOLEAN")
        self.assertEqual(types.SERIAL, "SERIAL")
        self.assertEqual(types.DECIMAL, "DECIMAL")

        # Check legacy style names
        self.assertEqual(types.VarChar, types.VARCHAR)
        self.assertEqual(types.BigInt, types.BIGINT)
        self.assertEqual(types.Integer, types.INTEGER)
        self.assertEqual(types.Real, types.REAL)
        self.assertEqual(types.DoublePrecision, types.DOUBLE_PRECISION)
        self.assertEqual(types.TimeStampTz, types.TIMESTAMPTZ)
        self.assertEqual(types.TimeStamp, types.TIMESTAMP)
        self.assertEqual(types.Float, types.FLOAT)
        self.assertEqual(types.Jsonb, types.JSONB)
        self.assertEqual(types.Boolean, types.BOOLEAN)
        self.assertEqual(types.serial, types.SERIAL)
        self.assertEqual(types.Decimal, types.DECIMAL)

    def test_lambda_varchar(self):
        """Test VARCHAR with length specification."""
        types = PGTypes()

        # Check lambda generation for VARCHAR with length
        self.assertEqual(types.lambdaVarChar(50), "VARCHAR(50)")
        self.assertEqual(types.lambdaVarChar(100), "VARCHAR(100)")
        self.assertEqual(types.lambdaVarChar(255), "VARCHAR(255)")


class TestPGSchemaParameters(unittest.TestCase):
    """Test the PGSchemaParameters class."""

    def setUp(self):
        """Set up test fixtures."""
        self.types = PGTypes()
        self.dtype_map = {
            "id": self.types.BigInt,
            "name": self.types.VarChar,
            "timestamp": self.types.TimeStampTz,
            "value": self.types.DoublePrecision,
        }

    def test_init(self):
        """Test initialization with valid parameters."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )

        # Test basic properties
        self.assertEqual(params.schema_name, "public")
        self.assertEqual(params.table_name, "test_table")
        self.assertEqual(params.dtype_map, self.dtype_map)
        self.assertEqual(params.time_index, "timestamp")
        self.assertEqual(params.primary_keys, ["id"])

    def test_invalid_time_index(self):
        """Test validation of time_index against dtype_map."""
        # Time index must be present in dtype_map
        with self.assertRaises(ValueError):
            PGSchemaParameters(
                schema_name="public",
                table_name="test_table",
                dtype_map=self.dtype_map,
                time_index="non_existent_column",
            )

    def test_to_dict(self):
        """Test to_dict() method."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )

        # Convert to dict and check values
        params_dict = params.to_dict()
        self.assertEqual(params_dict["schema_name"], "public")
        self.assertEqual(params_dict["table_name"], "test_table")
        self.assertEqual(params_dict["dtype_map"], self.dtype_map)
        self.assertEqual(params_dict["time_index"], "timestamp")
        self.assertEqual(params_dict["primary_keys"], ["id"])

    def test_qualified_name(self):
        """Test qualified_name property."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
        )

        # Test the qualified name property
        self.assertEqual(params.qualified_name, "public.test_table")

    def test_equality(self):
        """Test equality comparison."""
        params1 = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )

        params2 = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )

        params3 = PGSchemaParameters(
            schema_name="different",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )

        # Test equality operators
        self.assertEqual(params1, params2)
        self.assertNotEqual(params1, params3)
        self.assertNotEqual(params1, None)


class TestTSDBSql(unittest.TestCase):
    """Test the TSDBSql class."""

    def setUp(self):
        """Set up test fixtures."""
        self.sql = TSDBSql()
        self.types = PGTypes()

    def test_create_schema(self):
        """Test schema creation SQL."""
        # Test with default if_not_exists=True
        schema_sql = self.sql.create_schema("metrics")
        self.assertEqual(schema_sql, "CREATE SCHEMA IF NOT EXISTS metrics;")

        # Test with if_not_exists=False
        schema_sql = self.sql.create_schema("metrics", if_not_exists=False)
        self.assertEqual(schema_sql, "CREATE SCHEMA metrics;")

    def test_create_table(self):
        """Test table creation SQL."""
        columns = [
            f"id {self.types.BigInt} NOT NULL",
            f"time {self.types.TimeStampTz} NOT NULL",
            f"value {self.types.DoublePrecision}",
        ]

        constraints = [
            "CONSTRAINT pk_test PRIMARY KEY (id, time)",
        ]

        # Test with schema, table, columns, and constraints
        table_sql = self.sql.create_table(
            "metrics", "test", columns, constraints
        )

        # Verify parts of the SQL (ordering of columns might vary)
        self.assertIn("CREATE TABLE metrics.test", table_sql)
        for col in columns:
            self.assertIn(col, table_sql)
        for constraint in constraints:
            self.assertIn(constraint, table_sql)

        # Test with no constraints
        table_sql = self.sql.create_table("metrics", "test", columns)
        self.assertIn("CREATE TABLE metrics.test", table_sql)
        for col in columns:
            self.assertIn(col, table_sql)

    def test_create_hypertable(self):
        """Test hypertable creation SQL."""
        # Test with defaults
        hypertable_sql = self.sql.create_hypertable("metrics", "test", "time")
        self.assertIn("SELECT create_hypertable(", hypertable_sql)
        self.assertIn("'metrics.test'", hypertable_sql)
        self.assertIn("'time'", hypertable_sql)
        self.assertIn("if_not_exists => TRUE", hypertable_sql)

        # Test with custom interval
        hypertable_sql = self.sql.create_hypertable(
            "metrics", "test", "time", "12 hours"
        )
        self.assertIn(
            "chunk_time_interval => interval '12 hours'", hypertable_sql
        )

        # Test with if_not_exists=False
        hypertable_sql = self.sql.create_hypertable(
            "metrics", "test", "time", if_not_exists=False
        )
        self.assertIn("if_not_exists => FALSE", hypertable_sql)

    def test_create_index(self):
        """Test index creation SQL."""
        # Test basic index
        index_sql = self.sql.create_index(
            "metrics", "test", "idx_test", ["time"]
        )
        self.assertEqual(
            index_sql, "CREATE INDEX idx_test ON metrics.test (time);"
        )

        # Test with multiple columns
        index_sql = self.sql.create_index(
            "metrics", "test", "idx_test", ["time", "value"]
        )
        self.assertEqual(
            index_sql, "CREATE INDEX idx_test ON metrics.test (time, value);"
        )

        # Test with different method
        index_sql = self.sql.create_index(
            "metrics", "test", "idx_test", ["time"], method="hash"
        )
        self.assertEqual(
            index_sql,
            "CREATE INDEX idx_test ON metrics.test USING hash (time);",
        )

        # Test with unique flag
        index_sql = self.sql.create_index(
            "metrics", "test", "idx_test", ["time"], unique=True
        )
        self.assertEqual(
            index_sql, "CREATE UNIQUE INDEX idx_test ON metrics.test (time);"
        )

    def test_drop_table(self):
        """Test drop table SQL."""
        # Test with default if_exists=True
        drop_sql = self.sql.drop_table("metrics", "test")
        self.assertEqual(drop_sql, "DROP TABLE IF EXISTS metrics.test CASCADE;")

        # Test with if_exists=False
        drop_sql = self.sql.drop_table("metrics", "test", if_exists=False)
        self.assertEqual(drop_sql, "DROP TABLE metrics.test CASCADE;")


if __name__ == "__main__":
    unittest.main()
