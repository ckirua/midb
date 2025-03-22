"""
Integration tests for the midb.postgres module components.
"""

import unittest

from midb.postgres import (
    PGConnectionParameters,
    PGSchemaParameters,
    PGTypes,
    TSDBSql,
)


class TestComponentIntegration(unittest.TestCase):
    """Test how the different PostgreSQL components work together."""

    def setUp(self):
        """Set up test fixtures."""
        self.types = PGTypes()
        self.sql = TSDBSql()

        # Create schema parameters for a sensor data table
        dtype_map = {
            "time": self.types.TimeStampTz,
            "sensor_id": self.types.VarChar,
            "temperature": self.types.DoublePrecision,
            "humidity": self.types.DoublePrecision,
            "battery": self.types.Real,
        }

        self.schema_params = PGSchemaParameters(
            schema_name="iot",
            table_name="sensor_data",
            dtype_map=dtype_map,
            time_index="time",
            primary_keys=["time", "sensor_id"],
        )

        # Create connection parameters
        self.conn_params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="timeseries",
        )

    def test_schema_creation_workflow(self):
        """Test the workflow for creating a TimescaleDB schema."""
        # Generate column definitions based on schema parameters
        columns = []
        for col_name, col_type in self.schema_params.dtype_map.items():
            nullable = (
                "" if col_name in self.schema_params.primary_keys else " NULL"
            )
            columns.append(f"{col_name} {col_type}{nullable}")

        # Generate constraint based on primary keys
        constraints = []
        if self.schema_params.primary_keys:
            pk_cols = ", ".join(self.schema_params.primary_keys)
            constraints.append(
                f"CONSTRAINT pk_sensor_data PRIMARY KEY ({pk_cols})"
            )

        # 1. Create schema
        schema_sql = self.sql.create_schema(self.schema_params.schema_name)
        self.assertIn(self.schema_params.schema_name, schema_sql)

        # 2. Create table
        table_sql = self.sql.create_table(
            self.schema_params.schema_name,
            self.schema_params.table_name,
            columns,
            constraints,
        )

        # Basic validation of table SQL
        self.assertIn(
            f"CREATE TABLE {self.schema_params.qualified_name}", table_sql
        )
        for col_name in self.schema_params.dtype_map.keys():
            self.assertIn(col_name, table_sql)

        # 3. Convert to hypertable
        hypertable_sql = self.sql.create_hypertable(
            self.schema_params.schema_name,
            self.schema_params.table_name,
            self.schema_params.time_index,
        )

        # Basic validation of hypertable SQL
        self.assertIn(f"'{self.schema_params.qualified_name}'", hypertable_sql)
        self.assertIn(f"'{self.schema_params.time_index}'", hypertable_sql)

    def test_index_creation_from_schema(self):
        """Test creating indexes based on schema parameters."""
        # Create indexes for non-primary key columns
        index_sqls = []
        for col_name in self.schema_params.dtype_map.keys():
            if col_name not in self.schema_params.primary_keys:
                index_name = f"idx_{self.schema_params.table_name}_{col_name}"
                index_sql = self.sql.create_index(
                    self.schema_params.schema_name,
                    self.schema_params.table_name,
                    index_name,
                    [col_name],
                )
                index_sqls.append(index_sql)

        # Verify we have the expected number of indexes
        # (3 non-PK columns: temperature, humidity, battery)
        self.assertEqual(len(index_sqls), 3)

        # Check for each column in the index SQLs
        column_found = {
            "temperature": False,
            "humidity": False,
            "battery": False,
        }

        for sql in index_sqls:
            for col in column_found.keys():
                if f"({col})" in sql:
                    column_found[col] = True

        # Verify all columns were indexed
        for col, found in column_found.items():
            self.assertTrue(found, f"No index created for column {col}")

    def test_query_generation(self):
        """Test generating query SQL with time filtering."""
        # Build a simple query to select data within a time range
        query = f"SELECT * FROM {self.schema_params.qualified_name} "
        query += f"WHERE {self.schema_params.time_index} >= '2023-01-01' "
        query += f"AND {self.schema_params.time_index} <= '2023-01-31' "
        query += f"AND sensor_id = 'sensor1' "
        query += f"ORDER BY {self.schema_params.time_index} DESC "
        query += "LIMIT 100;"

        # This test validates that the schema parameters work correctly with
        # hand-crafted SQL generation, which is what our examples demonstrate
        self.assertIn(self.schema_params.qualified_name, query)
        self.assertIn(self.schema_params.time_index, query)
        self.assertIn("sensor_id = 'sensor1'", query)

        # Drop table validation
        drop_sql = self.sql.drop_table(
            self.schema_params.schema_name,
            self.schema_params.table_name,
        )
        self.assertEqual(
            drop_sql,
            f"DROP TABLE IF EXISTS {self.schema_params.qualified_name} CASCADE;",
        )

    def test_connection_parameter_integration(self):
        """Test using connection parameters with schema parameters."""
        # This simulates building a connection string and query in one step
        conn_url = self.conn_params.to_url()

        # Build a query that could be executed with this connection
        query = f"INSERT INTO {self.schema_params.qualified_name} "
        query += "(time, sensor_id, temperature, humidity, battery) "
        query += "VALUES ($1, $2, $3, $4, $5);"

        # Validate connection URL and query
        self.assertIn("postgres:password@localhost:5432/timeseries", conn_url)
        self.assertIn(self.schema_params.qualified_name, query)


if __name__ == "__main__":
    unittest.main()
