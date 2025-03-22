"""
Tests for the TimescaleDBManager pattern demonstrated in examples.
"""

import unittest
from typing import Dict, List, Optional

from midb.postgres import (
    PGConnectionParameters,
    PGSchemaParameters,
    PGTypes,
    TSDBSql,
)


class TimescaleDBManager:
    """Test implementation of the TimescaleDBManager pattern."""

    def __init__(self, connection_params):
        """Initialize the manager with connection parameters."""
        self.conn_params = connection_params
        self.sql = TSDBSql()
        self.types = PGTypes()
        self.schema_registry = {}

    def register_table(
        self,
        schema_name: str,
        table_name: str,
        columns: Dict[str, str],
        time_column: str,
        primary_keys: Optional[List[str]] = None,
    ) -> PGSchemaParameters:
        """Register a time series table."""
        schema_params = PGSchemaParameters(
            schema_name=schema_name,
            table_name=table_name,
            dtype_map=columns,
            time_index=time_column,
            primary_keys=primary_keys or [time_column],
        )

        table_key = schema_params.qualified_name
        self.schema_registry[table_key] = schema_params
        return schema_params

    def generate_table_creation_sql(self, schema_params):
        """Generate SQL statements for table creation."""
        statements = []

        # Schema creation
        statements.append(self.sql.create_schema(schema_params.schema_name))

        # Column definitions
        columns = []
        for col_name, col_type in schema_params.dtype_map.items():
            is_required = (
                col_name in (schema_params.primary_keys or [])
                or col_name == schema_params.time_index
            )
            column_def = (
                f"{col_name} {col_type}{' NOT NULL' if is_required else ''}"
            )
            columns.append(column_def)

        # Constraints
        constraints = []
        if schema_params.primary_keys:
            key_cols = ", ".join(schema_params.primary_keys)
            constraints.append(
                f"CONSTRAINT pk_{schema_params.table_name} PRIMARY KEY ({key_cols})"
            )

        # Table creation
        statements.append(
            self.sql.create_table(
                schema_params.schema_name,
                schema_params.table_name,
                columns,
                constraints,
            )
        )

        # Hypertable conversion
        if schema_params.time_index:
            statements.append(
                self.sql.create_hypertable(
                    schema_params.schema_name,
                    schema_params.table_name,
                    schema_params.time_index,
                )
            )

        return statements


class TestTimescaleDBManager(unittest.TestCase):
    """Test the TimescaleDBManager pattern."""

    def setUp(self):
        """Set up test fixtures."""
        # Create connection parameters
        self.conn_params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="metrics",
        )

        # Create manager
        self.manager = TimescaleDBManager(self.conn_params)

    def test_table_registration(self):
        """Test registering tables with the manager."""
        # Define types for weather data
        types = self.manager.types
        columns = {
            "time": types.TimeStampTz,
            "station_id": types.VarChar,
            "temperature": types.DoublePrecision,
            "humidity": types.DoublePrecision,
        }

        # Register table
        weather_schema = self.manager.register_table(
            schema_name="weather",
            table_name="observations",
            columns=columns,
            time_column="time",
            primary_keys=["time", "station_id"],
        )

        # Verify registration was successful
        self.assertEqual(weather_schema.schema_name, "weather")
        self.assertEqual(weather_schema.table_name, "observations")
        self.assertEqual(weather_schema.dtype_map, columns)
        self.assertEqual(weather_schema.time_index, "time")
        self.assertEqual(weather_schema.primary_keys, ["time", "station_id"])

        # Verify schema was added to registry
        self.assertIn(
            weather_schema.qualified_name, self.manager.schema_registry
        )

        # Verify we can retrieve it from the registry
        retrieved_schema = self.manager.schema_registry[
            weather_schema.qualified_name
        ]
        self.assertEqual(retrieved_schema, weather_schema)

    def test_table_creation_sql(self):
        """Test generating SQL statements for table creation."""
        # Define types for IoT sensor data
        types = self.manager.types
        columns = {
            "time": types.TimeStampTz,
            "device_id": types.VarChar,
            "temperature": types.Real,
            "battery": types.Real,
        }

        # Register table
        iot_schema = self.manager.register_table(
            schema_name="iot",
            table_name="sensor_data",
            columns=columns,
            time_column="time",
            primary_keys=["time", "device_id"],
        )

        # Generate SQL statements
        statements = self.manager.generate_table_creation_sql(iot_schema)

        # Verify we have the expected number of statements
        # 1. Create schema
        # 2. Create table
        # 3. Create hypertable
        self.assertEqual(len(statements), 3)

        # Verify schema creation
        self.assertIn("CREATE SCHEMA IF NOT EXISTS iot", statements[0])

        # Verify table creation
        table_sql = statements[1]
        self.assertIn("CREATE TABLE iot.sensor_data", table_sql)
        self.assertIn("time TIMESTAMPTZ NOT NULL", table_sql)
        self.assertIn("device_id VARCHAR NOT NULL", table_sql)
        self.assertIn("temperature REAL", table_sql)
        self.assertIn("battery REAL", table_sql)
        self.assertIn(
            "CONSTRAINT pk_sensor_data PRIMARY KEY (time, device_id)", table_sql
        )

        # Verify hypertable creation
        hypertable_sql = statements[2]
        self.assertIn("SELECT create_hypertable(", hypertable_sql)
        self.assertIn("'iot.sensor_data'", hypertable_sql)
        self.assertIn("'time'", hypertable_sql)

    def test_multiple_tables(self):
        """Test managing multiple tables."""
        types = self.manager.types

        # Register first table - weather
        weather_columns = {
            "time": types.TimeStampTz,
            "station_id": types.VarChar,
            "temperature": types.DoublePrecision,
        }

        weather_schema = self.manager.register_table(
            schema_name="weather",
            table_name="observations",
            columns=weather_columns,
            time_column="time",
            primary_keys=["time", "station_id"],
        )

        # Register second table - IoT
        iot_columns = {
            "time": types.TimeStampTz,
            "device_id": types.VarChar,
            "battery": types.Real,
        }

        iot_schema = self.manager.register_table(
            schema_name="iot",
            table_name="devices",
            columns=iot_columns,
            time_column="time",
            primary_keys=["time", "device_id"],
        )

        # Verify both tables are in the registry
        self.assertEqual(len(self.manager.schema_registry), 2)
        self.assertIn(
            weather_schema.qualified_name, self.manager.schema_registry
        )
        self.assertIn(iot_schema.qualified_name, self.manager.schema_registry)

        # Generate SQL for both tables
        weather_sql = self.manager.generate_table_creation_sql(weather_schema)
        iot_sql = self.manager.generate_table_creation_sql(iot_schema)

        # Verify each table's SQL contains the correct schema/table names
        for sql in weather_sql:
            self.assertNotIn("iot.devices", sql)
            if "CREATE TABLE" in sql:
                self.assertIn("weather.observations", sql)

        for sql in iot_sql:
            self.assertNotIn("weather.observations", sql)
            if "CREATE TABLE" in sql:
                self.assertIn("iot.devices", sql)


if __name__ == "__main__":
    unittest.main()
