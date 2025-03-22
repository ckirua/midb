"""
TimescaleDB Manager Example

This example demonstrates creating a higher-level abstraction
for TimescaleDB database management using the optimized
components from midb.
"""

import asyncio
from typing import Dict, List, Optional

from midb.postgres import (
    PGConnectionParameters,
    PGSchemaParameters,
    PGTypes,
    TSDBSql,
)


class TimescaleDBManager:
    """High-level manager for TimescaleDB operations."""

    def __init__(self, connection_params: PGConnectionParameters):
        """Initialize the manager with connection parameters."""
        self.conn_params = connection_params
        self.sql = TSDBSql()
        self.types = PGTypes()
        self.schema_registry: Dict[str, PGSchemaParameters] = {}

    def register_table(
        self,
        schema_name: str,
        table_name: str,
        columns: Dict[str, str],
        time_column: str,
        primary_keys: Optional[List[str]] = None,
    ) -> PGSchemaParameters:
        """
        Register a new time series table with the manager.

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: Dictionary of column names and their PostgreSQL types
            time_column: The time column for the hypertable
            primary_keys: Optional list of primary key columns

        Returns:
            The created schema parameters object
        """
        # Create schema parameters
        schema_params = PGSchemaParameters(
            schema_name=schema_name,
            table_name=table_name,
            dtype_map=columns,
            time_index=time_column,
            primary_keys=primary_keys or [time_column],
        )

        # Store in registry
        table_key = schema_params.qualified_name
        self.schema_registry[table_key] = schema_params
        return schema_params

    def generate_table_creation_sql(
        self, schema_params: PGSchemaParameters
    ) -> List[str]:
        """
        Generate SQL statements for creating a TimescaleDB table.

        Args:
            schema_params: The schema parameters

        Returns:
            List of SQL statements to execute
        """
        statements = []

        # 1. Create schema if needed
        statements.append(self.sql.create_schema(schema_params.schema_name))

        # 2. Build column definitions
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

        # 3. Build constraints
        constraints = []
        if schema_params.primary_keys:
            key_cols = ", ".join(schema_params.primary_keys)
            constraints.append(
                f"CONSTRAINT pk_{schema_params.table_name} PRIMARY KEY ({key_cols})"
            )

        # 4. Create table
        statements.append(
            self.sql.create_table(
                schema_params.schema_name,
                schema_params.table_name,
                columns,
                constraints,
            )
        )

        # 5. Convert to hypertable
        if schema_params.time_index:
            statements.append(
                self.sql.create_hypertable(
                    schema_params.schema_name,
                    schema_params.table_name,
                    schema_params.time_index,
                )
            )

            # 6. Create additional index on time column if it's not part of the primary key
            if (
                schema_params.primary_keys
                and schema_params.time_index not in schema_params.primary_keys
            ):
                statements.append(
                    self.sql.create_index(
                        schema_params.schema_name,
                        schema_params.table_name,
                        f"idx_{schema_params.table_name}_{schema_params.time_index}",
                        [schema_params.time_index],
                    )
                )

        return statements

    def generate_insert_sql(
        self, schema_params: PGSchemaParameters, columns: List[str]
    ) -> str:
        """
        Generate SQL for inserting data into a table.

        Args:
            schema_params: The schema parameters
            columns: List of columns to insert

        Returns:
            SQL statement for insertion
        """
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        columns_str = ", ".join(columns)

        return (
            f"INSERT INTO {schema_params.qualified_name} "
            f"({columns_str}) VALUES ({placeholders})"
        )

    def generate_query_sql(
        self,
        schema_params: PGSchemaParameters,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        filters: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> str:
        """
        Generate SQL for querying time series data.

        Args:
            schema_params: The schema parameters
            start_time: Optional start time for filtering
            end_time: Optional end time for filtering
            filters: Optional additional filters
            limit: Optional result limit

        Returns:
            SQL query statement
        """
        # Start building the query
        query = f"SELECT * FROM {schema_params.qualified_name}"

        # Add time range filter if specified
        conditions = []
        if start_time and schema_params.time_index:
            conditions.append(f"{schema_params.time_index} >= '{start_time}'")

        if end_time and schema_params.time_index:
            conditions.append(f"{schema_params.time_index} <= '{end_time}'")

        # Add additional filters
        if filters:
            for col, value in filters.items():
                if col in schema_params.dtype_map:
                    conditions.append(f"{col} = '{value}'")

        # Add WHERE clause if there are conditions
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Add ORDER BY time
        if schema_params.time_index:
            query += f" ORDER BY {schema_params.time_index} DESC"

        # Add LIMIT if specified
        if limit:
            query += f" LIMIT {limit}"

        return query + ";"


def simulate_async_pool():
    """Simulate an async pool for demonstration purposes only."""

    class MockConnection:
        async def execute(self, query, *args):
            print(f"Executing query: {query}")
            print(f"With args: {args}")
            return None

        async def fetch(self, query, *args):
            print(f"Fetching results from: {query}")
            print(f"With args: {args}")
            return [{"time": "2023-01-01", "value": 42}]

    class MockPool:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        async def acquire(self):
            return MockConnection()

    return MockPool()


async def main():
    """Demonstrate TimescaleDB Manager usage."""
    print("\n======= TimescaleDB Manager Example =======\n")

    # Create connection parameters
    conn_params = PGConnectionParameters(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        dbname="metrics",
    )

    # Create manager
    manager = TimescaleDBManager(conn_params)

    # Define column types for a weather station table
    types = manager.types
    columns = {
        "time": types.TimeStampTz,
        "station_id": types.VarChar,
        "temperature": types.DoublePrecision,
        "humidity": types.DoublePrecision,
        "pressure": types.DoublePrecision,
        "wind_speed": types.Real,
        "wind_direction": types.Real,
        "precipitation": types.Real,
    }

    # Register the table with the manager
    weather_schema = manager.register_table(
        schema_name="weather",
        table_name="observations",
        columns=columns,
        time_column="time",
        primary_keys=["time", "station_id"],
    )

    print("Registered table:", weather_schema.qualified_name)
    print("Table structure:", weather_schema.to_dict())
    print()

    # Generate table creation SQL
    creation_statements = manager.generate_table_creation_sql(weather_schema)
    print("Table Creation SQL Statements:")
    for i, stmt in enumerate(creation_statements, 1):
        print(f"{i}. {stmt}")
    print()

    # Generate insert SQL
    insert_columns = [
        "time",
        "station_id",
        "temperature",
        "humidity",
        "precipitation",
    ]
    insert_sql = manager.generate_insert_sql(weather_schema, insert_columns)
    print("Insert SQL:")
    print(insert_sql)
    print()

    # Generate query SQL
    query_sql = manager.generate_query_sql(
        weather_schema,
        start_time="2023-01-01",
        end_time="2023-01-31",
        filters={"station_id": "NYC001"},
        limit=100,
    )
    print("Query SQL:")
    print(query_sql)
    print()

    # Demonstrate how this would be used with a real database
    print("Simulating database interaction:")

    # Create a simulated connection pool (in real code, use asyncpg or similar)
    mock_pool = simulate_async_pool()

    # Execute table creation
    async with mock_pool as pool:
        conn = await pool.acquire()
        for stmt in creation_statements:
            await conn.execute(stmt)

    # Insert sample data
    async with mock_pool as pool:
        conn = await pool.acquire()
        await conn.execute(
            insert_sql,
            "2023-01-01 12:00:00",
            "NYC001",
            72.5,  # temperature
            65.0,  # humidity
            0.0,  # precipitation
        )

    # Query data
    async with mock_pool as pool:
        conn = await pool.acquire()
        results = await conn.fetch(query_sql)
        print(f"\nQuery returned {len(results)} results")
        print("Sample result:", results[0])


if __name__ == "__main__":
    asyncio.run(main())
