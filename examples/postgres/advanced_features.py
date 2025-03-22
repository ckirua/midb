"""
Combined example demonstrating TimescaleDB and Parameter handling in midb.

This example shows:
1. How to create connection parameters
2. How to define schema parameters with data types
3. How to generate SQL using the TimescaleDB helpers
4. How these components work together in a typical workflow
"""

from midb.postgres import (
    PGConnectionParameters,
    PGSchemaParameters,
    PGTypes,
    TSDBSql,
)


def print_section(title):
    """Helper to print a section title with formatting"""
    print(f"\n{'=' * 50}")
    print(f"  {title}")
    print(f"{'=' * 50}\n")


def main():
    """Demonstrate combined usage of midb PostgreSQL components"""
    print_section("1. Creating Connection Parameters")

    # Create connection parameters
    conn_params = PGConnectionParameters(
        host="timescaledb.example.com",
        port=5432,
        user="postgres",
        password="secure_password",
        dbname="sensor_data",
    )

    print("Connection URL (for asyncpg.connect):")
    print(conn_params.to_url())
    print()

    print("Connection Dictionary (for psycopg2.connect):")
    print(conn_params.to_dict())
    print()

    print("Connection String Representation (password hidden):")
    print(str(conn_params))
    print()

    # Create PG Types for column definitions
    print_section("2. Creating Schema Parameters")
    types = PGTypes()

    # Define data types for a sensor readings table
    dtype_map = {
        "time": types.TimeStampTz,
        "sensor_id": types.VarChar,  # Using default
        "location": types.lambdaVarChar(100),  # Using custom length
        "temperature": types.DoublePrecision,
        "humidity": types.DoublePrecision,
        "battery_level": types.Real,
    }

    # Create schema parameters
    schema_params = PGSchemaParameters(
        schema_name="iot",
        table_name="sensor_readings",
        dtype_map=dtype_map,
        time_index="time",  # Column to use as time index for TimescaleDB
        primary_keys=["time", "sensor_id"],  # Composite primary key
    )

    print(f"Table: {schema_params.qualified_name}")
    print(f"Schema: {schema_params.schema_name}")
    print(f"Table: {schema_params.table_name}")
    print(f"Time Index: {schema_params.time_index}")
    print(f"Primary Keys: {schema_params.primary_keys}")
    print(f"Columns: {len(schema_params.dtype_map)}")
    print()

    print("Schema Parameters Dict:")
    print(schema_params.to_dict())
    print()

    print_section("3. Generating TimescaleDB SQL")

    # Create TimescaleDB SQL generator
    tsdb = TSDBSql()

    # Create the schema
    schema_sql = tsdb.create_schema(schema_params.schema_name)
    print("Create Schema SQL:")
    print(schema_sql)
    print()

    # Build column definitions from schema parameters
    columns = [
        f"{col} {dtype}"
        + (
            " NOT NULL"
            if col in schema_params.primary_keys
            or col == schema_params.time_index
            else ""
        )
        for col, dtype in schema_params.dtype_map.items()
    ]

    # Add primary key constraint if defined
    constraints = []
    if schema_params.primary_keys:
        key_cols = ", ".join(schema_params.primary_keys)
        constraints.append(
            f"CONSTRAINT pk_{schema_params.table_name} PRIMARY KEY ({key_cols})"
        )

    # Generate table creation SQL
    create_sql = tsdb.create_table(
        schema_params.schema_name,
        schema_params.table_name,
        columns,
        constraints,
    )
    print("Create Table SQL:")
    print(create_sql)
    print()

    # Generate hypertable creation SQL
    if schema_params.time_index:
        hypertable_sql = tsdb.create_hypertable(
            schema_params.schema_name,
            schema_params.table_name,
            schema_params.time_index,
            "1 day",  # Chunk interval
        )
        print("Create Hypertable SQL:")
        print(hypertable_sql)
        print()

    # Create some useful indexes
    index_sql = tsdb.create_index(
        schema_params.schema_name,
        schema_params.table_name,
        "idx_sensor_id",
        ["sensor_id"],
    )
    print("Create Sensor ID Index SQL:")
    print(index_sql)
    print()

    # Create a more complex index
    location_time_idx = tsdb.create_index(
        schema_params.schema_name,
        schema_params.table_name,
        "idx_location_time",
        ["location", "time DESC"],
        method="btree",
        unique=False,
    )
    print("Create Location+Time Index SQL:")
    print(location_time_idx)
    print()

    print_section("4. Complete Workflow Example")

    print("Here's how you might use these components in a real application:\n")

    print("# 1. Set up database connection")
    print("conn_params = PGConnectionParameters(...)")
    print("pool = await asyncpg.create_pool(conn_params.to_url())")
    print()

    print("# 2. Define schema parameters")
    print("schema_params = PGSchemaParameters(...)")
    print()

    print("# 3. Generate and execute schema creation SQL")
    print("tsdb = TSDBSql()")
    print("async with pool.acquire() as conn:")
    print("    # Create schema")
    print(
        "    await conn.execute(tsdb.create_schema(schema_params.schema_name))"
    )
    print()
    print("    # Create table")
    print("    columns = [...]  # Generate from schema_params")
    print("    constraints = [...]  # Generate from schema_params")
    print(
        "    await conn.execute(tsdb.create_table(schema_params.schema_name, schema_params.table_name, columns, constraints))"
    )
    print()
    print("    # Convert to hypertable")
    print(
        "    await conn.execute(tsdb.create_hypertable(schema_params.schema_name, schema_params.table_name, schema_params.time_index))"
    )
    print()
    print("    # Create indexes")
    print("    await conn.execute(tsdb.create_index(...))")
    print()

    print("# 4. Later, you can insert data")
    print("async with pool.acquire() as conn:")
    print(f"    await conn.execute('''")
    print(
        f"        INSERT INTO {schema_params.qualified_name} (time, sensor_id, location, temperature, humidity)"
    )
    print(f"        VALUES ($1, $2, $3, $4, $5)")
    print(f"    ''', timestamp, sensor_id, location, temperature, humidity)")


if __name__ == "__main__":
    main()
