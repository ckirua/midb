"""
Example demonstrating PostgreSQL connections with asyncpg using the midb library.

This example shows how to:
1. Create connection parameters using PGConnectionParameters
2. Connect to a PostgreSQL database using the connect() function
3. Execute queries and transactions
"""

import asyncio
import os
from typing import List

from midb.postgres import (
    PGConnectionParameters,
    PGTypes,
    Transaction,
    connect,
    execute_query,
    fetch_all,
    fetch_row,
    fetch_val,
)


async def create_test_database(conn, db_name: str):
    """Create a test database if it doesn't exist."""
    # Check if database exists
    exists = await fetch_val(
        conn,
        "SELECT COUNT(*) FROM pg_database WHERE datname = $1",
        db_name,
    )

    if not exists:
        print(f"Creating database '{db_name}'...")
        await execute_query(conn, f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created.")
    else:
        print(f"Database '{db_name}' already exists.")


async def create_test_table(conn, schema: str, table: str):
    """Create a test schema and table if they don't exist."""
    # Create schema if it doesn't exist
    await execute_query(conn, f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # Create a table with some test data
    types = PGTypes()
    await execute_query(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id SERIAL PRIMARY KEY,
            name {types.lambdaVarChar(100)} NOT NULL,
            created_at {types.TimeStampTz} DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    print(f"Table '{schema}.{table}' is ready.")


async def insert_test_data(conn, schema: str, table: str, records: List[str]):
    """Insert test data into the table using a transaction."""
    async with Transaction(conn) as tx:
        print(f"Inserting {len(records)} records...")
        for name in records:
            await tx.execute(
                f"INSERT INTO {schema}.{table} (name) VALUES ($1)", name
            )
        print("All records inserted successfully.")


async def query_data(conn, schema: str, table: str):
    """Query and display data from the table."""
    # Fetch all rows
    rows = await fetch_all(conn, f"SELECT * FROM {schema}.{table} ORDER BY id")
    print(f"\nFound {len(rows)} records:")
    for row in rows:
        print(
            f"  ID: {row['id']}, Name: {row['name']}, Created: {row['created_at']}"
        )

    # Fetch a single row
    first_row = await fetch_row(
        conn, f"SELECT * FROM {schema}.{table} ORDER BY id LIMIT 1"
    )
    if first_row:
        print(f"\nFirst record: ID={first_row['id']}, Name={first_row['name']}")

    # Fetch a single value
    count = await fetch_val(conn, f"SELECT COUNT(*) FROM {schema}.{table}")
    print(f"Total records: {count}")


async def main():
    """Main function demonstrating database connections and queries."""
    print("===== PostgreSQL Connection Example =====\n")

    # Get database credentials (using environment variables or defaults)
    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", "5432"))
    user = os.environ.get("PG_USER", "postgres")
    password = os.environ.get("PG_PASSWORD", "postgres")
    dbname = os.environ.get("PG_DBNAME", "postgres")
    test_db = "midb_example"

    # Create connection parameters
    conn_params = PGConnectionParameters(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )

    print(f"Connecting to {conn_params}...")

    # Connect to the main PostgreSQL database
    try:
        # First connect to the postgres database to create our test database if needed
        conn = await connect(conn_params)
        print("Connected to PostgreSQL!")

        # Create test database if it doesn't exist
        try:
            await create_test_database(conn, test_db)
        finally:
            await conn.close()

        # Now connect to the test database
        test_conn_params = PGConnectionParameters(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=test_db,
        )

        print(f"\nConnecting to test database {test_db}...")
        conn = await connect(test_conn_params)

        # Set up schema and table
        schema = "test"
        table = "users"

        await create_test_table(conn, schema, table)

        # Insert test data
        test_names = ["Alice", "Bob", "Charlie", "David", "Eve"]
        await insert_test_data(conn, schema, table, test_names)

        # Query the data
        await query_data(conn, schema, table)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if "conn" in locals() and conn:
            print("\nClosing connection...")
            await conn.close()
            print("Connection closed.")


if __name__ == "__main__":
    # Run the async example
    asyncio.run(main())
