"""
Example demonstrating schema validation and handling empty dictionary cases.

This example shows how to:
1. Safely create schema parameters with validation
2. Handle empty dictionary errors gracefully
3. Create and validate tables with proper error handling
"""

import asyncio
import logging
import os
import sys
from typing import Dict, List

print("Script starting...")

try:
    from dotenv import load_dotenv
    from midb.postgres import (
        PGConnectionParameters,
        PGSchemaParameters,
        PGTypes,
        Pool,
    )

    print("Imports successful")
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_schema_safely(
    schema_name: str, table_name: str, columns_dict: Dict[str, str]
) -> PGSchemaParameters:
    """
    Safely create schema parameters with validation for empty dictionaries.

    Args:
        schema_name: The schema name
        table_name: The table name
        columns_dict: Dictionary mapping column names to PostgreSQL data types

    Returns:
        Valid PGSchemaParameters object

    Raises:
        ValueError: If inputs are invalid and cannot be fixed automatically
    """
    print(
        f"Creating schema: {schema_name}.{table_name} with cols: {columns_dict}"
    )
    if columns_dict is None:
        logger.warning("columns_dict is None, creating default columns")
        # Create default columns when None is provided
        types = PGTypes()
        columns_dict = {"id": types.serial, "created_at": types.timestamptz}

    if not columns_dict:
        logger.warning(
            "Empty columns dictionary provided, adding default columns"
        )
        # Handle empty dictionary by adding minimum required columns
        types = PGTypes()
        columns_dict = {"id": types.serial, "created_at": types.timestamptz}

    try:
        # Now create the schema parameters with the validated dictionary
        schema_params = PGSchemaParameters(
            schema_name=schema_name,
            table_name=table_name,
            dtype_map=columns_dict,
        )
        logger.info(
            f"Successfully created schema parameters for {schema_name}.{table_name}"
        )
        return schema_params
    except ValueError as e:
        logger.error(f"Error creating schema parameters: {e}")
        raise


async def create_table_from_params(
    conn, schema_params: PGSchemaParameters
) -> bool:
    """
    Create a table using the provided schema parameters.

    Args:
        conn: Database connection or pool
        schema_params: Validated schema parameters

    Returns:
        True if table creation succeeded
    """
    try:
        # Build the column definitions from the dtype_map
        column_defs = []
        for col_name, col_type in schema_params.dtype_map.items():
            if col_name == "id" and "SERIAL" in col_type:
                column_defs.append(f"{col_name} {col_type} PRIMARY KEY")
            else:
                column_defs.append(f"{col_name} {col_type}")

        # Create the table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_params.qualified_name} (
            {', '.join(column_defs)}
        )
        """
        await conn.execute(create_sql)
        logger.info(f"Created table {schema_params.qualified_name}")
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise


async def validate_schema_exists(conn, schema_name: str) -> bool:
    """Check if a schema exists."""
    try:
        result = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            schema_name,
        )
        return bool(result)
    except Exception as e:
        logger.error(f"Error checking if schema exists: {e}")
        raise


async def validate_table_exists(
    conn, schema_name: str, table_name: str
) -> bool:
    """Check if a table exists."""
    try:
        result = await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = $1 
                AND table_name = $2
            )
            """,
            schema_name,
            table_name,
        )
        return bool(result)
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        raise


async def list_table_columns(
    conn, schema_name: str, table_name: str
) -> List[Dict]:
    """List all columns in a table with their data types."""
    try:
        rows = await conn.fetch(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 
            AND table_name = $2
            ORDER BY ordinal_position
            """,
            schema_name,
            table_name,
        )
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error listing table columns: {e}")
        raise


async def demo_empty_dict_handling():
    """Demonstrate how to handle empty dictionaries in schema parameters."""
    # Case 1: Try with a valid dictionary
    print("\n--- Case 1: Valid Column Dictionary ---")
    types = PGTypes()
    valid_columns = {
        "id": types.serial,
        "name": types.varchar,
        "created_at": types.timestamptz,
    }

    try:
        params1 = create_schema_safely("example", "users", valid_columns)
        print(f"✅ Success: {params1}")
    except ValueError as e:
        print(f"❌ Error: {e}")

    # Case 2: Empty dictionary
    print("\n--- Case 2: Empty Dictionary ---")
    try:
        params2 = create_schema_safely("example", "empty_test", {})
        print(f"✅ Success (auto-fixed): {params2}")
        print(f"Auto-added columns: {params2.dtype_map}")
    except ValueError as e:
        print(f"❌ Error: {e}")

    # Case 3: None dictionary
    print("\n--- Case 3: None Dictionary ---")
    try:
        params3 = create_schema_safely("example", "none_test", None)
        print(f"✅ Success (auto-fixed): {params3}")
        print(f"Auto-added columns: {params3.dtype_map}")
    except ValueError as e:
        print(f"❌ Error: {e}")

    # Case 4: Empty schema name
    print("\n--- Case 4: Empty Schema Name ---")
    try:
        create_schema_safely("", "bad_schema", valid_columns)
        print("✅ Success")
    except ValueError as e:
        print(f"❌ Error (expected): {e}")

    # Case 5: Empty table name
    print("\n--- Case 5: Empty Table Name ---")
    try:
        create_schema_safely("example", "", valid_columns)
        print("✅ Success")
    except ValueError as e:
        print(f"❌ Error (expected): {e}")


async def main():
    """Main function demonstrating schema validation and error handling."""
    print("Entering main function...")
    # Load environment variables from .env file
    load_dotenv("/root/workspace/.env")

    # Get database connection parameters from environment variables
    host = os.getenv("PG_HOST", "localhost")
    port = int(os.getenv("PG_PORT", "5432"))
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "postgres")
    dbname = os.getenv("PG_DB", "postgres")

    print(f"Connecting to PostgreSQL at {host}:{port} as {user}")

    # Create connection parameters
    conn_params = PGConnectionParameters(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )

    # Create a connection pool
    pool = Pool(
        conn_params, min_size=2, max_size=10, name="schema_validation_example"
    )

    try:
        # Initialize the pool
        await pool.initialize()
        logger.info("Connected to PostgreSQL database")

        # First demonstrate empty dictionary handling without database
        await demo_empty_dict_handling()

        print("\n--- Creating Tables From Schema Parameters ---")

        # Create schema if it doesn't exist
        schema_name = "validation_demo"
        await pool.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Define columns for a users table
        types = PGTypes()
        user_columns = {
            "id": types.serial,
            "username": f"{types.varchar}(50) NOT NULL",
            "email": f"{types.varchar}(100) UNIQUE",
            "created_at": f"{types.timestamptz} DEFAULT CURRENT_TIMESTAMP",
        }

        # Create the schema parameters
        users_schema = create_schema_safely(schema_name, "users", user_columns)

        # Create the table
        await create_table_from_params(pool, users_schema)

        # Define columns for a products table
        # Start with an empty dictionary to demonstrate auto-fixing
        product_columns = {}  # This would normally cause a ValueError

        try:
            # This will add default columns since the dictionary is empty
            products_schema = create_schema_safely(
                schema_name, "products", product_columns
            )
            await create_table_from_params(pool, products_schema)
            print(
                f"Created products table with auto-fixed schema: {products_schema.dtype_map}"
            )
        except ValueError as e:
            print(f"Failed to create products schema: {e}")

        # Verify tables exist
        users_exists = await validate_table_exists(pool, schema_name, "users")
        products_exists = await validate_table_exists(
            pool, schema_name, "products"
        )

        print(f"\nUsers table exists: {users_exists}")
        print(f"Products table exists: {products_exists}")

        # List columns in users table
        if users_exists:
            user_columns = await list_table_columns(pool, schema_name, "users")
            print("\nUsers table columns:")
            for col in user_columns:
                nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
                print(f"- {col['column_name']} ({col['data_type']}) {nullable}")

        # List columns in products table
        if products_exists:
            product_columns = await list_table_columns(
                pool, schema_name, "products"
            )
            print("\nProducts table columns:")
            for col in product_columns:
                nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
                print(f"- {col['column_name']} ({col['data_type']}) {nullable}")

        print(
            "\nTables were created successfully and will remain in the database."
        )
        print("You can connect to the database and verify they exist.")
    except Exception as e:
        logger.error(f"Error in schema validation demo: {e}")
        print(f"Error: {e}")
    finally:
        # Close the connection pool
        await pool.close()
        logger.info("Connection pool closed")


if __name__ == "__main__":
    print("Starting main execution...")
    asyncio.run(main())
    print("Script completed.")
