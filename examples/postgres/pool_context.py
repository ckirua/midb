"""
Example demonstrating the Pool context manager functionality.

This example shows:
1. How to use the pool as a context manager
2. How to perform database operations within the context
3. How the pool is automatically initialized and cleaned up
"""

import asyncio
import logging
import os
from typing import List

from midb.postgres import PGConnectionParameters, Pool, connection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection_params():
    """Get database connection parameters from environment variables."""
    host = os.getenv("PG_HOST", "localhost")
    port = int(os.getenv("PG_PORT", "5432"))
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "password")
    dbname = os.getenv("PG_DB", "postgres")

    print(f"Connecting to PostgreSQL at {host}:{port} as {user}")

    return PGConnectionParameters(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )


async def create_test_table(pool: Pool) -> None:
    """Create a test table for demonstration."""
    async with connection() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_items (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        logger.info("Created test_items table")


async def insert_items(pool: Pool, items: List[dict]) -> List[int]:
    """Insert multiple items and return their IDs."""
    async with connection() as conn:
        async with conn.transaction():
            ids = []
            for item in items:
                item_id = await conn.fetchval(
                    """
                    INSERT INTO test_items (name, value)
                    VALUES ($1, $2)
                    RETURNING id
                    """,
                    item["name"],
                    item["value"],
                )
                ids.append(item_id)
            return ids


async def get_items(pool: Pool) -> List[dict]:
    """Get all items from the test table."""
    async with connection() as conn:
        rows = await conn.fetch("SELECT * FROM test_items ORDER BY id")
        return [dict(row) for row in rows]


async def main():
    """Demonstrate pool context manager usage."""
    # Create connection parameters
    params = get_connection_params()

    try:
        # Use pool as context manager
        async with Pool(params) as pool:
            # Create test table
            await create_test_table(pool)

            # Insert some test items
            test_items = [
                {"name": "Item 1", "value": 100},
                {"name": "Item 2", "value": 200},
                {"name": "Item 3", "value": 300},
            ]
            item_ids = await insert_items(pool, test_items)
            logger.info(f"Inserted {len(item_ids)} items with IDs: {item_ids}")

            # Retrieve and display items
            items = await get_items(pool)
            print("\nRetrieved items:")
            for item in items:
                print(
                    f"- ID: {item['id']}, Name: {item['name']}, Value: {item['value']}"
                )

            # Demonstrate transaction rollback
            try:
                async with connection() as conn:
                    async with conn.transaction():
                        # Insert an item
                        await conn.execute(
                            "INSERT INTO test_items (name, value) VALUES ($1, $2)",
                            "Item 4",
                            400,
                        )
                        # Force an error to demonstrate rollback
                        raise ValueError("Simulated error")
            except ValueError as e:
                logger.info(f"Transaction rolled back: {e}")

            # Verify the rollback
            items = await get_items(pool)
            print("\nItems after rollback:")
            for item in items:
                print(
                    f"- ID: {item['id']}, Name: {item['name']}, Value: {item['value']}"
                )

    except Exception as e:
        logger.error(f"Error in pool context demo: {e}")


if __name__ == "__main__":
    asyncio.run(main())
