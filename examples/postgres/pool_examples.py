"""
Comprehensive examples demonstrating PostgreSQL connection pool functionality.

This example shows:
1. Basic pool creation and management
2. Pool context manager usage
3. Connection context manager usage
4. Transaction handling
5. Concurrent operations
6. Error handling and rollback
"""

import asyncio
import logging
import os
import time

from midb.postgres import PGConnectionParameters, PGTypes, Pool, connection

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

    return PGConnectionParameters(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )


async def setup_database(pool: Pool) -> None:
    """Create a test schema and table for the example."""
    # Create schema if it doesn't exist
    await pool.execute("CREATE SCHEMA IF NOT EXISTS pool_test")

    # Create a simple test table
    types = PGTypes()
    await pool.execute(
        f"""
        CREATE TABLE IF NOT EXISTS pool_test.users (
            id SERIAL PRIMARY KEY,
            username {types.lambdaVarChar(50)} NOT NULL,
            email {types.lambdaVarChar(100)} NOT NULL,
            created_at {types.TimeStampTz} DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Clear any existing data for clean example
    await pool.execute("TRUNCATE pool_test.users RESTART IDENTITY")
    logger.info("Database setup complete.")


async def insert_user(pool: Pool, username: str, email: str) -> int:
    """Insert a user and return their ID."""
    async with pool.transaction() as tx:
        user_id = await tx.fetchval(
            "INSERT INTO pool_test.users (username, email) VALUES ($1, $2) RETURNING id",
            username,
            email,
        )
        logger.info(f"Inserted user: {username} with ID {user_id}")
        return user_id


async def get_user(pool: Pool, user_id: int) -> dict:
    """Get a user by ID."""
    row = await pool.fetchrow(
        "SELECT * FROM pool_test.users WHERE id = $1", user_id
    )
    if row:
        return dict(row)
    return None


async def concurrent_inserts(pool: Pool, num_users: int) -> None:
    """Demonstrate concurrent inserts with the connection pool."""
    logger.info(f"Inserting {num_users} users concurrently...")
    start_time = time.time()

    # Create tasks for concurrent insertion
    tasks = []
    for i in range(1, num_users + 1):
        username = f"user{i}"
        email = f"user{i}@example.com"
        tasks.append(insert_user(pool, username, email))

    # Wait for all inserts to complete
    user_ids = await asyncio.gather(*tasks)

    # Measure and display performance
    duration = time.time() - start_time
    logger.info(f"Inserted {num_users} users in {duration:.2f} seconds")
    logger.info(f"Average: {num_users / duration:.2f} inserts per second")

    return user_ids


async def demonstrate_transaction_rollback(pool: Pool) -> None:
    """Demonstrate transaction rollback functionality."""
    logger.info("\nDemonstrating transaction rollback...")

    try:
        async with connection() as conn:
            async with conn.transaction():
                # Insert a user
                await conn.execute(
                    "INSERT INTO pool_test.users (username, email) VALUES ($1, $2)",
                    "rollback_user",
                    "rollback@example.com",
                )
                # Force an error to demonstrate rollback
                raise ValueError("Simulated error")
    except ValueError as e:
        logger.info(f"Transaction rolled back: {e}")

    # Verify the rollback
    count = await pool.fetchval(
        "SELECT COUNT(*) FROM pool_test.users WHERE username = 'rollback_user'"
    )
    logger.info(f"Users after rollback: {count}")


async def main():
    """Main function demonstrating various pool features."""
    logger.info("===== PostgreSQL Connection Pool Examples =====\n")

    # Create connection parameters
    conn_params = get_connection_params()
    logger.info(f"Using connection parameters: {conn_params}")

    # Example 1: Basic pool usage
    logger.info("\nExample 1: Basic Pool Usage")
    pool = Pool(
        conn_params,
        min_size=5,
        max_size=20,
        name="example",
    )

    try:
        # Initialize the pool
        await pool.initialize()

        # Set up the test database
        await setup_database(pool)

        # Demonstrate concurrent inserts
        user_ids = await concurrent_inserts(pool, 5)

        # Fetch and display some users
        logger.info("\nFetching users:")
        for user_id in user_ids[:3]:
            user = await get_user(pool, user_id)
            logger.info(f"User {user_id}: {user['username']} ({user['email']})")

    finally:
        await pool.close()

    # Example 2: Pool Context Manager
    logger.info("\nExample 2: Pool Context Manager")
    async with Pool(conn_params, name="context_example") as ctx_pool:
        # Create test table
        await setup_database(ctx_pool)

        # Insert some test users
        test_users = [
            {"username": "ctx_user1", "email": "ctx1@example.com"},
            {"username": "ctx_user2", "email": "ctx2@example.com"},
        ]

        for user in test_users:
            await insert_user(ctx_pool, user["username"], user["email"])

        # Demonstrate transaction rollback
        await demonstrate_transaction_rollback(ctx_pool)

        # Show final state
        count = await ctx_pool.fetchval("SELECT COUNT(*) FROM pool_test.users")
        logger.info(f"Final user count: {count}")


if __name__ == "__main__":
    asyncio.run(main())
