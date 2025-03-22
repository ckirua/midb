"""
Example demonstrating PostgreSQL connection pool with asyncpg using the midb library.

This example shows how to:
1. Create a connection pool using PGConnectionParameters
2. Initialize and use the pool
3. Execute queries concurrently with the pool
4. Use the global registry and context managers
"""

import asyncio
import os
import time

from midb.postgres import (
    PGConnectionParameters,
    PGTypes,
    Pool,
    connection,
    get_pool,
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

    print("Database setup complete.")


async def insert_user(pool: Pool, username: str, email: str) -> int:
    """Insert a user and return their ID."""
    # Use a transaction to ensure atomicity
    async with pool.transaction() as tx:
        user_id = await tx.fetchval(
            "INSERT INTO pool_test.users (username, email) VALUES ($1, $2) RETURNING id",
            username,
            email,
        )
        print(f"Inserted user: {username} with ID {user_id}")
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
    print(f"\nInserting {num_users} users concurrently...")
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
    print(f"Inserted {num_users} users in {duration:.2f} seconds")
    print(f"Average: {num_users / duration:.2f} inserts per second")

    return user_ids


async def demonstrate_connection_context() -> None:
    """Demonstrate the connection context manager."""
    print("\nDemonstrating connection context manager...")

    # Get the 'example' pool from the global registry
    pool = get_pool("example")
    if not pool:
        print("Pool not found. Make sure to initialize it first.")
        return

    # Use the connection context manager
    async with connection("example") as conn:
        # Execute a query directly on the connection
        count = await conn.fetchval("SELECT COUNT(*) FROM pool_test.users")
        print(f"Total users in database: {count}")


async def main():
    """Main function demonstrating connection pool usage."""
    print("===== PostgreSQL Connection Pool Example =====\n")

    # Get database credentials from environment or use defaults
    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", "5432"))
    user = os.environ.get("PG_USER", "postgres")
    password = os.environ.get("PG_PASSWORD", "postgres")
    dbname = os.environ.get("PG_DBNAME", "postgres")

    # Create connection parameters
    conn_params = PGConnectionParameters(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )

    print(f"Creating pool with parameters: {conn_params}")

    # Create a connection pool with name 'example'
    pool = Pool(
        conn_params,
        min_size=5,
        max_size=20,
        name="example",
    )

    try:
        # Initialize the pool
        print("Initializing pool...")
        await pool.initialize()

        # Set up the test database
        await setup_database(pool)

        # Demonstrate concurrent inserts (5 users)
        user_ids = await concurrent_inserts(pool, 5)

        # Fetch and display some users
        print("\nFetching users:")
        for user_id in user_ids[:3]:  # Just show the first 3
            user = await get_user(pool, user_id)
            print(f"User {user_id}: {user['username']} ({user['email']})")

        # Demonstrate the connection context manager
        await demonstrate_connection_context()

        # Use the pool as a context manager
        print("\nUsing pool as a context manager:")
        async with Pool(conn_params, name="context_example") as ctx_pool:
            count = await ctx_pool.fetchval(
                "SELECT COUNT(*) FROM pool_test.users"
            )
            print(f"Users in database from context manager pool: {count}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the pool when done
        print("\nClosing pool...")
        await pool.close()
        print("Pool closed.")


if __name__ == "__main__":
    # Run the async example
    asyncio.run(main())
