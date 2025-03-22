# midb - High-Performance Database Interface Library

A lightweight, high-performance database interface library with Cython optimizations.

## Features

- PostgreSQL connection pool wrapper with optimized performance
- Global pool registry for easy access
- Connection acquisition with context manager support
- Transaction support with context manager
- Auto-reconnection capabilities
- Statically typed SQL data types
- Both pure Python and Cython implementations (use Cython for maximum performance)

## Installation

```bash
pip install midb
```

For development:

```bash
pip install -e ".[dev]"
```

## Usage

### Basic usage (pure Python)

```python
import asyncio
from midb.postgres import Pool

async def main():
    # Create a pool with a name
    pool = Pool(
        "postgresql://user:password@localhost/database",
        min_size=5,
        max_size=20,
        name="main"
    )
    
    # Initialize the pool
    await pool.initialize()
    
    try:
        # Execute a query
        result = await pool.fetch("SELECT * FROM users")
        
        # Use SQL types
        await pool.execute(f"""
            CREATE TABLE examples (
                id SERIAL PRIMARY KEY,
                name {pool.types.VarChar}(100) NOT NULL
            )
        """)
        
        # Use a transaction
        async with pool.transaction() as tx:
            await tx.connection.execute(
                "INSERT INTO examples(name) VALUES($1)",
                "Example"
            )
    
    finally:
        # Close the pool when done
        await pool.close()

asyncio.run(main())
```

### Using the Cython-optimized version

```python
import asyncio
from midb.postgres import CPool

async def main():
    # Create a pool with the Cython implementation
    pool = CPool(
        "postgresql://user:password@localhost/database",
        min_size=5,
        max_size=20,
        name="main"
    )
    
    # Usage is identical to the pure Python version
    await pool.initialize()
    
    try:
        result = await pool.fetch("SELECT * FROM examples")
        print(result)
    finally:
        await pool.close()

asyncio.run(main())
```

### Context Manager and Current Pool

```python
import asyncio
from midb.postgres import Pool
from midb.postgres.pool import connection

async def main():
    pool = Pool("postgresql://user:password@localhost/database")
    
    # Using context manager
    async with pool:  # Sets as current pool
        # Get connection from current pool
        conn = await connection()
        try:
            result = await conn.fetchval("SELECT 1")
            print(result)
        finally:
            await pool.release(conn)
    
    # Pool is automatically closed when exiting the context

asyncio.run(main())
```

## Performance

The Cython implementation (`CPool`) provides significantly better performance than the pure Python version, especially for high-throughput applications. In benchmarks, it shows:

- Up to 30% faster connection acquisition
- Reduced CPU usage
- Lower memory footprint

## License

MIT License
