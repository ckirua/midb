# midb - High-Performance Database Interface Library

A lightweight, high-performance database interface library with Cython optimizations.

## Features

- PostgreSQL connection pool wrapper with optimized performance
- Global pool registry for easy access
- Connection acquisition with context manager support
- Transaction support with context manager
- Auto-reconnection capabilities
- Statically typed SQL data types
- Environment variable support for configuration
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

### Basic usage with environment variables

```python
import asyncio
import os
from dotenv import load_dotenv
from midb.postgres import Pool, PGConnectionParameters

# Load environment variables
load_dotenv()

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

async def main():
    # Create connection parameters
    params = get_connection_params()
    
    # Use pool as context manager
    async with Pool(params) as pool:
        # Execute queries
        result = await pool.fetch("SELECT * FROM users")
        print(result)

asyncio.run(main())
```

### Using the connection context manager

```python
import asyncio
from midb.postgres import Pool, connection

async def main():
    params = PGConnectionParameters(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        dbname="postgres"
    )
    
    async with Pool(params) as pool:
        # Get connection from current pool
        async with connection() as conn:
            # Use transaction
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO users(name) VALUES($1)",
                    "John Doe"
                )
                user_id = await conn.fetchval(
                    "INSERT INTO users(name) VALUES($1) RETURNING id",
                    "Jane Smith"
                )
                
                # Fetch results
                users = await conn.fetch("SELECT * FROM users")
                print(users)

asyncio.run(main())
```

### Using SQL Types

```python
import asyncio
from midb.postgres import Pool, PGTypes

async def main():
    pool = Pool("postgresql://user:password@localhost/database")
    types = PGTypes()
    
    async with pool:
        # Create table with typed columns
        await pool.execute(f"""
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name {types.VarChar}(100) NOT NULL,
                price {types.Decimal}(10,2) NOT NULL,
                created_at {types.TimeStampTz} DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert data
        await pool.execute(
            "INSERT INTO products(name, price) VALUES($1, $2)",
            "Laptop",
            999.99
        )

asyncio.run(main())
```

## Environment Variables

The library supports configuration through environment variables:

- `PG_HOST`: Database host (default: "localhost")
- `PG_PORT`: Database port (default: 5432)
- `PG_USER`: Database user (default: "postgres")
- `PG_PASSWORD`: Database password (default: "password")
- `PG_DB`: Database name (default: "postgres")

## Performance

The Cython implementation (`CPool`) provides significantly better performance than the pure Python version, especially for high-throughput applications. In benchmarks, it shows:

- Up to 30% faster connection acquisition
- Reduced CPU usage
- Lower memory footprint

## Examples

Check out the `examples/postgres` directory for more detailed examples:

- `pool_context.py`: Demonstrates pool and connection context managers
- `crud.py`: Shows CRUD operations with transactions
- `schema_validation.py`: Shows schema validation and type handling
- `combined.py`: Demonstrates TimescaleDB integration

## License

MIT License
