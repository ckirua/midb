import asyncio
import logging
import os
from typing import Dict, List, Optional, Union

from dotenv import load_dotenv
from midb.postgres import PGConnectionParameters, PGTypes, Pool

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Product type hint
Product = Dict[str, Union[int, str, float]]


async def setup_schema(conn) -> None:
    """Create the necessary schema and tables."""
    try:
        types = PGTypes()

        # Create products table
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.products (
                id SERIAL PRIMARY KEY,
                name {types.VarChar} NOT NULL,
                description {types.VarChar},
                price {types.DoublePrecision} NOT NULL,
                quantity {types.BigInt} NOT NULL DEFAULT 0,
                created_at {types.TimeStampTz} DEFAULT CURRENT_TIMESTAMP,
                updated_at {types.TimeStampTz} DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        logger.info("Schema setup complete")
    except Exception as e:
        logger.error(f"Error setting up schema: {e}")
        raise


async def create_product(
    conn, name: str, description: str, price: float, quantity: int = 0
) -> int:
    """Create a new product and return its ID."""
    try:
        product_id = await conn.fetchval(
            """
            INSERT INTO public.products (name, description, price, quantity) 
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            name,
            description,
            price,
            quantity,
        )
        logger.info(f"Created product {name} with ID {product_id}")
        return product_id
    except Exception as e:
        logger.error(f"Error creating product: {e}")
        raise


async def create_products(conn, products: List[Product]) -> List[int]:
    """Create multiple products in a single transaction."""
    try:
        product_ids = []

        # Start a transaction using the connection's transaction method
        async with conn.transaction():
            for product in products:
                product_id = await conn.fetchval(
                    """
                    INSERT INTO public.products (name, description, price, quantity) 
                    VALUES ($1, $2, $3, $4)
                    RETURNING id
                    """,
                    product["name"],
                    product.get("description", ""),
                    product["price"],
                    product.get("quantity", 0),
                )
                product_ids.append(product_id)

        logger.info(f"Created {len(product_ids)} products in batch")
        return product_ids
    except Exception as e:
        logger.error(f"Error creating products in batch: {e}")
        raise


async def get_product(conn, product_id: int) -> Optional[Product]:
    """Get a product by ID."""
    try:
        row = await conn.fetchrow(
            "SELECT * FROM public.products WHERE id = $1", product_id
        )
        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error(f"Error getting product {product_id}: {e}")
        raise


async def search_products(conn, name_pattern: str) -> List[Product]:
    """Search for products by name pattern."""
    try:
        rows = await conn.fetch(
            "SELECT * FROM public.products WHERE name ILIKE $1",
            f"%{name_pattern}%",
        )
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(
            f"Error searching products with pattern '{name_pattern}': {e}"
        )
        raise


async def list_products(
    conn, limit: int = 100, offset: int = 0
) -> List[Product]:
    """List all products with pagination."""
    try:
        rows = await conn.fetch(
            "SELECT * FROM public.products ORDER BY id LIMIT $1 OFFSET $2",
            limit,
            offset,
        )
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error listing products: {e}")
        raise


async def update_product(conn, product_id: int, **kwargs) -> bool:
    """Update product attributes."""
    if not kwargs:
        return False

    try:
        # Build SET clause dynamically
        set_parts = []
        params = [product_id]
        param_index = 2  # Starting from $2

        for key, value in kwargs.items():
            set_parts.append(f"{key} = ${param_index}")
            params.append(value)
            param_index += 1

        # Always update the updated_at timestamp
        set_parts.append("updated_at = CURRENT_TIMESTAMP")

        query = f"""
            UPDATE public.products 
            SET {', '.join(set_parts)}
            WHERE id = $1
            RETURNING id
        """

        result = await conn.fetchval(query, *params)
        success = result is not None

        if success:
            logger.info(f"Updated product {product_id}")
        else:
            logger.warning(f"Product {product_id} not found for update")

        return success
    except Exception as e:
        logger.error(f"Error updating product {product_id}: {e}")
        raise


async def update_stock(
    conn, product_id: int, quantity_change: int
) -> Optional[int]:
    """Update product stock quantity, returning the new quantity."""
    try:
        new_quantity = await conn.fetchval(
            """
            UPDATE public.products
            SET quantity = quantity + $2, 
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $1 AND (quantity + $2) >= 0
            RETURNING quantity
            """,
            product_id,
            quantity_change,
        )

        if new_quantity is not None:
            logger.info(
                f"Updated product {product_id} stock by {quantity_change}, new quantity: {new_quantity}"
            )
        else:
            logger.warning(
                f"Could not update product {product_id} stock (insufficient quantity or product not found)"
            )

        return new_quantity
    except Exception as e:
        logger.error(f"Error updating product {product_id} stock: {e}")
        raise


async def delete_product(conn, product_id: int) -> bool:
    """Delete a product by ID."""
    try:
        result = await conn.fetchval(
            "DELETE FROM public.products WHERE id = $1 RETURNING id", product_id
        )
        success = result is not None

        if success:
            logger.info(f"Deleted product {product_id}")
        else:
            logger.warning(f"Product {product_id} not found for deletion")

        return success
    except Exception as e:
        logger.error(f"Error deleting product {product_id}: {e}")
        raise


async def delete_products(conn, product_ids: List[int]) -> int:
    """Delete multiple products by IDs, returning count of deleted products."""
    if not product_ids:
        return 0

    try:
        # Start a transaction using the connection's transaction method
        async with conn.transaction():
            # First count how many products will be deleted
            count_query = (
                "SELECT COUNT(*) FROM public.products WHERE id = ANY($1::int[])"
            )
            count = await conn.fetchval(count_query, product_ids)

            # Then delete them
            delete_query = (
                "DELETE FROM public.products WHERE id = ANY($1::int[])"
            )
            await conn.execute(delete_query, product_ids)

            logger.info(f"Deleted {count} products")
            return count
    except Exception as e:
        logger.error(f"Error deleting products: {e}")
        raise


async def drop_products_table(conn) -> None:
    """Drop the products table to clean up after the demo."""
    try:
        await conn.execute("DROP TABLE IF EXISTS public.products CASCADE")
        logger.info("Products table dropped")
    except Exception as e:
        logger.error(f"Error dropping products table: {e}")
        raise


async def main():
    """Main function to demonstrate CRUD operations."""
    # Load environment variables from .env file
    load_dotenv("/root/workspace/.env")

    # Get database connection parameters from environment variables
    host = os.getenv("PG_HOST", "localhost")
    port = int(os.getenv("PG_PORT", "5432"))
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "postgres")
    dbname = os.getenv(
        "PG_DB", "postgres"
    )  # Using PG_DB from .env instead of PG_DBNAME

    # Print all environment variables for debugging
    print("\n--- Environment Variables ---")
    print(f"PG_HOST from env: {os.getenv('PG_HOST', 'Not set')}")
    print(f"PG_PORT from env: {os.getenv('PG_PORT', 'Not set')}")
    print(f"PG_USER from env: {os.getenv('PG_USER', 'Not set')}")
    print(
        f"PG_PASSWORD from env: {'*****' if os.getenv('PG_PASSWORD') else 'Not set'}"
    )
    print(f"PG_DB from env: {os.getenv('PG_DB', 'Not set')}")
    print(f"PG_DBNAME from env: {os.getenv('PG_DBNAME', 'Not set')}")
    print("----------------------------\n")

    print(f"Connecting to PostgreSQL at {host}:{port} as {user}")

    # Create connection parameters
    conn_params = PGConnectionParameters(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )

    # Print full connection URL (with password masked)
    url = conn_params.to_url()
    masked_url = url.replace(password, "******")
    print(f"Connection URL: {masked_url}")

    # Print connection parameters as dictionary (with password masked)
    params_dict = conn_params.to_dict()
    params_dict_safe = params_dict.copy()
    params_dict_safe["password"] = "******"
    print(f"Connection parameters: {params_dict_safe}")

    # Create a connection pool
    pool = Pool(conn_params, min_size=2, max_size=10, name="crud_example")

    try:
        # Initialize the pool
        await pool.initialize()
        logger.info("Connected to PostgreSQL database")

        # Set up schema
        await setup_schema(pool)

        # Demo: Create products
        product_id = await create_product(
            pool,
            name="Laptop",
            description="High-performance laptop with 16GB RAM",
            price=999.99,
            quantity=10,
        )

        # Create multiple products
        batch_products = [
            {
                "name": "Smartphone",
                "description": "Latest model",
                "price": 699.99,
                "quantity": 20,
            },
            {
                "name": "Headphones",
                "description": "Noise-cancelling",
                "price": 149.99,
                "quantity": 30,
            },
            {
                "name": "Tablet",
                "description": "10-inch screen",
                "price": 349.99,
                "quantity": 15,
            },
        ]
        batch_ids = await create_products(pool, batch_products)

        # Demo: Read operations
        laptop = await get_product(pool, product_id)
        if laptop:
            print(f"Retrieved product: {laptop['name']} - ${laptop['price']}")

        # Search products
        search_results = await search_products(pool, "phone")
        print(f"Found {len(search_results)} products matching 'phone'")

        # List all products
        all_products = await list_products(pool)
        print(f"Total products: {len(all_products)}")
        for product in all_products:
            print(
                f"- {product['id']}: {product['name']} (${product['price']}, qty: {product['quantity']})"
            )

        # Demo: Update operations
        # Update product details
        await update_product(
            pool,
            product_id,
            price=1099.99,
            description="High-performance laptop with 32GB RAM and SSD",
        )

        # Update stock levels
        await update_stock(pool, product_id, -2)  # Decrease stock by 2

        # Verify the updates
        updated_laptop = await get_product(pool, product_id)
        if updated_laptop:
            print(
                f"Updated product: {updated_laptop['name']} - ${updated_laptop['price']}"
            )
            print(f"New quantity: {updated_laptop['quantity']}")

        # Demo: Delete operations
        # Delete one product
        if batch_ids:
            await delete_product(pool, batch_ids[0])

        # Delete multiple products
        if len(batch_ids) > 1:
            await delete_products(pool, batch_ids[1:])

        # Verify deletions
        remaining_products = await list_products(pool)
        print(f"Remaining products after deletion: {len(remaining_products)}")

        # Clean up by dropping the table
        print("\n--- Cleaning up ---")
        await drop_products_table(pool)

        # Verify table is dropped by trying to list products (should fail)
        try:
            await list_products(pool)
        except Exception as e:
            print(f"Verified table is dropped: {e}")

    except Exception as e:
        logger.error(f"Error in CRUD demo: {e}")
    finally:
        # Close the connection pool
        await pool.close()
        logger.info("Connection pool closed")


if __name__ == "__main__":
    asyncio.run(main())
