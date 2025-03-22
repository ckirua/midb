"""
Examples demonstrating TimescaleDB integration with midb.

This example shows:
1. Creating hypertables
2. Inserting time-series data
3. Querying time-series data
4. Using TimescaleDB-specific functions
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from midb.postgres import PGConnectionParameters, PGTypes, Pool

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def get_connection_params():
    """Get database connection parameters from environment variables."""
    return PGConnectionParameters(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB", "postgres"),
    )


async def setup_timescale_schema(pool: Pool) -> None:
    """Create TimescaleDB schema and hypertable."""
    types = PGTypes()

    # Create schema
    await pool.execute("CREATE SCHEMA IF NOT EXISTS timescale_test")

    # Create regular table first
    await pool.execute(
        f"""
        CREATE TABLE IF NOT EXISTS timescale_test.measurements (
            time {types.TimeStampTz} NOT NULL,
            device_id {types.VarChar}(50) NOT NULL,
            temperature {types.Decimal}(5,2) NOT NULL,
            humidity {types.Decimal}(5,2) NOT NULL
        )
        """
    )

    # Convert to hypertable
    await pool.execute(
        """
        SELECT create_hypertable('timescale_test.measurements', 'time', 
                               if_not_exists => TRUE)
        """
    )

    # Create index on device_id and time
    await pool.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_measurements_device_time 
        ON timescale_test.measurements (device_id, time DESC)
        """
    )

    logger.info("TimescaleDB schema setup complete")


async def insert_sample_data(pool: Pool) -> None:
    """Insert sample time-series data."""
    # Generate sample data
    base_time = datetime.now()
    devices = ["device1", "device2", "device3"]

    # Insert data for each device
    for device in devices:
        values = []
        for i in range(10):
            time = base_time + timedelta(minutes=i)
            values.append(f"('{time}', '{device}', {20 + i}, {40 + i})")

        # Insert in batches
        await pool.execute(
            f"""
            INSERT INTO timescale_test.measurements 
            (time, device_id, temperature, humidity)
            VALUES {','.join(values)}
            """
        )

    logger.info(f"Inserted sample data for {len(devices)} devices")


async def query_time_bucket(pool: Pool) -> None:
    """Demonstrate time_bucket function usage."""
    logger.info("\nQuerying data with time_bucket:")

    # Get hourly averages
    result = await pool.fetch(
        """
        SELECT 
            time_bucket('1 hour', time) AS hour,
            device_id,
            avg(temperature) as avg_temp,
            avg(humidity) as avg_humidity
        FROM timescale_test.measurements
        GROUP BY hour, device_id
        ORDER BY hour DESC, device_id
        """
    )

    for row in result:
        logger.info(
            f"Hour: {row['hour']}, Device: {row['device_id']}, "
            f"Avg Temp: {row['avg_temp']}, Avg Humidity: {row['avg_humidity']}"
        )


async def query_first_last_values(pool: Pool) -> None:
    """Demonstrate first and last value functions."""
    logger.info("\nQuerying first and last values:")

    result = await pool.fetch(
        """
        SELECT 
            device_id,
            first(temperature, time) as first_temp,
            last(temperature, time) as last_temp,
            first(humidity, time) as first_humidity,
            last(humidity, time) as last_humidity
        FROM timescale_test.measurements
        GROUP BY device_id
        """
    )

    for row in result:
        logger.info(
            f"Device: {row['device_id']}, "
            f"Temperature: {row['first_temp']} -> {row['last_temp']}, "
            f"Humidity: {row['first_humidity']} -> {row['last_humidity']}"
        )


async def main():
    """Main function demonstrating TimescaleDB features."""
    logger.info("===== TimescaleDB Examples =====\n")

    # Create connection parameters
    conn_params = get_connection_params()

    # Use pool as context manager
    async with Pool(conn_params) as pool:
        try:
            # Set up schema
            await setup_timescale_schema(pool)

            # Insert sample data
            await insert_sample_data(pool)

            # Demonstrate various TimescaleDB queries
            await query_time_bucket(pool)
            await query_first_last_values(pool)

        except Exception as e:
            logger.error(f"Error in TimescaleDB demo: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(main())
