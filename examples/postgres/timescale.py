"""
Example demonstrating the TimescaleDB schema support in midb
"""

from midb.postgres import TSDBSql


def main():
    """Show examples of TimescaleDB schema generation"""
    # Create a helper
    tsdb = TSDBSql()

    print("=== TimescaleDB Schema Example ===\n")

    # Create a schema
    schema_sql = tsdb.create_schema("metrics")
    print("Create Schema SQL:")
    print(schema_sql)
    print()

    # Define column definitions for a sensor readings table
    columns = [
        f"timestamp {tsdb.types.TimeStampTz} NOT NULL",
        f"sensor_id {tsdb.types.lambdaVarChar(64)} NOT NULL",
        f"temperature {tsdb.types.DoublePrecision}",
        f"humidity {tsdb.types.DoublePrecision}",
        f"battery {tsdb.types.Real}",
    ]

    # Define constraints
    constraints = [
        "CONSTRAINT pk_readings PRIMARY KEY (timestamp, sensor_id)",
        "CONSTRAINT valid_temp CHECK (temperature BETWEEN -50 AND 150)",
        "CONSTRAINT valid_humidity CHECK (humidity BETWEEN 0 AND 100)",
    ]

    # Generate SQL statements
    create_sql = tsdb.create_table(
        "metrics", "sensor_readings", columns, constraints
    )
    print("Create Table SQL:")
    print(create_sql)
    print()

    # Create a hypertable
    hypertable_sql = tsdb.create_hypertable(
        "metrics", "sensor_readings", "timestamp", "1 hour"
    )
    print("Create Hypertable SQL:")
    print(hypertable_sql)
    print()

    # Create indexes
    index_sql = tsdb.create_index(
        "metrics", "sensor_readings", "readings_sensor_idx", ["sensor_id"]
    )
    print("Create Index SQL:")
    print(index_sql)
    print()

    # More advanced index
    advanced_index_sql = tsdb.create_index(
        "metrics",
        "sensor_readings",
        "readings_low_battery_idx",
        ["sensor_id", "timestamp"],
        method="btree",
        unique=False,
    )
    print("Advanced Index SQL:")
    print(advanced_index_sql)
    print()

    # Drop table
    drop_sql = tsdb.drop_table("metrics", "sensor_readings")
    print("Drop Table SQL:")
    print(drop_sql)


if __name__ == "__main__":
    main()
