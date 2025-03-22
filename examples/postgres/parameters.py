"""
Example demonstrating the parameter handling in midb
"""

from midb.postgres import PGConnectionParameters, PGSchemaParameters, PGTypes


def main():
    """Show examples of parameter handling"""
    print("=== PostgreSQL Parameters Example ===\n")

    # Create connection parameters
    conn_params = PGConnectionParameters(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        dbname="example",
    )

    print("Connection URL:")
    print(conn_params.to_url())
    print()

    print("Connection Dictionary:")
    print(conn_params.to_dict())
    print()

    print("Connection String Representation:")
    print(str(conn_params))  # Notice password is not shown for security
    print()

    # Create schema parameters with PGTypes
    types = PGTypes()
    dtype_map = {
        "id": types.BigInt,
        "name": types.lambdaVarChar(100),
        "timestamp": types.TimeStampTz,
        "value": types.DoublePrecision,
    }

    schema_params = PGSchemaParameters(
        schema_name="metrics",
        table_name="samples",
        dtype_map=dtype_map,
        time_index="timestamp",
        primary_keys=["id"],
    )

    print("Schema Parameters:")
    print(schema_params.to_dict())
    print()

    # Access attributes directly
    print(f"Table: {schema_params.schema_name}.{schema_params.table_name}")
    print(f"Time Index: {schema_params.time_index}")
    print(f"Primary Keys: {schema_params.primary_keys}")
    print()

    # Demonstrate the new qualified_name property
    print(f"Qualified Table Name: {schema_params.qualified_name}")
    print()

    # String representation
    print("Schema Parameters String Representation:")
    print(str(schema_params))


if __name__ == "__main__":
    main()
