"""
Utility script to check available attributes in the PGTypes class.
"""

import sys

try:
    from midb.postgres import PGTypes

    print("Successfully imported PGTypes")
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)


def main():
    """Print all attributes of the PGTypes class."""
    types = PGTypes()

    print("\nAll attributes in PGTypes:")
    for attr_name in dir(types):
        # Skip private attributes
        if not attr_name.startswith("_"):
            attr_value = getattr(types, attr_name)
            print(f"- {attr_name}: {attr_value}")

    print("\nRecommended usage for common types:")
    # Check for common PostgreSQL types with different casing
    for possible_name in [
        "serial",
        "SERIAL",
        "Serial",
        "varchar",
        "VARCHAR",
        "VarChar",
        "timestamptz",
        "TIMESTAMPTZ",
        "TimeStampTz",
    ]:
        try:
            value = getattr(types, possible_name)
            print(f"✅ types.{possible_name}: {value}")
        except AttributeError:
            print(f"❌ types.{possible_name}: Not available")


if __name__ == "__main__":
    main()
