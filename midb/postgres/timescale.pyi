"""
Type stubs for the TimescaleDB schema support.
"""

from typing import List, Optional

from .dtypes import PGTypes

class TSDBSql:
    """Helper class for generating TimescaleDB SQL statements."""

    types: PGTypes

    def __init__(self) -> None: ...
    def create_table(
        self,
        schema: str,
        table: str,
        columns: List[str],
        constraints: Optional[List[str]] = None,
    ) -> str:
        """
        Generate SQL to create a table.

        Args:
            schema: The schema name
            table: The table name
            columns: List of column definitions
            constraints: Optional list of constraint definitions

        Returns:
            SQL statement for creating the table
        """
        ...

    def create_hypertable(
        self,
        schema: str,
        table: str,
        time_column: str,
        interval: str = "1 day",
        if_not_exists: bool = True,
    ) -> str:
        """
        Generate SQL to convert a table to a TimescaleDB hypertable.

        Args:
            schema: The schema name
            table: The table name
            time_column: The column to use as the time dimension
            interval: Chunk time interval (e.g., "1 hour", "1 day")
            if_not_exists: Whether to use IF NOT EXISTS

        Returns:
            SQL statement for creating the hypertable
        """
        ...

    def create_index(
        self,
        schema: str,
        table: str,
        name: str,
        columns: List[str],
        method: str = "btree",
        unique: bool = False,
    ) -> str:
        """
        Generate SQL to create an index.

        Args:
            schema: The schema name
            table: The table name
            name: The index name
            columns: List of columns to include in the index
            method: Index method (btree, gist, gin, etc.)
            unique: Whether to create a unique index

        Returns:
            SQL statement for creating the index
        """
        ...

    def drop_table(
        self, schema: str, table: str, if_exists: bool = True
    ) -> str:
        """
        Generate SQL to drop a table.

        Args:
            schema: The schema name
            table: The table name
            if_exists: Whether to use IF EXISTS

        Returns:
            SQL statement for dropping the table
        """
        ...

    def create_schema(self, schema: str, if_not_exists: bool = True) -> str:
        """
        Generate SQL to create a schema.

        Args:
            schema: The schema name
            if_not_exists: Whether to use IF NOT EXISTS

        Returns:
            SQL statement for creating the schema
        """
        ...
