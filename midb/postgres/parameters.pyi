"""
Type stubs for PostgreSQL parameter handling classes.
"""

from typing import Any, Dict, List, Optional, Union

class PGConnectionParameters:
    """
    Connection parameters for PostgreSQL database.
    Provides both connection URL and dictionary formats.
    """

    def __init__(
        self,
        host: str,
        port: Union[int, str],
        user: str,
        password: str,
        dbname: str,
    ) -> None: ...
    def to_dict(self) -> Dict[str, Any]:
        """Return connection parameters as a dictionary."""
        ...

    def to_url(self) -> str:
        """Return connection parameters as a PostgreSQL connection URL."""
        ...

    def __str__(self) -> str:
        """Return a sanitized string representation (no password)."""
        ...

    def __repr__(self) -> str:
        """Return a sanitized string representation (no password)."""
        ...

class PGSchemaParameters:
    """
    Schema parameters for PostgreSQL tables.
    Stores table structure information for schema generation and validation.
    """

    schema_name: str
    table_name: str
    dtype_map: Dict[str, str]
    time_index: Optional[str]
    primary_keys: Optional[List[str]]

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        dtype_map: Dict[str, str],
        time_index: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
    ) -> None: ...
    def to_dict(self) -> Dict[str, Any]:
        """Convert parameters to a dictionary representation."""
        ...

    @property
    def qualified_name(self) -> str:
        """Return the fully qualified table name as schema.table."""
        ...

    def __str__(self) -> str:
        """Return a string representation for debugging."""
        ...

    def __repr__(self) -> str:
        """Return a string representation for debugging."""
        ...
