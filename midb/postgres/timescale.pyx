# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native
"""
Data-oriented design (DoD) implementation for TimescaleDB schema support.
Provides efficient functions for generating TimescaleDB SQL statements.
"""
cimport cython
from libc.string cimport strcmp
from .dtypes cimport PGTypes


# Core SQL generation functions
cdef str generate_create_table_sql(str schema, str table, list columns, list constraints):
    """Generate SQL to create a table with the specified columns and constraints."""
    # Pre-allocate capacity for better string concatenation performance
    cdef list parts = []
    parts.append(f"CREATE TABLE {schema}.{table} (")
    
    # Join columns and constraints
    cdef list all_defs = []
    all_defs.extend(columns)
    
    if constraints:
        all_defs.extend(constraints)
    
    # Add indented definitions
    parts.append(",\n".join(["    " + part for part in all_defs]))
    parts.append(");")
    
    # Join all parts with newlines for better formatting
    return "\n".join(parts)


cdef str generate_create_hypertable_sql(hypertable_params_t params):
    """Generate SQL to convert a table to a hypertable with TimescaleDB."""
    # Convert bytes to strings once for better performance
    cdef str schema_name = params.schema_name.decode('utf-8')
    cdef str table_name = params.table_name.decode('utf-8')
    cdef str time_column = params.time_column.decode('utf-8')
    cdef str chunk_time_interval = params.chunk_time_interval.decode('utf-8')
    
    # Use list joining for better performance than string concatenation
    cdef list parts = []
    parts.append(f"SELECT create_hypertable(")
    parts.append(f"    '{schema_name}.{table_name}',")
    parts.append(f"    '{time_column}',")
    parts.append(f"    chunk_time_interval => interval '{chunk_time_interval}',")
    parts.append(f"    if_not_exists => {'TRUE' if params.if_not_exists else 'FALSE'},")
    parts.append(f"    create_default_indexes => {'TRUE' if params.create_default_indexes else 'FALSE'},")
    parts.append(f"    migrate_data => TRUE,")
    parts.append(f"    ignore_migration_errors => {'TRUE' if params.ignore_migration_errors else 'FALSE'}")
    parts.append(");")
    
    return "\n".join(parts)


cdef str generate_create_index_sql(str schema, str table, str index_name, list columns, str method, bint unique):
    """Generate SQL to create an index on the specified columns."""
    cdef list parts = []
    
    if unique:
        parts.append("CREATE UNIQUE INDEX")
    else:
        parts.append("CREATE INDEX")
    
    parts.append(f"{index_name} ON {schema}.{table}")
    
    if method != "btree":
        parts.append(f"USING {method}")
        
    parts.append("(" + ", ".join(columns) + ");")
    
    return " ".join(parts)


cdef str generate_drop_table_sql(str schema, str table, bint if_exists):
    """Generate SQL to drop a table with CASCADE option."""
    cdef str if_exists_clause = "IF EXISTS " if if_exists else ""
    return f"DROP TABLE {if_exists_clause}{schema}.{table} CASCADE;"


@cython.final
@cython.no_gc_clear
cdef class TSDBSql:
    """
    Helper class for generating TimescaleDB SQL statements.
    
    This class provides a clean interface for generating SQL statements
    for TimescaleDB operations like creating tables, hypertables, and indexes.
    It uses a data-oriented design for maximum performance.
    """
    
    def __cinit__(self):
        self.types = PGTypes()
    
    def __dealloc__(self):
        self.types = None
    
    cpdef str create_table(self, str schema, str table, list columns, list constraints=None):
        """
        Generate SQL to create a table.
        
        Args:
            schema: Schema name
            table: Table name
            columns: List of column definitions
            constraints: Optional list of table constraints
            
        Returns:
            SQL statement for table creation
        """
        return generate_create_table_sql(schema, table, columns, constraints or [])
    
    cpdef str create_hypertable(self, str schema, str table, str time_column, str interval="1 day", bint if_not_exists=True):
        """
        Generate SQL to convert a table to a hypertable.
        
        Args:
            schema: Schema name
            table: Table name
            time_column: Column to use as time dimension
            interval: Chunk time interval (e.g., "1 hour", "1 day")
            if_not_exists: Whether to use IF NOT EXISTS
            
        Returns:
            SQL statement for hypertable creation
        """
        # Store encoded strings in variables before assigning to struct
        cdef bytes schema_bytes = schema.encode('utf-8')
        cdef bytes table_bytes = table.encode('utf-8')
        cdef bytes time_column_bytes = time_column.encode('utf-8')
        cdef bytes interval_bytes = interval.encode('utf-8')
        
        cdef hypertable_params_t params
        params.schema_name = schema_bytes
        params.table_name = table_bytes
        params.time_column = time_column_bytes
        params.chunk_time_interval = interval_bytes
        params.if_not_exists = if_not_exists
        params.create_default_indexes = True
        params.ignore_migration_errors = False
        
        return generate_create_hypertable_sql(params)
    
    cpdef str create_index(self, str schema, str table, str name, list columns, str method="btree", bint unique=False):
        """
        Generate SQL to create an index.
        
        Args:
            schema: Schema name
            table: Table name
            name: Index name
            columns: List of columns to include in the index
            method: Index method (default: btree)
            unique: Whether to create a unique index
            
        Returns:
            SQL statement for index creation
        """
        return generate_create_index_sql(schema, table, name, columns, method, unique)
    
    cpdef str drop_table(self, str schema, str table, bint if_exists=True):
        """
        Generate SQL to drop a table.
        
        Args:
            schema: Schema name
            table: Table name
            if_exists: Whether to use IF EXISTS
            
        Returns:
            SQL statement for dropping the table
        """
        return generate_drop_table_sql(schema, table, if_exists)
        
    def create_schema(self, str schema, bint if_not_exists=True):
        """
        Generate SQL to create a schema.
        
        Args:
            schema: Schema name
            if_not_exists: Whether to use IF NOT EXISTS
            
        Returns:
            SQL statement for schema creation
        """
        return f"CREATE SCHEMA {'IF NOT EXISTS ' if if_not_exists else ''}{schema};" 