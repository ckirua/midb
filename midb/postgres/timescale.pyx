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
    
    def select(self, str table_name, schema_name=None, columns=None, where=None, 
               order_by=None, limit=None, offset=None, group_by=None, having=None):
        """
        Generate SQL for SELECT queries.
        
        Args:
            table_name: Table name
            schema_name: Optional schema name
            columns: List of columns to select (None means '*')
            where: WHERE clause condition
            order_by: ORDER BY clause
            limit: LIMIT clause
            offset: OFFSET clause
            group_by: GROUP BY clause
            having: HAVING clause
            
        Returns:
            SQL statement for the SELECT query
        """
        # Build column part
        column_str = "*"
        if columns:
            if isinstance(columns, (list, tuple)):
                column_str = ", ".join(columns)
            else:
                column_str = columns
                
        # Build from part
        from_clause = table_name
        if schema_name:
            from_clause = f"{schema_name}.{table_name}"
            
        # Start with the basic SELECT and FROM parts
        sql_parts = [f"SELECT {column_str} FROM {from_clause}"]
        
        # Add WHERE if provided
        if where:
            sql_parts.append(f"WHERE {where}")
            
        # Add GROUP BY if provided
        if group_by:
            sql_parts.append(f"GROUP BY {group_by}")
            
        # Add HAVING if provided
        if having:
            sql_parts.append(f"HAVING {having}")
            
        # Add ORDER BY if provided
        if order_by:
            sql_parts.append(f"ORDER BY {order_by}")
            
        # Add LIMIT if provided
        if limit:
            sql_parts.append(f"LIMIT {limit}")
            
        # Add OFFSET if provided
        if offset:
            sql_parts.append(f"OFFSET {offset}")
            
        # Join all parts with spaces and add a trailing semicolon
        return " ".join(sql_parts) + ";"
    
    def insert(self, str table_name, values, schema_name=None, returning=None):
        """
        Generate SQL for INSERT queries.
        
        Args:
            table_name: Table name
            values: Dictionary of column-value pairs or list of dictionaries for batch insert
            schema_name: Optional schema name
            returning: Optional RETURNING clause
            
        Returns:
            Tuple of (SQL statement, parameters list)
        """
        import warnings
        
        # Handle table name with schema
        full_table_name = table_name
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
            
        # Check if we have a list of dictionaries (batch insert)
        if isinstance(values, list) and all(isinstance(item, dict) for item in values):
            if not values:
                raise ValueError("Empty values list for batch insert")
            
            # Get columns from the first dictionary
            columns = list(values[0].keys())
            
            # Generate parameter placeholders
            param_count = 1
            value_groups = []
            params = []
            
            for row in values:
                if set(row.keys()) != set(columns):
                    warnings.warn("Inconsistent columns in batch insert values")
                    
                placeholders = []
                for col in columns:
                    placeholders.append(f"${param_count}")
                    params.append(row.get(col))
                    param_count += 1
                    
                value_groups.append(f"({', '.join(placeholders)})")
                
            # Build the SQL
            sql = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES {', '.join(value_groups)}"
            
        else:
            # Single row insert
            if not isinstance(values, dict):
                raise ValueError("Values must be a dictionary for single row insert")
                
            columns = list(values.keys())
            placeholders = [f"${i+1}" for i in range(len(columns))]
            params = [values[col] for col in columns]
            
            # Build the SQL
            sql = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        # Add RETURNING clause if provided
        if returning:
            sql += f" RETURNING {returning}"
            
        # Add trailing semicolon
        sql += ";"
        
        return sql, params
    
    def update(self, str table_name, values, where=None, schema_name=None, returning=None):
        """
        Generate SQL for UPDATE queries.
        
        Args:
            table_name: Table name
            values: Dictionary of column-value pairs to update
            where: Optional WHERE clause condition
            schema_name: Optional schema name
            returning: Optional RETURNING clause
            
        Returns:
            Tuple of (SQL statement, parameters list)
        """
        import warnings
        
        if not where:
            warnings.warn("UPDATE without WHERE clause will update all rows")
            
        # Handle table name with schema
        full_table_name = table_name
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
            
        # Generate SET clause with placeholders
        set_parts = []
        params = []
        
        for i, (col, val) in enumerate(values.items(), 1):
            set_parts.append(f"{col} = ${i}")
            params.append(val)
            
        # Build the SQL
        sql = f"UPDATE {full_table_name} SET {', '.join(set_parts)}"
        
        # Add WHERE clause if provided
        if where:
            sql += f" WHERE {where}"
            
        # Add RETURNING clause if provided
        if returning:
            sql += f" RETURNING {returning}"
            
        # Add trailing semicolon
        sql += ";"
        
        return sql, params
    
    def insert_many(self, str table_name, values, schema_name=None, returning=None):
        """
        Generate SQL for multi-row INSERT queries.
        
        This is a convenience method that handles batch inserts specifically.
        It's functionally equivalent to calling insert() with a list of dictionaries,
        but is provided for API clarity.
        
        Args:
            table_name: Table name
            values: List of dictionaries of column-value pairs
            schema_name: Optional schema name
            returning: Optional RETURNING clause
            
        Returns:
            Tuple of (SQL statement, parameters list)
        """
        if not isinstance(values, list):
            raise ValueError("Values must be a list of dictionaries for insert_many")
            
        # Handle table name with schema
        full_table_name = table_name
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
            
        # Verify we have at least one row
        if not values:
            raise ValueError("Empty values list for batch insert")
            
        # Get columns from the first dictionary
        columns = list(values[0].keys())
        
        # Generate parameter placeholders
        param_count = 1
        value_groups = []
        params = []
        
        for row in values:
            if set(row.keys()) != set(columns):
                import warnings
                warnings.warn("Inconsistent columns in batch insert values")
                
            placeholders = []
            for col in columns:
                placeholders.append(f"${param_count}")
                params.append(row.get(col))
                param_count += 1
                
            value_groups.append(f"({', '.join(placeholders)})")
            
        # Build the SQL
        sql = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES {', '.join(value_groups)}"
        
        # Add RETURNING clause if provided
        if returning:
            sql += f" RETURNING {returning}"
            
        # Add trailing semicolon
        sql += ";"
        
        return sql, params
    
    def delete(self, str table_name, where=None, schema_name=None, returning=None):
        """
        Generate SQL for DELETE queries.
        
        Args:
            table_name: Table name
            where: Optional WHERE clause condition
            schema_name: Optional schema name
            returning: Optional RETURNING clause
            
        Returns:
            SQL statement for the DELETE query
        """
        import warnings
        
        if not where:
            warnings.warn("DELETE without WHERE clause will delete all rows")
            
        # Handle table name with schema
        full_table_name = table_name
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
            
        # Build the SQL
        sql = f"DELETE FROM {full_table_name}"
        
        # Add WHERE clause if provided
        if where:
            sql += f" WHERE {where}"
            
        # Add RETURNING clause if provided
        if returning:
            sql += f" RETURNING {returning}"
            
        # Add trailing semicolon
        sql += ";"
        
        return sql 