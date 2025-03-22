# Declaration file for TimescaleDB schema support - DoD approach

# Simple structure types
ctypedef struct hypertable_params_t:
    const char* table_name
    const char* schema_name
    const char* time_column
    const char* chunk_time_interval
    bint if_not_exists
    bint create_default_indexes
    bint ignore_migration_errors

# Core functions declarations
cdef str generate_create_table_sql(str schema, str table, list columns, list constraints)
cdef str generate_create_hypertable_sql(hypertable_params_t params)
cdef str generate_create_index_sql(str schema, str table, str index_name, list columns, str method, bint unique)
cdef str generate_drop_table_sql(str schema, str table, bint if_exists)

# Helper class for SQL types, leveraging existing PGTypes
cdef class TSDBSql:
    cdef:
        readonly object types  # PGTypes instance
    
    # SQL generation helpers
    cpdef str create_table(self, str schema, str table, list columns, list constraints=*)
    cpdef str create_hypertable(self, str schema, str table, str time_column, str interval=*, bint if_not_exists=*)
    cpdef str create_index(self, str schema, str table, str name, list columns, str method=*, bint unique=*)
    cpdef str drop_table(self, str schema, str table, bint if_exists=*) 