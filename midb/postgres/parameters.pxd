# Declaration file for parameters.pyx
# Defines PostgreSQL connection and schema parameter types

cdef class PGSchemaParameters:
    """
    Schema parameters for PostgreSQL tables.
    Stores information for table structure, data types, and key columns.
    """
    cdef:
        readonly str schema_name    # Schema containing the table
        readonly str table_name     # Table name
        readonly dict dtype_map     # Map of column names to PostgreSQL types
        readonly str time_index     # Optional time series column name
        readonly list primary_keys  # Optional list of primary key columns

    # Methods
    cpdef dict to_dict(self)
    
    # We can't declare Python properties in pxd files
    # The qualified_name property is defined in the pyx file 