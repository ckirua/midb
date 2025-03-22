# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native
"""
PostgreSQL parameter handling for connections and schema definitions.
Optimized for performance using Cython.
"""
cimport cython
from cpython.dict cimport PyDict_Contains, PyDict_GetItem, PyDict_SetItem
from cpython.ref cimport Py_INCREF


# Regular Python class - can't use cython.freelist here
class PGConnectionParameters:
    """
    Connection parameters for PostgreSQL database.
    Provides both connection URL and dictionary formats.
    
    Optimized with __slots__ for reduced memory footprint.
    """
    __slots__ = ["_url", "_dict"]

    def __init__(self, host, port, user, password, dbname):
        # Format connection string once at initialization time
        self._url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        # Store parameters in dict for individual access
        self._dict = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "dbname": dbname,
        }

    def to_dict(self):
        """Return connection parameters as a dictionary."""
        return self._dict

    def to_url(self):
        """Return connection parameters as a PostgreSQL connection URL."""
        return self._url
    
    def __str__(self):
        """Return a sanitized string representation (no password)."""
        return f"PGConnectionParameters(host={self._dict['host']}, port={self._dict['port']}, user={self._dict['user']}, dbname={self._dict['dbname']})"
    
    def __repr__(self):
        """Return a sanitized string representation (no password)."""
        return self.__str__()


@cython.final
@cython.no_gc_clear
@cython.freelist(8)  # Cache up to 8 instances for reuse - valid for cdef classes
cdef class PGSchemaParameters:
    """
    Schema parameters for PostgreSQL tables.
    Stores table structure information for schema generation and validation.
    
    Optimized with Cython for high performance and memory efficiency.
    """
    def __cinit__(
        self,
        str schema_name,
        str table_name,
        dict dtype_map,
        str time_index = None,
        list primary_keys = None,
    ):
        self.schema_name = schema_name
        self.table_name = table_name
        self.dtype_map = dtype_map
        self.time_index = time_index
        
        # Validate schema_name and table_name
        if not schema_name:
            raise ValueError("schema_name cannot be empty")
        if not table_name:
            raise ValueError("table_name cannot be empty")
        
        # Check if dtype_map is None - explicitly raise ValueError
        if dtype_map is None:
            raise ValueError("dtype_map cannot be None")
        # Validate dtype_map is not empty
        elif not dtype_map:
            raise ValueError("dtype_map cannot be empty")
        
        # Validate time_index against dtype_map
        if self.time_index is not None and not PyDict_Contains(self.dtype_map, self.time_index):
            raise ValueError(
                f"time_index '{self.time_index}' not found in dtype_map"
            )
            
        self.primary_keys = primary_keys

    def __dealloc__(self):
        # Clear references to aid garbage collection
        self.schema_name = None
        self.table_name = None
        self.dtype_map = None
        self.time_index = None
        self.primary_keys = None

    def __richcmp__(self, other, int op):
        """
        Rich comparison implementation for equality testing.
        Supports == and != operators.
        """
        if other is None:
            if op == 2:  # ==
                return False
            elif op == 3:  # !=
                return True
            return NotImplemented
        
        # Check if other is a PGSchemaParameters
        if not isinstance(other, PGSchemaParameters):
            if op == 2:  # ==
                return False
            elif op == 3:  # !=
                return True
            return NotImplemented

        cdef PGSchemaParameters typed_other = <PGSchemaParameters>other
        if op == 2:  # ==
            return (
                self.schema_name == typed_other.schema_name
                and self.table_name == typed_other.table_name
                and self.dtype_map == typed_other.dtype_map
                and self.time_index == typed_other.time_index
                and self.primary_keys == typed_other.primary_keys
            )
        elif op == 3:  # !=
            return (
                self.schema_name != typed_other.schema_name
                or self.table_name != typed_other.table_name
                or self.dtype_map != typed_other.dtype_map
                or self.time_index != typed_other.time_index
                or self.primary_keys != typed_other.primary_keys
            )
        return NotImplemented
    
    def __str__(self):
        """Return a string representation for debugging."""
        return (f"PGSchemaParameters(schema={self.schema_name}, table={self.table_name}, "
                f"cols={len(self.dtype_map)}, time_index={self.time_index})")
    
    def __repr__(self):
        """Return a string representation for debugging."""
        return self.__str__()

    cpdef dict to_dict(self):
        """
        Convert parameters to a dictionary representation.
        
        Returns:
            Dict with schema parameters for serialization or storage.
        """
        # Pre-allocate the dict with known size
        cdef dict result = {}
        
        # Set items directly for better performance
        PyDict_SetItem(result, "schema_name", self.schema_name)
        PyDict_SetItem(result, "table_name", self.table_name)
        PyDict_SetItem(result, "dtype_map", self.dtype_map)
        PyDict_SetItem(result, "time_index", self.time_index)
        PyDict_SetItem(result, "primary_keys", self.primary_keys)
        
        # Increment ref count to avoid premature deallocation
        Py_INCREF(result)
        return result
    
    @property
    def qualified_name(self):
        """Return the fully qualified table name as schema.table."""
        return f"{self.schema_name}.{self.table_name}" 