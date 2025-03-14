# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native
cimport cython
from cpython.dict cimport PyDict_Contains


class PGConnectionParameters:
    __slots__ = ["_url", "_dict"]

    def __init__(self, host, port, user, password, dbname):
        self._url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self._dict = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "dbname": dbname,
        }

    def to_dict(self):
        return self._dict

    def to_url(self):
        return self._url

@cython.final
@cython.no_gc_clear
cdef class PGSchemaParameters:
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
        if self.time_index is not None and not PyDict_Contains(self.dtype_map, self.time_index):
            raise ValueError(
                f"time_index '{self.time_index}' not found in dtype_map"
            )
        self.primary_keys = primary_keys

    def __dealloc__(self):
        self.schema_name = None
        self.table_name = None
        self.dtype_map = None
        self.time_index = None
        self.primary_keys = None

    def __richcmp__(self, PGSchemaParameters other, int op):
        if other is None:
            if op == 2:  # ==
                return False
            elif op == 3:  # !=
                return True
            return NotImplemented

        if op == 2:  # ==
            return (
                self.schema_name == other.schema_name
                and self.table_name == other.table_name
                and self.dtype_map == other.dtype_map
                and self.time_index == other.time_index
                and self.primary_keys == other.primary_keys
            )
        elif op == 3:  # !=
            return (
                self.schema_name != other.schema_name
                or self.table_name != other.table_name
                or self.dtype_map != other.dtype_map
                or self.time_index != other.time_index
                or self.primary_keys != other.primary_keys
            )
        return NotImplemented


    cpdef inline dict to_dict(self):
        return {
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "dtype_map": self.dtype_map,
            "time_index": self.time_index,
            "primary_keys": self.primary_keys}