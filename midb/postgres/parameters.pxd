# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native

cdef class PGSchemaParameters:
    cdef:
        readonly str schema_name
        readonly str table_name
        readonly dict dtype_map
        readonly str time_index
        readonly list primary_keys

    cpdef dict to_dict(self)
