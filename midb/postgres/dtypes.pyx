# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native
cimport cython


@cython.final
@cython.no_gc_clear
cdef class PGTypes:
    # Define SQL type constants
    def __cinit__(self):
        self.VarChar = "VARCHAR"
        self.BigInt = "BIGINT"
        self.Real = "REAL"
        self.DoublePrecision = "DOUBLE PRECISION"
        self.TimeStampTz = "TIMESTAMPTZ"

    # Public Python method to create VARCHAR with length
    @staticmethod
    def lambdaVarChar(length: int) -> str:
        return "VARCHAR(" + str(length) + ")"
