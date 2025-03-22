# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native
cimport cython


@cython.final
@cython.no_gc_clear
cdef class PGTypes:
    # Define as readonly C constants for better performance
    cdef readonly str VarChar
    cdef readonly str BigInt
    cdef readonly str Real
    cdef readonly str DoublePrecision
    cdef readonly str TimeStampTz

    def __cinit__(self):
        self.VarChar = "VARCHAR"
        self.BigInt = "BIGINT"
        self.Real = "REAL"
        self.DoublePrecision = "DOUBLE PRECISION"
        self.TimeStampTz = "TIMESTAMPTZ"

    @staticmethod
    @cython.returns(str)
    def lambdaVarChar(length: cython.int) -> str:
        return "VARCHAR(" + str(length) + ")"
