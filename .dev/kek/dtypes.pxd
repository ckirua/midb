# Declaration file for dtypes.pyx
# This allows other Cython modules to recognize and use PGTypes

cdef class PGTypes:
    cdef readonly str VarChar
    cdef readonly str BigInt
    cdef readonly str Real
    cdef readonly str DoublePrecision
    cdef readonly str TimeStampTz

    @staticmethod
    cpdef str lambdaVarChar(int length) 