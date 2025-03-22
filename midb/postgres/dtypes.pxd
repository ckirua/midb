# Declaration file for dtypes.pyx
# This allows other Cython modules to recognize and use PGTypes

cdef class PGTypes:
    # SQL type constants (PEP-8 style)
    cdef readonly str VARCHAR
    cdef readonly str BIGINT
    cdef readonly str INTEGER
    cdef readonly str REAL
    cdef readonly str DOUBLE_PRECISION
    cdef readonly str TIMESTAMPTZ
    cdef readonly str TIMESTAMP
    cdef readonly str FLOAT
    cdef readonly str JSONB
    cdef readonly str BOOLEAN
    cdef readonly str SERIAL
    
    # Legacy style names (for backward compatibility)
    cdef readonly str VarChar
    cdef readonly str BigInt
    cdef readonly str Integer
    cdef readonly str Real
    cdef readonly str DoublePrecision
    cdef readonly str TimeStampTz
    cdef readonly str TimeStamp
    cdef readonly str Float
    cdef readonly str Jsonb
    cdef readonly str Boolean
    cdef readonly str serial 