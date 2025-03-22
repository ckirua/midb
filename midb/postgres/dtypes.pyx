# cython: infer_types=True, boundscheck=False, wraparound=False, cdivision=True
# distutils: extra_compile_args=-O2 -march=native
cimport cython


@cython.final
@cython.no_gc_clear
cdef class PGTypes:
    # Define SQL type constants as instance attributes
    def __cinit__(self):
        # SQL type constants (PEP-8 style)
        self.VARCHAR = "VARCHAR"
        self.BIGINT = "BIGINT"
        self.INTEGER = "INTEGER"
        self.REAL = "REAL"
        self.DOUBLE_PRECISION = "DOUBLE PRECISION"
        self.TIMESTAMPTZ = "TIMESTAMPTZ"
        self.TIMESTAMP = "TIMESTAMP"
        self.FLOAT = "FLOAT"
        self.JSONB = "JSONB"
        self.BOOLEAN = "BOOLEAN"
        self.SERIAL = "SERIAL"
        self.DECIMAL = "DECIMAL"
        
        # Legacy style names (for backward compatibility)
        self.VarChar = self.VARCHAR
        self.BigInt = self.BIGINT
        self.Integer = self.INTEGER
        self.Real = self.REAL
        self.DoublePrecision = self.DOUBLE_PRECISION
        self.TimeStampTz = self.TIMESTAMPTZ
        self.TimeStamp = self.TIMESTAMP
        self.Float = self.FLOAT
        self.Jsonb = self.JSONB
        self.Boolean = self.BOOLEAN
        self.serial = self.SERIAL
        self.Decimal = self.DECIMAL

    # Public Python method to create VARCHAR with length
    @staticmethod
    def lambdaVarChar(length: int) -> str:
        return "VARCHAR(" + str(length) + ")"
