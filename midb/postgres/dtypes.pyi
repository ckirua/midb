class PGTypes:
    # SQL type constants (PEP-8 style)
    VARCHAR: str
    BIGINT: str
    INTEGER: str
    REAL: str
    DOUBLE_PRECISION: str
    TIMESTAMPTZ: str
    TIMESTAMP: str
    FLOAT: str
    JSONB: str
    BOOLEAN: str

    def __cinit__(self) -> None: ...
    @staticmethod
    def lambdaVarChar(length: int) -> str: ...
