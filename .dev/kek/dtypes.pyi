class PGTypes:
    VarChar: str
    BigInt: str
    Real: str
    DoublePrecision: str
    TimeStampTz: str

    def __cinit__(self) -> None: ...
    @staticmethod
    def lambdaVarChar(length: int) -> str: ...
