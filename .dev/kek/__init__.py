from .dtypes import PGTypes
from .pool import *  # This imports Pool and connection

# Try to import the Cython version, fall back to pure Python if not available
try:
    from .cpool import CPool, cconnection

    __all__ = ["PGTypes", "CPool", "cconnection"]
except ImportError:
    pass
