"""
midb - Database interface library
"""

# Import submodules
from . import postgres
from .__about__ import __version__

# Export public API
__all__ = [
    "postgres",
    "__version__",
]
