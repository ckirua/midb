# midb.postgres

PostgreSQL connector and utilities for the midb package.

## Features

- Connection parameters management
- Schema management
- Data types handling

## Usage

```python
from midb.postgres import PGConnectionParameters, PGSchemaDatamodel, PGTypes

# Create connection parameters
conn_params = PGConnectionParameters(
    host="localhost",
    port=5432,
    user="postgres",
    password="password",
    dbname="mydb"  # Note: parameter name is dbname, not database
)

# Access parameters as a dictionary
params_dict = conn_params.to_dict()
host = params_dict["host"]  # "localhost"

# Get connection URL string
conn_url = conn_params.to_url()  # "postgresql://postgres:password@localhost:5432/mydb"

# Use schema datamodel
schema = PGSchemaDatamodel(...)
```

## Building

This module uses Cython for performance. To build the extension:

```bash
make build
```

Or manually:

```bash
python setup.py build_ext --inplace
``` 