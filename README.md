# ncdb-tools

Tools for managing and analyzing National Cancer Database (NCDB) data files.

## Installation

```bash
pip install ncdb-tools
```

## Quick Start

```python
import ncdb_tools

# Convert all NCDB data files in a directory to parquet format
paths = ncdb_tools.build_database("/path/to/NCDB_DATA/")

# The function will:
# 1. Find all .dat files
# 2. Find the SAS labels file
# 3. Create a new subdirectory with today's date
# 4. Convert all files to parquet format
# 5. Generate a comprehensive data dictionary
# 6. Create a summary report

print(f"Database created in: {paths['output_dir']}")
```

## Working with the Data

After building the database, you can query the parquet files:

```python
# Load data from a parquet file
query = ncdb_tools.load_data("path/to/data.parquet")

# Filter and analyze
results = (
    query
    .filter_by_year(2019)
    .filter_by_primary_site("C509")  # Breast
    .select_demographics()
    .collect()
)
```

## Features

- Efficiently converts NCDB fixed-width text files to parquet format
- Automatically parses SAS labels for meaningful column names
- Generates comprehensive data dictionaries in CSV, JSON, and HTML formats
- Memory-efficient processing using Polars
- Simple, high-level API for common tasks