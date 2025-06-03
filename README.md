# ncdb-tools

A Python package for efficiently processing and analyzing National Cancer Database (NCDB) data files.

## Installation

```bash
pip install ncdb-tools
```

## Important Notice

This package provides tools for processing NCDB data files. **You must obtain NCDB data through official channels** - this package does not include any patient data. The National Cancer Database (NCDB) is a clinical oncology database sourced from hospital registry data that are collected in more than 1,500 Commission on Cancer (CoC)-accredited facilities.

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

After building the database, you can query the parquet files using NCDB-specific filters and standard Polars operations:

```python
import polars as pl

# Load data with NCDB-specific filters
query = ncdb_tools.load_data("path/to/parquet_directory/")

# Chain NCDB filters, then use Polars for analysis
df = (
    query
    .filter_by_year(2021)
    .filter_by_primary_site("C509")  # Breast
    .filter_by_histology([8140, 8500])  # Adenocarcinoma codes
    .drop_missing_vital_status()
    .lazy_frame()  # Get Polars LazyFrame
)

# Use standard Polars operations
results = (
    df
    .filter(pl.col("AGE") >= 50)
    .group_by(["SEX", "RACE"])
    .agg([
        pl.count().alias("count"),
        pl.col("AGE").mean().alias("mean_age")
    ])
    .collect()
)
```

The query interface provides these NCDB-specific filters:
- `filter_by_year()` - Filter by year of diagnosis
- `filter_by_primary_site()` - Filter by ICD-O-3 primary site codes
- `filter_by_histology()` - Filter by histology codes (accepts integers or strings)
- `drop_missing_vital_status()` - Remove cases with missing vital status

After applying NCDB filters, use `.lazy_frame()` to access the Polars LazyFrame for further analysis.

## Features

- Efficiently converts NCDB fixed-width text files to parquet format
- Automatically parses SAS labels for meaningful column names
- Generates comprehensive data dictionaries in CSV, JSON, and HTML formats
- Memory-efficient processing using Polars
- Simple, high-level API for common tasks
- NCDB-specific data filters and transformations
- Compatible with all Python 3.9+ versions

## Requirements

- Python 3.9 or higher
- NCDB data files (obtained through official channels)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Disclaimer

This software is provided for research purposes. Users are responsible for ensuring compliance with all applicable data use agreements and privacy regulations when working with NCDB data.