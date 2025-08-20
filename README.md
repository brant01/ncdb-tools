# NCDB Tools

A Python package for working with National Cancer Database (NCDB) data. This package provides tools to convert NCDB data files into optimized parquet datasets, perform standard data transformations, and query the data efficiently using Polars.

## Features

- **Data Ingestion**: Convert NCDB fixed-width data files (.dat) to parquet format
- **Automatic Transformations**: Standard data cleaning and derived variables for cancer research
- **Data Verification**: Validate datasets against expected NCDB structure
- **Efficient Querying**: Filter by primary site, histology, years, and more
- **Data Dictionary**: Auto-generate comprehensive data dictionaries in CSV, JSON, and HTML formats
- **Memory Efficient**: Designed to work on regular computers with limited RAM
- **Network Drive Compatible**: Works seamlessly on local or network file systems
- **Type Safe**: Comprehensive type hints throughout

## Installation

```bash
pip install ncdb-tools
```

### Optional Dependencies

For enhanced configuration management:
```bash
pip install ncdb-tools[config]
```

## Configuration

### Environment Setup

NCDB Tools can be configured using environment variables for easier data path management:

```bash
# Set your NCDB data directory
export NCDB_DATA_DIR="/path/to/your/ncdb/data"

# Optional: Set custom output directory  
export NCDB_OUTPUT_DIR="/path/to/output"

# Optional: Set memory limit
export NCDB_MEMORY_LIMIT="8GB"
```

### Using .env Files

Create a `.env` file in your project directory:

```bash
# Copy the example configuration
cp .env.example .env

# Edit with your paths
NCDB_DATA_DIR=/path/to/your/ncdb/data
NCDB_OUTPUT_DIR=/path/to/output
NCDB_MEMORY_LIMIT=8GB
```

**Note:** Never commit `.env` files to version control as they may contain sensitive paths.

### Data Access Requirements

**Important:** This package does not include NCDB data. You must obtain access to NCDB data through official channels:

- **NCDB Participant Sites**: Contact your institution's NCDB coordinator
- **Research Access**: Apply through the [American College of Surgeons](https://www.facs.org/quality-programs/cancer-programs/national-cancer-database/)
- **Data Use Agreements**: Required for all NCDB data usage

This tool works with the standard NCDB participant user files (PUF) in fixed-width format (.dat files).

## Quick Start

### Building a Dataset

```python
import ncdb_tools

# Build parquet dataset from NCDB data files
result = ncdb_tools.build_parquet_dataset(
    data_dir="/path/to/ncdb/files",
    generate_dictionary=True
)

print(f"Dataset created at: {result['parquet_dir']}")
print(f"Data dictionary at: {result['dictionary']}")
```

### Querying Data

```python
import ncdb_tools
import polars as pl

# Load and filter data
df = (ncdb_tools.load_data("/path/to/parquet/dataset")
      .filter_by_primary_site(["C509", "C500"])  # Breast sites
      .filter_by_year([2020, 2021])
      .collect())

# Chain with Polars operations
df = (ncdb_tools.load_data("/path/to/parquet/dataset")
      .filter_by_histology([8140, 8500])  # Adenocarcinoma codes
      .lazy_frame  # Access the Polars LazyFrame
      .select(["PUF_CASE_ID", "AGE", "PRIMARY_SITE", "YEAR_OF_DIAGNOSIS"])
      .filter(pl.col("AGE") > 50)
      .group_by("PRIMARY_SITE")
      .agg(pl.count())
      .collect())
```

## API Reference

### Building Datasets

#### `build_parquet_dataset()`

Build an NCDB parquet dataset from data files with standard transformations.

```python
result = ncdb_tools.build_parquet_dataset(
    data_dir,                    # Path to NCDB data files
    output_dir=None,            # Output directory (defaults to data_dir)
    generate_dictionary=True,   # Generate data dictionary
    memory_limit="4GB",         # Memory limit for operations
    apply_transforms=True,      # Apply standard transformations
    verify_files=True           # Verify dataset after creation
)
```

**Returns:** Dictionary with paths to:
- `parquet_dir`: Parquet dataset directory
- `dictionary`: Data dictionary file (if generated)
- `log`: Build log file
- `summary`: Dataset summary file

### Querying Data

#### `load_data()`

Load NCDB data from a parquet dataset for querying.

```python
query = ncdb_tools.load_data("/path/to/parquet/dataset")
```

#### Filter Methods

All filter methods return the query object for chaining:

- **`filter_by_primary_site(sites)`**: Filter by ICD-O-3 primary site codes
- **`filter_by_histology(codes)`**: Filter by histology codes
- **`filter_by_year(years)`**: Filter by diagnosis years
- **`drop_missing_vital_status()`**: Remove cases with missing vital status
- **`filter_active_variables()`**: Keep only variables with data in most recent year
- **`select_demographics()`**: Select common demographic variables
- **`select_outcomes()`**: Select common outcome variables

#### Accessing Results

- **`.lazy_frame`**: Get the Polars LazyFrame for custom operations
- **`.collect()`**: Execute query and return Polars DataFrame
- **`.count()`**: Get count of rows without collecting full data
- **`.sample(n)`**: Get a random sample of n rows
- **`.describe()`**: Get summary statistics about the query

## Standard Transformations

The `build_parquet_dataset()` function automatically applies these transformations:

1. **Data Type Conversion**: Identifies and converts numeric columns while preserving categorical codes
2. **Age Processing**: 
   - Keeps original `AGE` column with "90+" values
   - Creates `AGE_AS_INT` (numeric, with 90 for "90+")
   - Creates `AGE_IS_90_PLUS` boolean flag
3. **Site Groupings**: Creates `SITE_GROUP` based on primary site codes
4. **Histology Groupings**: Creates `HISTOLOGY_GROUP` for major cancer types
5. **Schema Consistency**: Ensures consistent data types across all files

## Data Dictionary

Generated data dictionaries include:

- **Column name and data type**
- **Variable descriptions** (from SAS labels if available)
- **Missing data counts and percentages**
- **Summary statistics** (numeric: min/max/mean/median, categorical: unique values)
- **Interactive HTML format** with color coding and summary statistics

Available formats:
- **CSV**: For Excel/spreadsheet users
- **JSON**: For programmatic access
- **HTML**: For easy web viewing with interactive features

## Memory Optimization

The package is designed for regular computers:

- **Automatic memory detection**: Recommends appropriate memory limits based on available RAM
- **Columnar storage**: Uses parquet format for efficient compression and access
- **Lazy evaluation**: Polars LazyFrames enable efficient query planning
- **Streaming support**: Can process datasets larger than available memory

```python
# Check system memory
mem_info = ncdb_tools.get_memory_info()
print(f"Total RAM: {mem_info['total']}")
print(f"Available: {mem_info['available']}")
print(f"Recommended limit: {mem_info['recommended_limit']}")

# Use automatic memory detection (default)
result = ncdb_tools.build_parquet_dataset(data_dir="/path/to/files")

# Or specify custom limit
result = ncdb_tools.build_parquet_dataset(
    data_dir="/path/to/files",
    memory_limit="8GB"
)
```

### Safe Data Collection

The package includes memory-safe collection to prevent out-of-memory errors:

```python
# Check size before collecting
query = ncdb_tools.load_data("/path/to/parquet/dataset").filter_by_year([2021])
info = query.describe()
print(f"Total rows: {info['total_rows']}")
print(f"Columns: {info['columns']}")

# Get a sample for exploration
sample_df = query.sample(n=10000)
```

## Network Drive Support

The package works seamlessly on network drives and file systems that don't support file locking:

```python
# Works on network drives, SMB shares, etc.
result = ncdb_tools.build_parquet_dataset(
    data_dir="/Volumes/network_drive/ncdb_data",
    output_dir="/Volumes/network_drive/processed"
)

# Query from network location
query = ncdb_tools.load_data("/Volumes/network_drive/processed/ncdb_parquet_20240315")
```

## Cancer-Specific Features

### Primary Site Filtering

```python
# Filter by specific cancer sites
breast_cases = query.filter_by_primary_site(["C509", "C500", "C501"])
lung_cases = query.filter_by_primary_site(["C349", "C780", "C781"])
```

### Histology Filtering

```python
# Filter by histology codes
adenocarcinoma = query.filter_by_histology([8140, 8141, 8142])
squamous_cell = query.filter_by_histology(["8070", "8071", "8072"])
```

### Treatment Analysis

```python
# Analyze treatment patterns
treatment_summary = (query
    .filter_by_primary_site("C509")  # Breast
    .lazy_frame
    .group_by("RX_SUMM_SURG_PRIM_SITE")
    .agg([
        pl.count().alias("count"),
        pl.col("PUF_VITAL_STATUS").value_counts().alias("outcomes")
    ])
    .collect())
```

## Data Requirements

- NCDB data files must be in fixed-width format (.dat files)
- Files should follow standard NCDB naming conventions (NCDBPUF_*.dat)
- SAS labels files are optional but recommended for variable descriptions

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Disclaimer

This package is not affiliated with or endorsed by the American College of Surgeons National Cancer Database. Users must obtain NCDB data through official channels and comply with all applicable data use agreements.