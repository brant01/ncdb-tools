# ncdb-tools

Tools for processing and analyzing National Cancer Database (NCDB) data.

## Tech Stack
- **Python** 3.10+ with strict type hints
- **Polars** for dataframes (not pandas)
- **PyArrow** for parquet format
- **pytest** for testing
- **ruff** for linting, **mypy** for type checking

## Architecture
```
src/ncdb_tools/
├── builder.py          # Primary API: build_parquet_dataset()
├── query.py            # NCDBQuery fluent API for filtering
├── data_dictionary.py  # Data dictionary generation
├── config.py           # Environment/config management
├── constants.py        # NCDB-specific constants
└── _internal/          # Internal utilities
    ├── ingest.py       # Data file reading/parsing
    ├── sas_parser.py   # SAS label file parsing
    ├── transform.py    # Data transformations
    ├── inspect.py      # Parquet inspection
    └── memory_utils.py # Memory management
```

## Quick Commands
```bash
# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=ncdb_tools

# Type checking
uv run mypy src/

# Linting
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

## Key Concepts

### Primary API
```python
from ncdb_tools import build_parquet_dataset, load_data

# Convert .dat files to parquet
result = build_parquet_dataset(data_dir="path/to/ncdb/data")

# Query the data
query = load_data(result["parquet_dir"])
query.filter_by_year(2020).filter_by_primary_site("C50").collect()
```

### Environment Variables
- `NCDB_DATA_DIR` - Path to raw NCDB .dat files
- `NCDB_OUTPUT_DIR` - Output directory for parquet files
- `NCDB_MEMORY_LIMIT` - Memory limit (e.g., "4GB")

### Data Notes
- NCDB .dat files are fixed-width (1032 chars, 338 columns)
- All data files are gitignored (PHI considerations)
- Use `NEVER_NUMERIC_COLUMNS` constant for columns that must stay categorical
