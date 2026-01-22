# ncdb-tools

Python package for working with NCDB (National Cancer Database) data.

## Tech Stack
- **Language:** Python 3.10+
- **Data Processing:** Polars, PyArrow
- **Package Management:** uv
- **Testing:** pytest

## Architecture
```
src/ncdb_tools/
├── __init__.py          # Public API exports
├── builder.py           # build_parquet_dataset() - ETL orchestration
├── query.py             # NCDBQuery class - fluent query API
├── config.py            # Environment variable management
├── constants.py         # Variable types, column groupings
├── data_dictionary.py   # DataDictionaryGenerator class
└── _internal/
    ├── ingest.py        # Fixed-width .dat file parsing
    ├── sas_parser.py    # SAS label file parsing
    ├── transform.py     # Data transformations (age, site groups, etc.)
    ├── inspect.py       # File inspection utilities
    ├── validation.py    # Input validation utilities
    └── memory_utils.py  # System memory detection
```

## Quick Commands
```bash
# Install dependencies
uv sync

# Run tests (unit only - no real data needed)
uv run pytest -m unit

# Run all tests (requires NCDB data configured)
uv run pytest

# Build package
uv build
```

## Key Patterns

### Data Flow
1. Fixed-width .dat files → `ingest.py` + `sas_parser.py` → raw parquet
2. Raw parquet → `transform.py` → transformed parquet with derived columns
3. Transformed parquet → `NCDBQuery` → filtered results

### Important Variables
- `NEVER_NUMERIC_COLUMNS` in `constants.py` - columns that must stay as strings
- `PRIMARY_SITE_COLUMN`, `HISTOLOGY_COLUMN` - cancer classification columns
- Derived: `AGE_AS_INT`, `AGE_IS_90_PLUS`, `SITE_GROUP`, `HISTOLOGY_GROUP`

### Environment Configuration
```bash
NCDB_DATA_DIR=/path/to/ncdb/data
NCDB_OUTPUT_DIR=/path/to/output
NCDB_MEMORY_LIMIT=8GB
```

## Testing
- Unit tests use synthetic data (no real NCDB files needed)
- Integration tests require real data paths in environment
- Markers: `@unit`, `@integration`, `@slow`, `@requires_data`

## Data Notes
- NCDB .dat files are fixed-width (1032 chars, 338 columns)
- Requires SAS labels file for column definitions
- All data files are gitignored (PHI considerations)
