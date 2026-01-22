# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2025-01-22

### Added
- `py.typed` marker for PEP 561 type hint support
- `NCDBValidationError` exception for input validation errors
- Path validation utilities in `_internal/validation.py`
- Sanitized logging to avoid PHI exposure in log files
- CHANGELOG.md following Keep a Changelog format
- Project conventions in `.claude/rules/ncdb-conventions.md`
- Test markers (`@unit`, `@integration`, `@slow`, `@requires_data`)
- Synthetic data fixtures for unit tests

### Changed
- Replace `print()` statements with proper logging in database_builder.py
- Restructure constants.py with `STRING_VARIABLES`, `NEVER_NUMERIC` patterns
- Clean up `__init__.py` exports to match nsqip_tools style
- Add optional `year` parameter to `load_data()` for consistency
- Align project structure with nsqip_tools for cross-project consistency

### Fixed
- Fix ruff target-version to match requires-python (3.10)
- Remove duplicate dependency-groups section from pyproject.toml
- Validate memory_limit parameter before use

## [0.2.0] - 2025-01-03

### Changed
- Major refactor: align architecture with nsqip_tools patterns
- Simplified public API with `build_parquet_dataset()` as main entry point

### Added
- `NCDBQuery` class with fluent query interface
- `load_data()` convenience function for querying parquet datasets
- Memory utilities (`get_memory_info()`, `get_recommended_memory_limit()`)
- Environment variable configuration (`NCDB_DATA_DIR`, `NCDB_OUTPUT_DIR`, `NCDB_MEMORY_LIMIT`)
- Data dictionary generation in multiple formats (CSV, JSON, HTML)

### Fixed
- Version consistency between pyproject.toml and __init__.py

## [0.1.0] - 2025-01-03

### Added
- Initial release
- NCDB fixed-width .dat file to parquet conversion
- SAS label file parsing for column definitions
- Basic query interface with year, primary site, and histology filtering
- Data dictionary generation
- Memory-efficient processing with configurable limits
