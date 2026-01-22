# NCDB Tools Project Conventions

## Data Processing
- Use Polars LazyFrames for all data operations (enables query optimization)
- Preserve clinical codes as strings (site codes, histology codes, ICD codes)
- Check `constants.py` for variable type definitions before processing

## Column Naming
- Derived columns use SCREAMING_SNAKE_CASE (e.g., `AGE_AS_INT`, `SITE_GROUP`)
- Boolean flags end with `_IS_*` or `_HAS_*`
- Group columns end with `_GROUP`

## Age Handling (HIPAA)
- Original `AGE` column may contain special values for 90+
- Use `AGE_AS_INT` for numeric comparisons
- Use `AGE_IS_90_PLUS` flag for accurate 90+ identification

## Query API Pattern
- `NCDBQuery` returns `self` from filter methods (fluent interface)
- Delegate unknown attributes to underlying LazyFrame
- Use `collect()` only at the end of a query chain

## Input File Handling
- NCDB .dat files are fixed-width format (1032 chars, 338 columns)
- Requires SAS labels file for column definitions
- Use `sas_parser.py` for parsing label definitions

## Testing
- Unit tests must work without real NCDB data
- Use synthetic data fixtures in `conftest.py`
- Mark real data tests with `@pytest.mark.requires_data`

## Memory Safety
- Check `memory_utils.py` for available memory before large operations
- Use streaming mode for datasets larger than available RAM
- Default memory limit is conservative (4GB)
