"""Data inspection utilities for NCDB data."""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import polars as pl

logger = logging.getLogger(__name__)


def inspect_parquet_files(parquet_dir: Path) -> Dict[str, Any]:
    """Inspect parquet files to gather metadata and statistics.

    Args:
        parquet_dir: Directory containing parquet files

    Returns:
        Dictionary with inspection results
    """
    parquet_files = [f for f in parquet_dir.glob("*.parquet")]

    if not parquet_files:
        raise ValueError(f"No parquet files found in {parquet_dir}")

    logger.info(f"Inspecting {len(parquet_files)} parquet files")

    # Collect basic information
    file_info = []
    total_rows = 0
    schemas = []

    for pf in parquet_files:
        try:
            # Read parquet metadata without loading data
            df_info = pl.scan_parquet(pf)
            schema = df_info.collect_schema()

            # Get row count efficiently
            row_count = df_info.select(pl.len()).collect().item()

            file_info.append({
                'file': pf.name,
                'rows': row_count,
                'columns': len(schema),
                'size_mb': pf.stat().st_size / (1024 * 1024)
            })

            total_rows += row_count
            schemas.append(schema)

        except Exception as e:
            logger.warning(f"Could not inspect {pf.name}: {e}")

    # Analyze schema consistency
    common_columns = analyze_schema_consistency(schemas)

    return {
        'total_files': len(parquet_files),
        'total_rows': total_rows,
        'file_details': file_info,
        'common_columns': len(common_columns),
        'column_names': list(common_columns),
        'schemas_consistent': len(set(str(s) for s in schemas)) == 1
    }


def analyze_schema_consistency(schemas: List[pl.Schema]) -> Set[str]:
    """Analyze schema consistency across files.

    Args:
        schemas: List of Polars schemas

    Returns:
        Set of column names common to all schemas
    """
    if not schemas:
        return set()

    # Find columns present in all schemas
    common_columns = set(schemas[0].names())
    for schema in schemas[1:]:
        common_columns &= set(schema.names())

    return common_columns


def get_column_statistics(
    parquet_path: Path, columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Get basic statistics for columns in the dataset.

    Args:
        parquet_path: Path to parquet file or directory
        columns: Optional list of columns to analyze (if None, analyzes all)

    Returns:
        Dictionary with column statistics
    """
    if parquet_path.is_file():
        df = pl.scan_parquet(parquet_path)
    else:
        parquet_files = list(parquet_path.glob("*.parquet"))
        df = pl.scan_parquet(parquet_files)

    if columns:
        df = df.select(columns)

    # Get basic statistics
    stats = {}

    # Null counts
    null_counts = df.null_count().collect()
    total_rows = df.select(pl.len()).collect().item()

    for col in null_counts.columns:
        null_count = null_counts[col].item()
        stats[col] = {
            'null_count': null_count,
            'null_percentage': (null_count / total_rows) * 100 if total_rows > 0 else 0,
            'data_type': str(df.collect_schema()[col])
        }

    return {
        'total_rows': total_rows,
        'columns': stats
    }
