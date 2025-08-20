"""Transformation functions for NCDB data using Polars."""
import logging
from pathlib import Path
from typing import Dict, List

import polars as pl

from ..constants import NEVER_NUMERIC_COLUMNS

logger = logging.getLogger(__name__)


def apply_transformations(
    parquet_dir: Path, dataset_type: str, memory_limit: str
) -> None:
    """Apply all standard transformations to NCDB parquet files.

    Args:
        parquet_dir: Directory containing parquet files
        dataset_type: Type of dataset (not used for NCDB, kept for compatibility)
        memory_limit: Memory limit for operations (not used in Polars version)
    """
    logger.info(f"Applying transformations to NCDB parquet files in {parquet_dir}")

    # First, determine global schema by examining all files
    parquet_files = [
        f for f in parquet_dir.glob("*.parquet") if f.name != "metadata.json"
    ]
    logger.info("Determining global schema for consistent data types...")
    global_schema = determine_global_schema(parquet_files)
    logger.info(f"Global schema determined for {len(global_schema)} columns")

    # Process each parquet file with consistent schema
    for parquet_file in parquet_files:
        logger.info(f"Transforming {parquet_file.name}")

        # Read the parquet file
        df = pl.read_parquet(parquet_file)

        # Apply transformations
        df = apply_data_type_conversions(df, global_schema)
        df = apply_ncdb_specific_transformations(df)

        # Write back to parquet
        df.write_parquet(parquet_file)

        logger.info(f"Completed transformation of {parquet_file.name}")


def determine_global_schema(parquet_files: List[Path]) -> Dict[str, pl.DataType]:
    """Determine consistent data types across all parquet files.

    Args:
        parquet_files: List of parquet files to analyze

    Returns:
        Dictionary mapping column names to optimal data types
    """
    logger.info("Analyzing schemas across all parquet files...")

    # Collect all schemas
    all_columns = set()
    column_types: Dict[str, List[pl.DataType]] = {}

    for pf in parquet_files:
        df_schema = pl.scan_parquet(pf).collect_schema()
        all_columns.update(df_schema.names())

        for col, dtype in df_schema.items():
            if col not in column_types:
                column_types[col] = []
            column_types[col].append(dtype)

    # Determine optimal type for each column
    optimal_schema: Dict[str, pl.DataType] = {}
    for col in all_columns:
        if col in NEVER_NUMERIC_COLUMNS:
            optimal_schema[col] = pl.Utf8()
        else:
            # Use the most common type, preferring more general types
            types = column_types.get(col, [pl.Utf8()])
            optimal_schema[col] = resolve_column_type(types)

    return optimal_schema


def resolve_column_type(types: List[pl.DataType]) -> pl.DataType:
    """Resolve the best data type from a list of candidate types.

    Args:
        types: List of potential data types for a column

    Returns:
        The most appropriate data type
    """
    # Convert to string representation for easier comparison
    type_strs = [str(t) for t in types]
    unique_types = set(type_strs)

    # If all the same, return that type
    if len(unique_types) == 1:
        return types[0]

    # Preference order: String > Float64 > Int64
    if any('Utf8' in t or 'String' in t for t in type_strs):
        return pl.Utf8()
    elif any('Float64' in t for t in type_strs):
        return pl.Float64()
    elif any('Int64' in t for t in type_strs):
        return pl.Int64()
    else:
        # Default to string for safety
        return pl.Utf8()


def apply_data_type_conversions(
    df: pl.DataFrame, target_schema: Dict[str, pl.DataType]
) -> pl.DataFrame:
    """Apply data type conversions to match target schema.

    Args:
        df: Input DataFrame
        target_schema: Target schema with desired data types

    Returns:
        DataFrame with converted data types
    """
    conversions = []

    current_schema = df.schema

    for col, target_type in target_schema.items():
        if col in current_schema and current_schema[col] != target_type:
            # Skip conversion if column should stay as string
            if col in NEVER_NUMERIC_COLUMNS and target_type != pl.Utf8():
                continue

            conversions.append(pl.col(col).cast(target_type, strict=False))

    if conversions:
        df = df.with_columns(conversions)

    return df


def apply_ncdb_specific_transformations(df: pl.DataFrame) -> pl.DataFrame:
    """Apply NCDB-specific data transformations.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with NCDB-specific transformations applied
    """
    transformations = []

    # Age processing - create numeric version while preserving original
    if "AGE" in df.columns:
        transformations.extend([
            # Create numeric age (convert "90+" to 90)
            pl.when(pl.col("AGE") == "90+")
            .then(90)
            .otherwise(pl.col("AGE").cast(pl.Int64, strict=False))
            .alias("AGE_AS_INT"),

            # Flag for 90+ ages
            pl.when(pl.col("AGE") == "90+")
            .then(True)
            .otherwise(False)
            .alias("AGE_IS_90_PLUS")
        ])

    # Create tumor site groupings based on PRIMARY_SITE
    if "PRIMARY_SITE" in df.columns:
        transformations.append(
            create_site_groups_expr().alias("SITE_GROUP")
        )

    # Create histology groupings
    if "HISTOLOGY" in df.columns:
        transformations.append(
            create_histology_groups_expr().alias("HISTOLOGY_GROUP")
        )

    # Apply all transformations
    if transformations:
        df = df.with_columns(transformations)

    return df


def create_site_groups_expr() -> pl.Expr:
    """Create expression for grouping primary sites into major categories."""
    return (
        pl.when(pl.col("PRIMARY_SITE").str.starts_with("C50"))
        .then("Breast")
        .when(pl.col("PRIMARY_SITE").str.starts_with("C78"))
        .then("Lymph Node")
        .when(pl.col("PRIMARY_SITE").str.starts_with("C77"))
        .then("Lymph Node")
        .when(pl.col("PRIMARY_SITE").str.starts_with("C71"))
        .then("Brain/CNS")
        .when(pl.col("PRIMARY_SITE").str.starts_with("C72"))
        .then("Brain/CNS")
        .when(pl.col("PRIMARY_SITE").str.starts_with("C43"))
        .then("Skin/Melanoma")
        .when(pl.col("PRIMARY_SITE").str.starts_with("C44"))
        .then("Skin/Melanoma")
        .otherwise("Other")
    )


def create_histology_groups_expr() -> pl.Expr:
    """Create expression for grouping histology codes into major categories."""
    return (
        pl.when(pl.col("HISTOLOGY").str.starts_with("814"))
        .then("Adenocarcinoma")
        .when(pl.col("HISTOLOGY").str.starts_with("805"))
        .then("Squamous Cell Carcinoma")
        .when(pl.col("HISTOLOGY").str.starts_with("872"))
        .then("Melanoma")
        .when(pl.col("HISTOLOGY").str.starts_with("959"))
        .then("Lymphoma")
        .when(pl.col("HISTOLOGY").str.starts_with("967"))
        .then("Lymphoma")
        .otherwise("Other")
    )
