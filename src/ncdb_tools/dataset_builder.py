"""Dataset builder for converting NCDB text files to parquet."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Union

import polars as pl

from ._internal.sas_parser import parse_column_positions, parse_sas_labels
from ._internal.validation import validate_path
from .constants import NCDB_RECORD_LENGTH, PARQUET_EXTENSION

logger = logging.getLogger(__name__)


def build_dataset(
    input_file: Union[str, Path],
    sas_labels_file: Union[str, Path],
    output_file: Optional[Union[str, Path]] = None,
    columns_file: Optional[Union[str, Path]] = None,
    batch_size: int = 10000,
) -> Path:
    """Build parquet dataset from NCDB fixed-width text file.

    Args:
        input_file: Path to input .dat file
        sas_labels_file: Path to SAS labels file (required for column
                         positions and labels)
        output_file: Optional output path (defaults to same name with .parquet)
        columns_file: Optional path to columns.csv file (overrides SAS column positions)
        batch_size: Number of rows to process at once

    Returns:
        Path to created parquet file

    Raises:
        NCDBValidationError: If input paths are invalid
        FileNotFoundError: If required files are not found
    """
    # Validate required inputs
    input_path = validate_path(
        input_file,
        must_exist=True,
        allowed_extensions=[".dat"],
        description="input file",
    )
    sas_path = validate_path(
        sas_labels_file,
        must_exist=True,
        allowed_extensions=[".sas"],
        description="SAS labels file",
    )

    # Determine output path
    if output_file:
        output_path = Path(output_file)
    else:
        output_path = input_path.with_suffix(PARQUET_EXTENSION)

    # Load column definitions
    if columns_file:
        # Use provided columns file
        columns_path = validate_path(
            columns_file,
            must_exist=True,
            allowed_extensions=[".csv"],
            description="columns file",
        )
        column_defs = _load_column_definitions(columns_path)
    else:
        # Parse from SAS file
        column_defs = parse_column_positions(sas_path)
        if not column_defs:
            raise ValueError(
                f"Could not parse column positions from SAS file: {sas_path}"
            )

    # Load labels from SAS file
    _variable_labels, value_formats = parse_sas_labels(sas_path)

    # Extract tumor type from filename
    tumor_type = _extract_tumor_type(input_path.name)

    # Process the file in batches
    batches = []
    with open(input_path, 'r', encoding='latin-1') as f:
        batch_data = []

        for line in f:
            # Skip empty lines
            if not line.strip():
                continue

            # Validate line length
            if len(line.rstrip('\n')) != NCDB_RECORD_LENGTH:
                continue  # Skip invalid lines

            # Parse the line
            row_data = _parse_line(line, column_defs)
            batch_data.append(row_data)

            # Process batch
            if len(batch_data) >= batch_size:
                df_batch = pl.DataFrame(batch_data)
                batches.append(df_batch)
                batch_data = []

        # Process remaining data
        if batch_data:
            df_batch = pl.DataFrame(batch_data)
            batches.append(df_batch)

    # Combine all batches
    df = pl.concat(batches) if batches else pl.DataFrame()

    # Apply data types and transformations
    df = _apply_transformations(df)

    # Apply value labels if available
    if value_formats:
        df = _apply_value_labels(df, value_formats)

    # Add metadata columns
    df = df.with_columns([
        pl.lit(tumor_type).alias("_tumor_type"),
        pl.lit(input_path.name).alias("_source_file"),
    ])

    # Write to parquet
    df.write_parquet(output_path, compression="snappy")

    return output_path


def _load_column_definitions(columns_file: Path) -> List[Dict[str, Union[str, int]]]:
    """Load column definitions from CSV file."""
    df = pl.read_csv(columns_file)

    columns = []
    for row in df.iter_rows(named=True):
        name = row["name"]
        start = int(row["start"]) - 1  # Convert to 0-based index
        end = int(row["end"])  # End position is inclusive

        columns.append({
            "name": name,
            "start": start,
            "end": end,
            "width": end - start
        })

    return columns


def _parse_line(line: str, column_defs: List[Dict]) -> Dict[str, str]:
    """Parse a single line of fixed-width data."""
    row = {}
    for col_def in column_defs:
        value = line[col_def["start"]:col_def["end"]].strip()
        row[col_def["name"]] = value
    return row


def _apply_transformations(df: pl.DataFrame) -> pl.DataFrame:
    """Apply data type conversions and transformations."""
    # Convert numeric fields
    numeric_patterns = [
        "AGE", "YEAR", "DAYS", "SIZE", "NODES", "DOSE", "FRACTION", "MONTHS"
    ]

    for col in df.columns:
        # Skip metadata columns
        if col.startswith("_"):
            continue

        # Check if column should be numeric
        if any(pattern in col.upper() for pattern in numeric_patterns):
            # Try to convert to numeric, keeping nulls
            try:
                df = df.with_columns([
                    pl.col(col).str.strip_chars()
                    .replace("", None)
                    .cast(pl.Int64, strict=False)
                    .alias(col)
                ])
            except Exception as e:
                # If conversion fails, keep as string
                logger.debug("Could not convert column %s to numeric: %s", col, e)

    return df


def _apply_value_labels(
    df: pl.DataFrame, value_formats: Dict[str, Dict[str, str]]
) -> pl.DataFrame:
    """Apply value labels to create descriptive columns."""
    for col, value_map in value_formats.items():
        if col in df.columns:
            # Create a labeled version of the column
            label_col = f"{col}_LABEL"

            # Apply value mappings
            df = df.with_columns([
                pl.col(col).cast(pl.Utf8).replace(value_map).alias(label_col)
            ])

    return df


def _extract_tumor_type(filename: str) -> str:
    """Extract tumor type from filename."""
    match = re.match(r"NCDBPUF_(.+?)\..*\.dat", filename)
    if match:
        return match.group(1)
    return "Unknown"


