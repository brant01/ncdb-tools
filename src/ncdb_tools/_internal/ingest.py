"""Data ingestion functions for NCDB data using Polars."""
import logging
from pathlib import Path
from typing import List

import polars as pl
from tqdm import tqdm

logger = logging.getLogger(__name__)


def create_parquet_from_text(
    text_files: List[Path],
    output_dir: Path,
    memory_limit: str = "4GB"
) -> None:
    """Convert NCDB text files to parquet format.

    Args:
        text_files: List of paths to NCDB .dat files
        output_dir: Directory to write parquet files
        memory_limit: Memory limit for operations (not used in Polars)
    """
    logger.info(f"Converting {len(text_files)} NCDB files to parquet format")

    output_dir.mkdir(parents=True, exist_ok=True)

    for text_file in tqdm(text_files, desc="Converting files"):
        logger.info(f"Processing {text_file.name}")

        # Read NCDB fixed-width file
        # Note: This assumes the SAS parser is already implemented in the existing code
        try:
            df = read_ncdb_file(text_file)

            # Write to parquet
            output_file = output_dir / f"{text_file.stem}.parquet"
            df.write_parquet(output_file)

            logger.info(f"Created {output_file.name} with {df.height:,} rows")

        except Exception as e:
            logger.error(f"Failed to process {text_file.name}: {e}")
            raise


def read_ncdb_file(file_path: Path) -> pl.DataFrame:
    """Read a single NCDB data file.

    This is a placeholder that should integrate with the existing
    SAS parser and database builder functionality.

    Args:
        file_path: Path to NCDB .dat file

    Returns:
        Polars DataFrame with the data
    """
    # This should use the existing SAS parser and database builder
    # For now, this is a placeholder that delegates to existing code

    # Use existing functionality to read the file
    # This needs to be adapted to work with the new architecture
    raise NotImplementedError(
        "This function needs to integrate with existing SAS parser"
    )
