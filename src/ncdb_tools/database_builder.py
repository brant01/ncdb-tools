"""High-level database builder for NCDB data."""

import datetime
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl

from ._internal.validation import sanitize_path_for_logging, validate_directory
from .data_dictionary import generate_data_dictionary
from .dataset_builder import build_dataset

logger = logging.getLogger(__name__)


def build_database(
    data_dir: Union[str, Path],
    output_subdir: Optional[str] = None,
    batch_size: int = 10000,
) -> Dict[str, Any]:
    """
    Build complete NCDB database from directory of .dat files.

    This function:
    1. Finds all .dat files in the directory
    2. Locates the SAS labels file
    3. Creates a new subdirectory for output
    4. Converts all .dat files to .parquet
    5. Generates a comprehensive data dictionary

    Args:
        data_dir: Directory containing NCDB .dat files and SAS labels file
        output_subdir: Name for output subdirectory (defaults to ncdb_parquet_YYYYMMDD)
        batch_size: Number of rows to process at once during conversion

    Returns:
        Dictionary with paths to:
        - output_dir: Path to created output directory
        - parquet_files: List of created parquet files
        - data_dictionary_csv: Path to CSV dictionary
        - data_dictionary_json: Path to JSON dictionary
        - data_dictionary_html: Path to HTML dictionary

    Example:
        >>> # Simple usage - just pass the directory
        >>> paths = build_database("/path/to/NCDB_DATA/")
        >>> print(f"Created database in: {paths['output_dir']}")
    """
    data_dir = validate_directory(
        data_dir, must_exist=True, description="data directory"
    )

    # Find all .dat files
    dat_files = list(data_dir.glob("*.dat"))
    if not dat_files:
        raise FileNotFoundError(f"No .dat files found in {data_dir}")

    logger.info("Found %d data files", len(dat_files))

    # Find SAS labels file
    sas_files = list(data_dir.glob("*.sas"))
    if not sas_files:
        raise FileNotFoundError(f"No SAS labels file found in {data_dir}")
    if len(sas_files) > 1:
        # Pick the one with "label" in the name, or the first one
        sas_file = next(
            (f for f in sas_files if "label" in f.name.lower()), sas_files[0]
        )
        logger.info("Multiple SAS files found, using: %s", sas_file.name)
    else:
        sas_file = sas_files[0]

    logger.info("Using SAS labels file: %s", sas_file.name)

    # Create output directory
    if output_subdir is None:
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        output_subdir = f"ncdb_parquet_{date_str}"

    output_dir = data_dir / output_subdir
    output_dir.mkdir(exist_ok=True)
    logger.info("Creating output in: %s", sanitize_path_for_logging(output_dir))

    # Convert each .dat file to parquet and collect summary info
    parquet_files = []
    file_summaries: List[Dict[str, Any]] = []
    total_size_mb = 0.0
    start_time = time.time()

    logger.info("Converting data files...")

    for i, dat_file in enumerate(dat_files, 1):
        original_size_mb = dat_file.stat().st_size / 1024 / 1024
        logger.info(
            "[%d/%d] Processing %s (%.1f MB)",
            i, len(dat_files), dat_file.name, original_size_mb
        )

        try:
            # Build parquet file in output directory
            output_file = output_dir / dat_file.with_suffix(".parquet").name

            parquet_path = build_dataset(
                input_file=dat_file,
                sas_labels_file=sas_file,
                output_file=output_file,
                batch_size=batch_size,
            )

            # Get row count and compressed size
            df = pl.scan_parquet(parquet_path)
            row_count = df.select(pl.count()).collect().item()
            compressed_size_mb = parquet_path.stat().st_size / 1024 / 1024
            total_size_mb += compressed_size_mb

            # Extract tumor type from filename
            tumor_type = dat_file.name.replace(
                "NCDBPUF_", ""
            ).replace(".3.2021.0.dat", "")

            file_summaries.append({
                "tumor_type": tumor_type,
                "rows": row_count,
                "original_size_mb": original_size_mb,
                "compressed_size_mb": compressed_size_mb,
                "filename": parquet_path.name
            })

            parquet_files.append(parquet_path)
            logger.info("Created: %s", parquet_path.name)

        except (OSError, ValueError, pl.exceptions.ComputeError) as e:
            logger.error("Failed to process %s: %s", dat_file.name, e)
            continue

    if not parquet_files:
        raise RuntimeError("No parquet files were created successfully")

    logger.info("Converted %d files successfully", len(parquet_files))

    # Calculate processing time
    processing_time = time.time() - start_time
    processing_time_str = f"{processing_time:.1f} seconds"
    if processing_time > 60:
        processing_time_str = f"{processing_time/60:.1f} minutes"

    # Create dataset summary (for future use in HTML dictionary)
    total_rows = sum(fs["rows"] for fs in file_summaries)
    _dataset_summary = {
        "total_rows": total_rows,
        "file_count": len(file_summaries),
        "compressed_size": total_size_mb,
        "processing_time": processing_time_str,
        "output_directory": str(output_dir),
        "files": sorted(
            file_summaries, key=lambda x: x["rows"], reverse=True
        ),
    }

    # Generate data dictionary from all parquet files
    logger.info("Generating data dictionary...")

    # Use the output directory as the dataset path (it contains all parquet files)
    dict_paths = generate_data_dictionary(
        dataset_path=output_dir,
        output_dir=output_dir,
        formats=["csv", "json", "html"],
        include_stats=True,
        sample_size=10000,
        sas_labels_file=sas_file,
    )

    logger.info("Data dictionary created")

    # Create summary report
    summary_path = output_dir / "conversion_summary.txt"
    with open(summary_path, "w") as f:
        f.write("NCDB Data Conversion Summary\n")
        f.write("============================\n")
        f.write(f"Date: {datetime.datetime.now()}\n")
        f.write(f"Source directory: {data_dir}\n")
        f.write(f"Output directory: {output_dir}\n")
        f.write(f"SAS labels file: {sas_file.name}\n")
        f.write("\nFiles converted:\n")
        for pf in parquet_files:
            f.write(f"  - {pf.name}\n")
        f.write(f"\nTotal files: {len(parquet_files)}\n")

    logger.info("Summary written to: %s", summary_path.name)

    # Return paths
    result = {
        "output_dir": output_dir,
        "parquet_files": parquet_files,
        "data_dictionary_csv": dict_paths.get("csv"),
        "data_dictionary_json": dict_paths.get("json"),
        "data_dictionary_html": dict_paths.get("html"),
        "summary": summary_path,
    }

    logger.info(
        "Database build complete: %d files in %s",
        len(parquet_files), output_dir.name
    )

    return result
