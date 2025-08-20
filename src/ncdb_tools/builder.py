"""Main API for building NCDB parquet datasets.

This module provides the high-level interface for converting NCDB data files
into optimized parquet datasets with standard transformations.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

from ._internal.inspect import inspect_parquet_files
from ._internal.memory_utils import get_recommended_memory_limit, warn_if_low_memory
from ._internal.transform import apply_transformations
from .data_dictionary import DataDictionaryGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def build_parquet_dataset(
    data_dir: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    generate_dictionary: bool = True,
    memory_limit: Optional[str] = None,
    apply_transforms: bool = True,
    verify_files: bool = True,
) -> Dict[str, Optional[Path]]:
    """Build an NCDB parquet dataset from data files with standard transformations.

    This function performs the complete pipeline of ingesting NCDB data files,
    applying standard transformations, and optionally generating a data dictionary.
    All original data is preserved - transformations only add new columns.

    Args:
        data_dir: Directory containing NCDB data files (.dat files or existing parquet).
        output_dir: Directory for output files. If None, creates a parquet subdirectory
                   within data_dir (e.g., data_dir/ncdb_parquet_YYYYMMDD/).
        generate_dictionary: Whether to generate a comprehensive data dictionary.
        memory_limit: Memory limit for operations (e.g., "4GB", "8GB"). If None,
                     automatically detects based on available system memory.
        apply_transforms: Whether to apply standard NCDB transformations.
        verify_files: Whether to verify the dataset after creation.

    Returns:
        Dictionary with paths to created files:
        - 'parquet_dir': Path to parquet dataset directory
        - 'dictionary': Path to data dictionary file (if generated)
        - 'log': Path to build log file
        - 'summary': Path to dataset summary file

    Raises:
        FileNotFoundError: If data_dir doesn't exist or contains no NCDB files
        ValueError: If memory_limit format is invalid

    Examples:
        >>> # Build dataset with default settings
        >>> result = build_parquet_dataset("/path/to/ncdb/data")
        >>> print(f"Dataset created: {result['parquet_dir']}")

        >>> # Custom output location with more memory
        >>> result = build_parquet_dataset(
        ...     data_dir="/path/to/ncdb/data",
        ...     output_dir="/path/to/output",
        ...     memory_limit="8GB"
        ... )
    """
    data_dir = Path(data_dir)

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory does not exist: {data_dir}")

    # Set up memory limit
    if memory_limit is None:
        memory_limit = get_recommended_memory_limit()
        logger.info(f"Using recommended memory limit: {memory_limit}")

    warn_if_low_memory("NCDB dataset building")

    # Set up output directory
    if output_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d")
        output_dir = data_dir / f"ncdb_parquet_{timestamp}"
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Set up logging
    log_file = output_dir / "build.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(file_handler)

    logger.info("Starting NCDB dataset build")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Memory limit: {memory_limit}")

    try:
        # Check if we have existing parquet files or need to convert from data files
        parquet_files = list(data_dir.glob("*.parquet"))
        data_files = list(data_dir.glob("*.dat"))

        if parquet_files and not data_files:
            # Working with existing parquet files
            logger.info(f"Found {len(parquet_files)} existing parquet files")

            # Copy parquet files to output directory if different
            if data_dir != output_dir:
                for pf in parquet_files:
                    import shutil
                    shutil.copy2(pf, output_dir / pf.name)

        elif data_files:
            # Convert data files to parquet
            logger.info(f"Found {len(data_files)} NCDB data files")

            # For now, this delegates to existing functionality
            # This needs to be properly integrated with the new architecture
            logger.info("Converting data files to parquet format...")
            from .database_builder import build_database

            # Use existing build_database function
            result = build_database(str(data_dir))
            existing_parquet_dir = Path(result['output_dir'])

            # Copy files to our output directory if different
            if existing_parquet_dir != output_dir:
                import shutil
                for item in existing_parquet_dir.iterdir():
                    if item.is_file():
                        shutil.copy2(item, output_dir / item.name)

        else:
            raise FileNotFoundError(
                f"No NCDB data files (.dat) or parquet files found in {data_dir}"
            )

        # Apply transformations if requested
        if apply_transforms:
            logger.info("Applying standard NCDB transformations...")
            apply_transformations(output_dir, "ncdb", memory_limit)

        # Verify the dataset
        dataset_info = None
        if verify_files:
            logger.info("Verifying dataset...")
            dataset_info = inspect_parquet_files(output_dir)
            logger.info(f"Dataset verification: {dataset_info['total_files']} files, "
                       f"{dataset_info['total_rows']:,} total rows")

        # Generate data dictionary
        dictionary_path = None
        if generate_dictionary:
            logger.info("Generating data dictionary...")
            dict_gen = DataDictionaryGenerator()
            dictionary_path = dict_gen.generate_from_parquet(
                output_dir,
                output_file=output_dir / "data_dictionary.csv"
            )
            logger.info(f"Data dictionary saved to: {dictionary_path}")

        # Create summary file
        summary_path = output_dir / "dataset_summary.json"
        create_summary_file(summary_path, data_dir, output_dir, dataset_info)

        logger.info("NCDB dataset build completed successfully")

        result_dict: Dict[str, Optional[Path]] = {
            'parquet_dir': output_dir,
            'dictionary': dictionary_path,
            'log': log_file,
            'summary': summary_path
        }
        return result_dict

    except Exception as e:
        logger.error(f"Failed to build NCDB dataset: {e}")
        raise

    finally:
        # Remove file handler to avoid duplicate logs
        logger.removeHandler(file_handler)


def create_summary_file(
    summary_path: Path,
    data_dir: Path,
    output_dir: Path,
    dataset_info: Optional[Dict]
) -> None:
    """Create a summary file with dataset information.

    Args:
        summary_path: Path where to save the summary
        data_dir: Original data directory
        output_dir: Output directory
        dataset_info: Dataset inspection information
    """
    import json

    summary = {
        'build_timestamp': datetime.now().isoformat(),
        'data_directory': str(data_dir),
        'output_directory': str(output_dir),
        'dataset_info': dataset_info
    }

    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)


def detect_dataset_type(data_dir: Path) -> str:
    """Detect the type of NCDB dataset based on files present.

    Args:
        data_dir: Directory to analyze

    Returns:
        Dataset type identifier
    """
    files = list(data_dir.glob("*"))
    file_names = [f.name.lower() for f in files]

    # Look for NCDB-specific patterns
    ncdb_patterns = ['ncdbpuf', 'ncdb_puf', 'cancer']

    for pattern in ncdb_patterns:
        if any(pattern in name for name in file_names):
            return "ncdb"

    return "unknown"
