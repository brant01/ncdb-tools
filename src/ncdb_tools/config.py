"""Configuration management for NCDB Tools."""

import os
from pathlib import Path
from typing import Optional


def get_data_directory() -> Optional[Path]:
    """Get the NCDB data directory from environment variables.

    Returns:
        Path to NCDB data directory, or None if not configured.

    Environment Variables:
        NCDB_DATA_DIR: Path to directory containing NCDB .dat files or parquet files
    """
    data_dir = os.getenv('NCDB_DATA_DIR')
    if data_dir:
        return Path(data_dir)

    # Check for .env file in current directory
    env_file = Path('.env')
    if env_file.exists():
        try:
            import dotenv
            dotenv.load_dotenv()
            data_dir = os.getenv('NCDB_DATA_DIR')
            if data_dir:
                return Path(data_dir)
        except ImportError:
            pass  # python-dotenv not installed, continue without it

    return None


def get_output_directory() -> Optional[Path]:
    """Get the output directory from environment variables."""
    output_dir = os.getenv('NCDB_OUTPUT_DIR')
    return Path(output_dir) if output_dir else None


def get_memory_limit() -> str:
    """Get memory limit from environment variables."""
    return os.getenv('NCDB_MEMORY_LIMIT', '4GB')


def validate_data_directory(data_dir: Path) -> bool:
    """Validate that a directory contains NCDB data files.

    Args:
        data_dir: Directory to validate

    Returns:
        True if directory contains NCDB files
    """
    if not data_dir.exists():
        return False

    # Look for NCDB file patterns
    dat_files = list(data_dir.glob('*.dat'))
    parquet_files = list(data_dir.glob('*.parquet'))

    if not dat_files and not parquet_files:
        return False

    # Check for NCDB-like filenames
    ncdb_patterns = ['ncdb', 'ncdbpuf', 'puf', 'cancer']
    all_files = dat_files + parquet_files
    has_ncdb_file = any(
        any(pattern in f.name.lower() for pattern in ncdb_patterns)
        for f in all_files
    )

    return has_ncdb_file
