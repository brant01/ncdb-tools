"""Test configuration and fixtures for NCDB Tools."""

import pytest
from pathlib import Path
import tempfile
import shutil


@pytest.fixture
def sample_data_dir():
    """Path to the real NCDB parquet data for testing."""
    data_dir = Path(__file__).parent.parent / "data" / "ncdb_parquet_20250603"
    if not data_dir.exists():
        pytest.skip(f"Test data not found at {data_dir}")
    return data_dir


@pytest.fixture
def temp_output_dir():
    """Temporary directory for test outputs."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    # Cleanup
    if temp_dir.exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def small_sample_size():
    """Small sample size for faster tests."""
    return 1000


@pytest.fixture
def medium_sample_size():
    """Medium sample size for more comprehensive tests."""
    return 10000