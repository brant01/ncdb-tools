"""
Shared pytest fixtures and configuration for ncdb_tools tests.
"""

import json
from pathlib import Path

import polars as pl
import pytest


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run slow tests"
    )
    parser.addoption(
        "--dataset-dir",
        action="store",
        default="data",
        help="Directory containing the datasets (default: data)"
    )


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests requiring real data"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "requires_data: marks tests that require actual dataset files"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers."""
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # Skip data-dependent tests if data directory doesn't exist
    dataset_dir = Path(config.getoption("--dataset-dir"))
    if not dataset_dir.exists():
        skip_data = pytest.mark.skip(
            reason=f"dataset directory {dataset_dir} not found"
        )
        for item in items:
            if "requires_data" in item.keywords or "integration" in item.keywords:
                item.add_marker(skip_data)


@pytest.fixture(scope="session")
def dataset_dir(request):
    """Get the dataset directory from command line or use default."""
    return Path(request.config.getoption("--dataset-dir"))


@pytest.fixture(scope="session")
def ncdb_parquet_path(dataset_dir):
    """Path to NCDB parquet dataset."""
    return dataset_dir / "ncdb_parquet_20250603"


@pytest.fixture
def sample_data_dir(ncdb_parquet_path):
    """Path to the real NCDB parquet data for testing (backward compat)."""
    if not ncdb_parquet_path.exists():
        pytest.skip(f"Test data not found at {ncdb_parquet_path}")
    return ncdb_parquet_path


@pytest.fixture
def temp_output_dir(tmp_path):
    """Temporary directory for test outputs."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def sample_ncdb_data():
    """Create sample NCDB data for unit tests."""
    return pl.DataFrame({
        "PUF_CASE_ID": ["1", "2", "3", "4", "5"],
        "YEAR_OF_DIAGNOSIS": ["2018", "2019", "2020", "2021", "2022"],
        "AGE": [45, 60, 90, 55, 70],
        "AGE_AS_INT": [45, 60, 90, 55, 70],
        "AGE_IS_90_PLUS": [False, False, True, False, False],
        "SEX": [1, 2, 1, 2, 1],
        "PRIMARY_SITE": ["C50", "C34", "C18", "C61", "C50"],
        "HISTOLOGY": ["8500", "8140", "8480", "8140", "8520"],
        "PUF_VITAL_STATUS": [0, 1, 1, 0, 0],
        "DX_LASTCONTACT_DEATH_MONTHS": [24, 12, 6, 36, 48],
    })


@pytest.fixture
def temp_dataset_dir(tmp_path, sample_ncdb_data):
    """Create a temporary dataset directory with sample data."""
    dataset_dir = tmp_path / "test_dataset"
    dataset_dir.mkdir()

    # Split by year and save
    for year in ["2018", "2019", "2020", "2021", "2022"]:
        year_data = sample_ncdb_data.filter(pl.col("YEAR_OF_DIAGNOSIS") == year)
        if len(year_data) > 0:
            year_data.write_parquet(dataset_dir / f"ncdb_{year}.parquet")

    # Create metadata
    metadata = {
        "dataset_type": "ncdb",
        "created_at": "2025-01-01",
        "transform_version": "1.0.0",
        "years_included": ["2018", "2019", "2020", "2021", "2022"],
        "total_cases": len(sample_ncdb_data)
    }

    with open(dataset_dir / "metadata.json", "w") as f:
        json.dump(metadata, f)

    return dataset_dir


@pytest.fixture
def small_sample_size():
    """Small sample size for faster tests."""
    return 1000


@pytest.fixture
def medium_sample_size():
    """Medium sample size for more comprehensive tests."""
    return 10000


# Validation helpers
@pytest.fixture
def validation_helpers():
    """Helper functions for common validations."""

    class Helpers:
        @staticmethod
        def validate_dataframe_schema(df, required_columns):
            """Validate that a dataframe has required columns."""
            missing = set(required_columns) - set(df.columns)
            assert not missing, f"Missing columns: {missing}"

        @staticmethod
        def validate_year_range(df, min_year, max_year, year_col="YEAR_OF_DIAGNOSIS"):
            """Validate year range in dataset."""
            years = df[year_col].unique().sort().to_list()
            years_int = [int(y) for y in years]
            assert min(years_int) >= min_year, f"Found year before {min_year}"
            assert max(years_int) <= max_year, f"Found year after {max_year}"

        @staticmethod
        def validate_site_codes(df, site_col="PRIMARY_SITE"):
            """Validate primary site codes are in expected format (ICD-O-3)."""
            sites = df[site_col].drop_nulls().to_list()
            for site in sites[:100]:  # Check first 100
                assert site.startswith("C"), f"Invalid site code: {site}"

    return Helpers()
