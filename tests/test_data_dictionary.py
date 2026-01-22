"""Tests for data dictionary generation."""

import json
from pathlib import Path

from ncdb_tools.data_dictionary import DataDictionaryGenerator, generate_data_dictionary


class TestDataDictionaryGenerator:
    """Test the DataDictionaryGenerator class."""

    def test_init(self):
        """Test generator initialization."""
        gen = DataDictionaryGenerator()
        assert hasattr(gen, 'variable_descriptions')
        assert isinstance(gen.variable_descriptions, dict)

    def test_generate_csv(self, sample_data_dir, temp_output_dir):
        """Test generating CSV data dictionary."""
        gen = DataDictionaryGenerator()

        output_file = temp_output_dir / "test_dict.csv"
        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=output_file,
            formats=["csv"],
            include_stats=False,  # Disable stats for faster testing
            sample_size=100
        )

        assert result_path.exists()
        assert result_path.suffix == ".csv"

        # Read and verify CSV content
        with open(result_path, 'r') as f:
            content = f.read()
            assert "variable" in content
            assert "type" in content
            assert "PUF_CASE_ID" in content

    def test_generate_json(self, sample_data_dir, temp_output_dir):
        """Test generating JSON data dictionary."""
        gen = DataDictionaryGenerator()

        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=temp_output_dir / "test_dict.json",
            formats=["json"],
            include_stats=False,
            sample_size=100
        )

        assert result_path.exists()

        # Read and verify JSON content
        with open(result_path, 'r') as f:
            data = json.load(f)
            assert isinstance(data, list)
            assert len(data) > 0

            # Check first entry structure
            first_entry = data[0]
            required_keys = ["variable", "type", "missing_count", "missing_pct"]
            for key in required_keys:
                assert key in first_entry

    def test_generate_html(self, sample_data_dir, temp_output_dir):
        """Test generating HTML data dictionary."""
        gen = DataDictionaryGenerator()

        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=temp_output_dir / "test_dict.html",
            formats=["html"],
            include_stats=False,
            sample_size=100
        )

        assert result_path.exists()
        assert result_path.suffix == ".html"

        # Read and verify HTML content
        with open(result_path, 'r') as f:
            content = f.read()
            assert "<html>" in content
            assert "<title>NCDB Data Dictionary</title>" in content
            assert "PUF_CASE_ID" in content

    def test_generate_multiple_formats(self, sample_data_dir, temp_output_dir):
        """Test generating multiple formats."""
        gen = DataDictionaryGenerator()

        # Generate all formats
        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=temp_output_dir / "test_dict.csv",
            formats=["csv", "json", "html"],
            include_stats=True,
            sample_size=500
        )

        # Check all files were created
        expected_files = [
            temp_output_dir / "data_dictionary.csv",
            temp_output_dir / "data_dictionary.json",
            temp_output_dir / "data_dictionary.html"
        ]

        for file_path in expected_files:
            assert file_path.exists(), f"Expected file {file_path} was not created"

    def test_statistics_calculation(self, sample_data_dir, temp_output_dir):
        """Test that statistics are properly calculated."""
        gen = DataDictionaryGenerator()

        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=temp_output_dir / "stats_test.json",
            formats=["json"],
            include_stats=True,
            sample_size=1000
        )

        with open(result_path, 'r') as f:
            data = json.load(f)

        # Find a numeric column to test statistics
        numeric_entry = None
        for entry in data:
            if entry['type'] in ['Int64', 'Float64'] and entry.get('min') is not None:
                numeric_entry = entry
                break

        if numeric_entry:
            # Should have numeric statistics
            assert numeric_entry['unique_values'] is not None
            assert numeric_entry['min'] is not None
            assert numeric_entry['max'] is not None

    def test_no_statistics(self, sample_data_dir, temp_output_dir):
        """Test generating dictionary without statistics."""
        gen = DataDictionaryGenerator()

        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=temp_output_dir / "no_stats.json",
            formats=["json"],
            include_stats=False
        )

        with open(result_path, 'r') as f:
            data = json.load(f)

        # Statistics should be None
        for entry in data:
            assert entry['unique_values'] is None
            assert entry['min'] is None
            assert entry['max'] is None


class TestLegacyFunction:
    """Test the legacy generate_data_dictionary function."""

    def test_legacy_function(self, sample_data_dir, temp_output_dir):
        """Test the legacy function interface."""
        result_paths = generate_data_dictionary(
            dataset_path=sample_data_dir,
            output_dir=temp_output_dir,
            formats=["csv", "json"],
            include_stats=True,
            sample_size=500
        )

        assert isinstance(result_paths, dict)
        assert "csv" in result_paths
        assert "json" in result_paths

        for format_name, path in result_paths.items():
            assert Path(path).exists()


class TestDataDictionaryContent:
    """Test the content and structure of generated dictionaries."""

    def test_required_columns_present(self, sample_data_dir, temp_output_dir):
        """Test that all required NCDB columns are documented."""
        gen = DataDictionaryGenerator()

        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=temp_output_dir / "content_test.json",
            formats=["json"],
            include_stats=True,
            sample_size=1000
        )

        with open(result_path, 'r') as f:
            data = json.load(f)

        # Extract variable names
        variable_names = [entry['variable'] for entry in data]

        # Check for key NCDB variables
        expected_vars = ["PUF_CASE_ID", "AGE", "YEAR_OF_DIAGNOSIS"]
        for var in expected_vars:
            assert var in variable_names, f"Expected variable {var} not found"

    def test_missing_data_calculation(self, sample_data_dir, temp_output_dir):
        """Test that missing data percentages are calculated correctly."""
        gen = DataDictionaryGenerator()

        result_path = gen.generate_from_parquet(
            sample_data_dir,
            output_file=temp_output_dir / "missing_test.json",
            formats=["json"],
            include_stats=True,
            sample_size=1000
        )

        with open(result_path, 'r') as f:
            data = json.load(f)

        for entry in data:
            # Missing percentage should be valid
            assert isinstance(entry['missing_pct'], (int, float))
            assert 0 <= entry['missing_pct'] <= 100

            # Missing count should be non-negative integer
            assert isinstance(entry['missing_count'], int)
            assert entry['missing_count'] >= 0
