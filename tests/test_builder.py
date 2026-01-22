"""Tests for the dataset builder functionality."""

import pytest
from pathlib import Path
import tempfile
import shutil

from ncdb_tools import NCDBValidationError
from ncdb_tools.builder import build_parquet_dataset, detect_dataset_type


class TestDatasetDetection:
    """Test dataset type detection."""
    
    def test_detect_ncdb_dataset(self, sample_data_dir):
        """Test detecting NCDB dataset type."""
        dataset_type = detect_dataset_type(sample_data_dir)
        assert dataset_type == "ncdb"
    
    def test_detect_unknown_dataset(self, temp_output_dir):
        """Test detecting unknown dataset type."""
        # Create some non-NCDB files
        (temp_output_dir / "random.txt").touch()
        (temp_output_dir / "data.csv").touch()
        
        dataset_type = detect_dataset_type(temp_output_dir)
        assert dataset_type == "unknown"


class TestBuildParquetDataset:
    """Test the build_parquet_dataset function."""
    
    def test_build_with_existing_parquet(self, sample_data_dir, temp_output_dir):
        """Test building dataset with existing parquet files."""
        result = build_parquet_dataset(
            data_dir=sample_data_dir,
            output_dir=temp_output_dir,
            generate_dictionary=True,
            apply_transforms=False,  # Skip transforms for faster testing
            verify_files=True
        )
        
        # Check return structure
        assert isinstance(result, dict)
        required_keys = ['parquet_dir', 'dictionary', 'log', 'summary']
        for key in required_keys:
            assert key in result
        
        # Check files were created
        assert result['parquet_dir'].exists()
        assert result['log'].exists()
        assert result['summary'].exists()
        
        if result['dictionary']:
            assert result['dictionary'].exists()
    
    def test_build_nonexistent_directory(self):
        """Test building dataset from nonexistent directory."""
        with pytest.raises(NCDBValidationError):
            build_parquet_dataset("/nonexistent/directory")
    
    def test_build_with_custom_memory_limit(self, sample_data_dir, temp_output_dir):
        """Test building with custom memory limit."""
        result = build_parquet_dataset(
            data_dir=sample_data_dir,
            output_dir=temp_output_dir,
            memory_limit="2GB",
            generate_dictionary=False,
            apply_transforms=False
        )
        
        assert result['parquet_dir'].exists()
        assert result['log'].exists()
    
    def test_build_without_dictionary(self, sample_data_dir, temp_output_dir):
        """Test building dataset without generating dictionary."""
        result = build_parquet_dataset(
            data_dir=sample_data_dir,
            output_dir=temp_output_dir,
            generate_dictionary=False,
            apply_transforms=False
        )
        
        assert result['dictionary'] is None
        assert result['parquet_dir'].exists()
    
    def test_build_without_transforms(self, sample_data_dir, temp_output_dir):
        """Test building dataset without applying transformations."""
        result = build_parquet_dataset(
            data_dir=sample_data_dir,
            output_dir=temp_output_dir,
            apply_transforms=False,
            generate_dictionary=False
        )
        
        assert result['parquet_dir'].exists()
        
        # Should have parquet files
        parquet_files = list(result['parquet_dir'].glob("*.parquet"))
        assert len(parquet_files) > 0
    
    def test_build_default_output_dir(self, sample_data_dir):
        """Test building with default output directory."""
        # Use a subdirectory to avoid modifying the main sample data
        test_data_dir = sample_data_dir.parent / "test_build"
        
        try:
            # Copy some sample files
            test_data_dir.mkdir(exist_ok=True)
            sample_files = list(sample_data_dir.glob("*.parquet"))[:2]  # Just copy 2 files
            for f in sample_files:
                shutil.copy2(f, test_data_dir / f.name)
            
            result = build_parquet_dataset(
                data_dir=test_data_dir,
                generate_dictionary=False,
                apply_transforms=False
            )
            
            # Output dir should be created within data_dir
            assert result['parquet_dir'].parent == test_data_dir
            assert result['parquet_dir'].exists()
            
        finally:
            # Cleanup
            if test_data_dir.exists():
                shutil.rmtree(test_data_dir)


class TestBuilderIntegration:
    """Integration tests for builder functionality."""
    
    def test_full_build_pipeline(self, sample_data_dir, temp_output_dir):
        """Test the full build pipeline with all features."""
        result = build_parquet_dataset(
            data_dir=sample_data_dir,
            output_dir=temp_output_dir,
            generate_dictionary=True,
            memory_limit="4GB",
            apply_transforms=True,
            verify_files=True
        )
        
        # Verify all outputs
        assert result['parquet_dir'].exists()
        assert result['dictionary'].exists()
        assert result['log'].exists()
        assert result['summary'].exists()
        
        # Check parquet files exist
        parquet_files = list(result['parquet_dir'].glob("*.parquet"))
        assert len(parquet_files) > 0
        
        # Check dictionary content
        dict_content = result['dictionary'].read_text()
        assert "variable,type" in dict_content  # CSV header
        assert "PUF_CASE_ID" in dict_content
        
        # Check log file has content
        log_content = result['log'].read_text()
        assert "NCDB dataset build" in log_content
        
        # Check summary file
        import json
        summary_data = json.loads(result['summary'].read_text())
        assert "build_timestamp" in summary_data
        assert "data_directory" in summary_data
    
    def test_build_with_verification(self, sample_data_dir, temp_output_dir):
        """Test building with dataset verification."""
        result = build_parquet_dataset(
            data_dir=sample_data_dir,
            output_dir=temp_output_dir,
            verify_files=True,
            generate_dictionary=False,
            apply_transforms=False
        )
        
        # Should complete without errors
        assert result['parquet_dir'].exists()
        
        # Check summary includes verification info
        import json
        summary_data = json.loads(result['summary'].read_text())
        assert "dataset_info" in summary_data
        if summary_data["dataset_info"]:
            assert "total_files" in summary_data["dataset_info"]
            assert "total_rows" in summary_data["dataset_info"]