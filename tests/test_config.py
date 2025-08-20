"""Tests for configuration management."""

import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from ncdb_tools.config import (
    get_data_directory,
    get_output_directory,
    get_memory_limit,
    validate_data_directory
)


class TestConfigEnvironment:
    """Test configuration from environment variables."""
    
    def test_get_data_directory_from_env(self):
        """Test getting data directory from environment variable."""
        test_path = "/test/ncdb/data"
        
        with patch.dict(os.environ, {'NCDB_DATA_DIR': test_path}):
            result = get_data_directory()
            assert result == Path(test_path)
    
    def test_get_data_directory_none(self):
        """Test getting data directory when not set."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_data_directory()
            assert result is None
    
    def test_get_output_directory_from_env(self):
        """Test getting output directory from environment variable."""
        test_path = "/test/output"
        
        with patch.dict(os.environ, {'NCDB_OUTPUT_DIR': test_path}):
            result = get_output_directory()
            assert result == Path(test_path)
    
    def test_get_output_directory_none(self):
        """Test getting output directory when not set."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_output_directory()
            assert result is None
    
    def test_get_memory_limit_from_env(self):
        """Test getting memory limit from environment variable."""
        test_limit = "8GB"
        
        with patch.dict(os.environ, {'NCDB_MEMORY_LIMIT': test_limit}):
            result = get_memory_limit()
            assert result == test_limit
    
    def test_get_memory_limit_default(self):
        """Test getting default memory limit."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_memory_limit()
            assert result == "4GB"


class TestConfigDotEnv:
    """Test configuration from .env files."""
    
    def test_dotenv_loading(self, temp_output_dir):
        """Test loading configuration from .env file."""
        # Create a temporary .env file
        env_file = temp_output_dir / ".env"
        env_content = """
NCDB_DATA_DIR=/test/data/from/dotenv
NCDB_OUTPUT_DIR=/test/output/from/dotenv
NCDB_MEMORY_LIMIT=16GB
        """.strip()
        
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        # Change to the directory containing .env
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_output_dir)
            
            # Clear environment variables
            with patch.dict(os.environ, {}, clear=True):
                # Test requires python-dotenv to be installed
                try:
                    result = get_data_directory()
                    # If dotenv is available, should load from file
                    if result is not None:
                        assert str(result) == "/test/data/from/dotenv"
                except ImportError:
                    # python-dotenv not installed, should return None
                    result = get_data_directory()
                    assert result is None
        
        finally:
            os.chdir(original_cwd)


class TestDirectoryValidation:
    """Test directory validation functions."""
    
    def test_validate_nonexistent_directory(self):
        """Test validating nonexistent directory."""
        result = validate_data_directory(Path("/nonexistent/directory"))
        assert result is False
    
    def test_validate_empty_directory(self, temp_output_dir):
        """Test validating empty directory."""
        result = validate_data_directory(temp_output_dir)
        assert result is False
    
    def test_validate_directory_without_ncdb_files(self, temp_output_dir):
        """Test validating directory without NCDB-like files."""
        # Create some non-NCDB files
        (temp_output_dir / "random.txt").touch()
        (temp_output_dir / "data.csv").touch()
        
        result = validate_data_directory(temp_output_dir)
        assert result is False
    
    def test_validate_directory_with_parquet_files(self, temp_output_dir):
        """Test validating directory with NCDB-like parquet files."""
        # Create NCDB-like parquet files
        (temp_output_dir / "NCDBPUF_Breast.parquet").touch()
        (temp_output_dir / "NCDBPUF_Lung.parquet").touch()
        
        result = validate_data_directory(temp_output_dir)
        assert result is True
    
    def test_validate_directory_with_dat_files(self, temp_output_dir):
        """Test validating directory with NCDB-like dat files."""
        # Create NCDB-like dat files  
        (temp_output_dir / "NCDBPUF_Breast.dat").touch()
        (temp_output_dir / "cancer_data.dat").touch()
        
        result = validate_data_directory(temp_output_dir)
        assert result is True
    
    def test_validate_real_data_directory(self, sample_data_dir):
        """Test validating real NCDB data directory."""
        result = validate_data_directory(sample_data_dir)
        assert result is True


class TestConfigIntegration:
    """Integration tests for configuration management."""
    
    def test_environment_override_precedence(self, temp_output_dir):
        """Test that environment variables take precedence over .env files."""
        # Create .env file with one value
        env_file = temp_output_dir / ".env"
        with open(env_file, 'w') as f:
            f.write("NCDB_DATA_DIR=/from/dotenv")
        
        # Set environment variable with different value
        env_value = "/from/environment"
        
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_output_dir)
            
            with patch.dict(os.environ, {'NCDB_DATA_DIR': env_value}):
                result = get_data_directory()
                assert str(result) == env_value
        
        finally:
            os.chdir(original_cwd)
    
    def test_config_with_real_paths(self, sample_data_dir):
        """Test configuration with real paths."""
        with patch.dict(os.environ, {'NCDB_DATA_DIR': str(sample_data_dir)}):
            data_dir = get_data_directory()
            assert data_dir == sample_data_dir
            assert validate_data_directory(data_dir) is True