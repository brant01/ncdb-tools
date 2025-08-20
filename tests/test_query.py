"""Tests for NCDB query functionality."""

import pytest
import polars as pl
from pathlib import Path

import ncdb_tools
from ncdb_tools.query import NCDBQuery


class TestNCDBQuery:
    """Test the NCDBQuery class."""
    
    def test_load_data(self, sample_data_dir):
        """Test loading data from parquet directory."""
        query = ncdb_tools.load_data(sample_data_dir)
        assert isinstance(query, NCDBQuery)
        assert query.parquet_path == sample_data_dir
    
    def test_load_nonexistent_data(self):
        """Test loading data from nonexistent path."""
        with pytest.raises(FileNotFoundError):
            ncdb_tools.load_data("/nonexistent/path")
    
    def test_count(self, sample_data_dir):
        """Test counting rows."""
        query = ncdb_tools.load_data(sample_data_dir)
        count = query.count()
        assert isinstance(count, int)
        assert count > 0
    
    def test_columns_property(self, sample_data_dir):
        """Test accessing column names."""
        query = ncdb_tools.load_data(sample_data_dir)
        columns = query.columns
        assert isinstance(columns, list)
        assert len(columns) > 0
        assert "PUF_CASE_ID" in columns
        assert "AGE" in columns
    
    def test_filter_by_year(self, sample_data_dir):
        """Test filtering by year."""
        query = ncdb_tools.load_data(sample_data_dir)
        original_count = query.count()
        
        # Filter by a single year
        filtered = query.filter_by_year(2021)
        filtered_count = filtered.count()
        
        assert filtered_count <= original_count
        assert filtered_count > 0
    
    def test_filter_by_year_multiple(self, sample_data_dir):
        """Test filtering by multiple years."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        # Filter by multiple years
        filtered = query.filter_by_year([2020, 2021])
        count = filtered.count()
        
        assert count > 0
    
    def test_filter_by_primary_site(self, sample_data_dir):
        """Test filtering by primary site."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        # Filter by breast cancer sites
        filtered = query.filter_by_primary_site(["C509"])
        count = filtered.count()
        
        # Should have some breast cancer cases
        assert count >= 0  # Might be 0 if no breast cases in sample
    
    def test_filter_by_histology(self, sample_data_dir):
        """Test filtering by histology codes."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        # Filter by adenocarcinoma codes
        filtered = query.filter_by_histology([8140, 8141])
        count = filtered.count()
        
        assert count >= 0
    
    def test_drop_missing_vital_status(self, sample_data_dir):
        """Test dropping missing vital status."""
        query = ncdb_tools.load_data(sample_data_dir)
        original_count = query.count()
        
        filtered = query.drop_missing_vital_status()
        filtered_count = filtered.count()
        
        assert filtered_count <= original_count
    
    def test_method_chaining(self, sample_data_dir):
        """Test chaining multiple filter methods."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        # Chain multiple filters
        result = (query
                 .filter_by_year([2021])
                 .drop_missing_vital_status()
                 .filter_by_primary_site(["C509"]))
        
        count = result.count()
        assert count >= 0
    
    def test_sample(self, sample_data_dir, small_sample_size):
        """Test sampling data."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        sample_df = query.sample(n=small_sample_size)
        assert isinstance(sample_df, pl.DataFrame)
        assert sample_df.height <= small_sample_size
    
    def test_describe(self, sample_data_dir):
        """Test describe method."""
        query = ncdb_tools.load_data(sample_data_dir)
        info = query.describe()
        
        assert isinstance(info, dict)
        assert "total_rows" in info
        assert "columns" in info
        assert "column_names" in info
        assert "parquet_path" in info
    
    def test_lazy_frame_access(self, sample_data_dir):
        """Test accessing the underlying LazyFrame."""
        query = ncdb_tools.load_data(sample_data_dir)
        lf = query.lazy_frame
        
        assert isinstance(lf, pl.LazyFrame)
        
        # Use Polars operations directly
        result = lf.select("PUF_CASE_ID").limit(10).collect()
        assert isinstance(result, pl.DataFrame)
        assert result.height <= 10
    
    def test_polars_method_delegation(self, sample_data_dir):
        """Test that Polars methods are properly delegated."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        # Use Polars methods directly on the query
        result = (query
                 .select(["PUF_CASE_ID", "AGE"])
                 .limit(100)
                 .collect())
        
        assert isinstance(result, pl.DataFrame)
        assert result.height <= 100
        assert result.width == 2
    
    def test_select_demographics(self, sample_data_dir):
        """Test selecting demographic columns."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        demo_query = query.select_demographics()
        columns = demo_query.columns
        
        # Should have demographic columns that exist in the data
        expected_demo_cols = ["AGE", "SEX", "RACE"]
        for col in expected_demo_cols:
            if col in query.columns:
                assert col in columns
    
    def test_select_outcomes(self, sample_data_dir):
        """Test selecting outcome columns."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        outcome_query = query.select_outcomes()
        columns = outcome_query.columns
        
        # Should have outcome columns that exist in the data
        if "PUF_VITAL_STATUS" in query.columns:
            assert "PUF_VITAL_STATUS" in columns


class TestQueryIntegration:
    """Integration tests for query functionality."""
    
    def test_real_data_analysis(self, sample_data_dir, small_sample_size):
        """Test realistic data analysis workflow."""
        # Load and perform basic analysis
        result = (ncdb_tools.load_data(sample_data_dir)
                 .filter_by_year([2021])
                 .sample(n=small_sample_size, seed=42)
                 .select(["PUF_CASE_ID", "AGE", "SEX", "PRIMARY_SITE"]))
        
        assert isinstance(result, pl.DataFrame)
        assert result.height <= small_sample_size
        assert "PUF_CASE_ID" in result.columns
    
    def test_complex_filtering(self, sample_data_dir):
        """Test complex filtering operations."""
        query = ncdb_tools.load_data(sample_data_dir)
        
        # Complex filter combining NCDB-specific and Polars methods
        result = (query
                 .filter_by_year([2021])
                 .filter(pl.col("AGE").cast(pl.Int64, strict=False) > 50)
                 .count())
        
        assert isinstance(result, int)
        assert result >= 0