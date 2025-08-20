"""Tests for memory management utilities."""

import pytest

from ncdb_tools._internal.memory_utils import (
    get_memory_info,
    get_recommended_memory_limit,
    parse_memory_limit,
    check_memory_usage,
    warn_if_low_memory
)


class TestMemoryInfo:
    """Test memory information functions."""
    
    def test_get_memory_info(self):
        """Test getting system memory information."""
        info = get_memory_info()
        
        assert isinstance(info, dict)
        
        # Check required keys
        required_keys = [
            'total', 'available', 'used', 'total_bytes', 
            'available_bytes', 'used_bytes', 'percent_used',
            'recommended_limit', 'recommended_limit_gb'
        ]
        for key in required_keys:
            assert key in info
        
        # Check data types
        assert isinstance(info['total_bytes'], int)
        assert isinstance(info['available_bytes'], int)
        assert isinstance(info['used_bytes'], int)
        assert isinstance(info['percent_used'], float)
        assert isinstance(info['recommended_limit_gb'], float)
        
        # Check values are reasonable
        assert info['total_bytes'] > 0
        assert info['available_bytes'] >= 0
        assert info['used_bytes'] >= 0
        assert 0 <= info['percent_used'] <= 100
        assert info['recommended_limit_gb'] > 0
    
    def test_get_recommended_memory_limit(self):
        """Test getting recommended memory limit."""
        limit = get_recommended_memory_limit()
        
        assert isinstance(limit, str)
        assert limit.endswith('GB')
        
        # Should be a reasonable value
        limit_val = float(limit[:-2])
        assert limit_val > 0
        assert limit_val < 1000  # Sanity check


class TestMemoryParsing:
    """Test memory limit parsing."""
    
    def test_parse_memory_limit_gb(self):
        """Test parsing GB memory limits."""
        assert parse_memory_limit("4GB") == 4 * 1024**3
        assert parse_memory_limit("2.5GB") == int(2.5 * 1024**3)
        assert parse_memory_limit("8gb") == 8 * 1024**3
    
    def test_parse_memory_limit_mb(self):
        """Test parsing MB memory limits."""
        assert parse_memory_limit("512MB") == 512 * 1024**2
        assert parse_memory_limit("1024mb") == 1024 * 1024**2
    
    def test_parse_memory_limit_kb(self):
        """Test parsing KB memory limits."""
        assert parse_memory_limit("1024KB") == 1024 * 1024
    
    def test_parse_memory_limit_bytes(self):
        """Test parsing byte memory limits."""
        assert parse_memory_limit("1048576B") == 1048576
    
    def test_parse_memory_limit_numeric(self):
        """Test parsing numeric memory limits (assumes GB)."""
        assert parse_memory_limit("4") == 4 * 1024**3
        assert parse_memory_limit("2.5") == int(2.5 * 1024**3)
    
    def test_parse_memory_limit_invalid(self):
        """Test parsing invalid memory limits."""
        with pytest.raises(ValueError):
            parse_memory_limit("invalid")
        
        with pytest.raises(ValueError):
            parse_memory_limit("4XB")


class TestMemoryChecks:
    """Test memory checking functions."""
    
    def test_check_memory_usage_no_requirement(self):
        """Test memory usage check with no specific requirement."""
        result = check_memory_usage()
        assert isinstance(result, bool)
    
    def test_check_memory_usage_small_requirement(self):
        """Test memory usage check with small requirement."""
        result = check_memory_usage(required_gb=0.1)  # 100MB
        assert isinstance(result, bool)
        # Should generally be True for small requirements
        assert result is True
    
    def test_check_memory_usage_large_requirement(self):
        """Test memory usage check with large requirement."""
        result = check_memory_usage(required_gb=1000)  # 1TB
        assert isinstance(result, bool)
        # Should generally be False for very large requirements
        assert result is False
    
    def test_warn_if_low_memory(self):
        """Test low memory warning function."""
        # Should not raise an exception
        warn_if_low_memory("test operation")
        warn_if_low_memory()


class TestMemoryUtilsIntegration:
    """Integration tests for memory utilities."""
    
    def test_memory_info_consistency(self):
        """Test that memory information is internally consistent."""
        info = get_memory_info()
        
        # Total should be >= used + available (approximately, due to caching)
        total_approx = info['used_bytes'] + info['available_bytes']
        
        # Allow for some variance due to system caching
        assert abs(info['total_bytes'] - total_approx) / info['total_bytes'] < 0.1
        
        # Percent used should match calculated value (approximately)
        calculated_percent = (info['used_bytes'] / info['total_bytes']) * 100
        assert abs(info['percent_used'] - calculated_percent) < 1.0
    
    def test_recommended_limit_reasonable(self):
        """Test that recommended limit is reasonable."""
        info = get_memory_info()
        recommended_bytes = info['recommended_limit_gb'] * 1024**3
        
        # Should be less than available memory
        assert recommended_bytes <= info['available_bytes']
        
        # Should be at least 1GB
        assert info['recommended_limit_gb'] >= 1.0