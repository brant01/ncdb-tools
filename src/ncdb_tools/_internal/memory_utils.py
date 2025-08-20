"""Memory management utilities for NCDB Tools."""

import logging
from typing import Dict, Optional, Union

import psutil

logger = logging.getLogger(__name__)


def get_memory_info() -> Dict[str, Union[str, int, float]]:
    """Get system memory information.

    Returns:
        Dictionary with memory information including total, available,
        used memory in GB and recommended memory limit.
    """
    memory = psutil.virtual_memory()

    total_gb = memory.total / (1024**3)
    available_gb = memory.available / (1024**3)
    used_gb = memory.used / (1024**3)

    # Recommend using 60% of available memory for safety
    recommended_limit = max(1.0, available_gb * 0.6)

    return {
        'total': f"{total_gb:.1f}GB",
        'available': f"{available_gb:.1f}GB",
        'used': f"{used_gb:.1f}GB",
        'total_bytes': memory.total,
        'available_bytes': memory.available,
        'used_bytes': memory.used,
        'percent_used': float(memory.percent),
        'recommended_limit': f"{recommended_limit:.1f}GB",
        'recommended_limit_gb': recommended_limit
    }


def get_recommended_memory_limit() -> str:
    """Get recommended memory limit for NCDB operations.

    Returns:
        Recommended memory limit as string (e.g., "4.5GB")
    """
    memory_info = get_memory_info()
    recommended_limit = memory_info['recommended_limit']
    assert isinstance(recommended_limit, str)  # Type guard
    return recommended_limit


def parse_memory_limit(limit: str) -> int:
    """Parse memory limit string to bytes.

    Args:
        limit: Memory limit string (e.g., "4GB", "2.5GB", "1024MB")

    Returns:
        Memory limit in bytes

    Raises:
        ValueError: If limit format is invalid
    """
    limit = limit.upper().strip()

    if limit.endswith('GB'):
        return int(float(limit[:-2]) * 1024**3)
    elif limit.endswith('MB'):
        return int(float(limit[:-2]) * 1024**2)
    elif limit.endswith('KB'):
        return int(float(limit[:-2]) * 1024)
    elif limit.endswith('B'):
        return int(limit[:-1])
    else:
        # Try to parse as number (assume GB)
        try:
            return int(float(limit) * 1024**3)
        except ValueError:
            raise ValueError(f"Invalid memory limit format: {limit}")


def check_memory_usage(required_gb: Optional[float] = None) -> bool:
    """Check if there's sufficient memory available.

    Args:
        required_gb: Required memory in GB. If None, uses current available memory.

    Returns:
        True if sufficient memory is available
    """
    memory_info = get_memory_info()
    available_bytes = memory_info['available_bytes']
    assert isinstance(available_bytes, int)  # Type guard
    available_gb = available_bytes / (1024**3)

    if required_gb is None:
        return available_gb > 1.0  # At least 1GB available

    return available_gb >= required_gb


def warn_if_low_memory(operation: str = "operation") -> None:
    """Warn user if system memory is low.

    Args:
        operation: Name of operation being performed (for warning message)
    """
    memory = psutil.virtual_memory()

    if memory.percent > 85:
        logger.warning(
            f"System memory usage is high ({memory.percent:.1f}%). "
            f"Consider closing other applications before running {operation}."
        )
    elif memory.available < 1 * 1024**3:  # Less than 1GB available
        logger.warning(
            f"Low available memory ({memory.available / 1024**3:.1f}GB). "
            f"The {operation} may be slow or fail."
        )
