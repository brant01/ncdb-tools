"""NCDB Tools: A Python package for working with NCDB cancer registry data.

This package provides tools for ingesting, transforming, and querying
National Cancer Database (NCDB) data using Polars and parquet datasets.
"""

from ._internal.memory_utils import get_memory_info, get_recommended_memory_limit
from ._internal.validation import NCDBValidationError
from .builder import build_parquet_dataset
from .config import get_data_directory, get_memory_limit, get_output_directory
from .query import NCDBQuery, load_data

__all__ = [
    "NCDBQuery",
    "NCDBValidationError",
    "build_parquet_dataset",
    "get_data_directory",
    "get_memory_info",
    "get_memory_limit",
    "get_output_directory",
    "get_recommended_memory_limit",
    "load_data",
]

try:
    from importlib.metadata import version
    __version__ = version("ncdb-tools")
except ImportError:
    # Fallback for Python < 3.8
    from importlib_metadata import version  # type: ignore
    __version__ = version("ncdb-tools")
except Exception:
    __version__ = "unknown"
