"""NCDB Tools - Tools for managing and analyzing National Cancer Database data."""

__version__ = "0.1.0"

# New primary API (nsqip_tools style)
from ._internal.memory_utils import get_memory_info, get_recommended_memory_limit
from .builder import build_parquet_dataset
from .config import get_data_directory, get_memory_limit, get_output_directory

# Legacy API (for backward compatibility)
from .data_dictionary import generate_data_dictionary
from .database_builder import build_database
from .dataset_builder import build_dataset
from .query import NCDBQuery, load_data

__all__ = [
    "NCDBQuery",
    # Legacy API
    "build_database",
    "build_dataset",
    # New primary API
    "build_parquet_dataset",
    "generate_data_dictionary",
    "get_data_directory",
    "get_memory_info",
    "get_memory_limit",
    "get_output_directory",
    "get_recommended_memory_limit",
    "load_data",
]

try:
    from importlib.metadata import version as _version
    __version__ = _version("ncdb-tools")
except ImportError:
    # Fallback for Python < 3.8
    try:
        from importlib_metadata import version as _version  # type: ignore
        __version__ = _version("ncdb-tools")
    except ImportError:
        __version__ = "0.1.0"
except Exception:
    __version__ = "0.1.0"
