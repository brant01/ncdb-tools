"""NCDB Tools - Tools for managing and analyzing National Cancer Database data."""

__version__ = "0.1.0"

# Core functionality
from .data_dictionary import generate_data_dictionary
from .database_builder import build_database
from .dataset_builder import build_dataset
from .query import NCDBQuery, load_data

__all__ = [
    "NCDBQuery",
    "build_database",  # High-level function for most users
    "build_dataset",
    "generate_data_dictionary",
    "load_data",
]
