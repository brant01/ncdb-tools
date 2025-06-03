"""NCDB Tools - Tools for managing and analyzing National Cancer Database data."""

__version__ = "0.1.0"

# Core functionality
from .database_builder import build_database
from .dataset_builder import build_dataset
from .data_dictionary import generate_data_dictionary
from .query import load_data, NCDBQuery

__all__ = [
    "build_database",  # High-level function for most users
    "build_dataset",
    "generate_data_dictionary", 
    "load_data",
    "NCDBQuery",
]