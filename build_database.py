#!/usr/bin/env python3
"""
Command-line script to build NCDB database from .dat files.

Usage:
    uv run build_database.py <data_directory>

This script will:
1. Find all .dat files in the specified directory
2. Locate the SAS labels file
3. Create a timestamped output subdirectory
4. Convert all .dat files to parquet format
5. Generate comprehensive data dictionaries (CSV, JSON, HTML)
"""

import sys
from pathlib import Path
from ncdb_tools import build_database

def main():
    if len(sys.argv) != 2:
        print("Usage: uv run build_database.py <data_directory>")
        print("\nExample:")
        print("  uv run build_database.py \"R:\\Jason\\NCDB\\NCDB_PUF_DATA_Sep-14-2024\"")
        return 1
    
    data_dir = sys.argv[1]
    
    try:
        result = build_database(data_dir=data_dir)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())