#!/usr/bin/env python
"""Test script for ncdb_tools - processes a single file."""

import sys
from pathlib import Path
import ncdb_tools

def main():
    # Test with a single small file first
    data_dir = Path("/Volumes/jabrant/Jason/NCDB/NCDB_PUF_DATA_Sep-14-2024")
    
    # Pick the smallest file to test
    test_file = data_dir / "NCDBPUF_BoneJont.3.2021.0.dat"
    sas_file = Path("docs/NCDB PUF SAS Labels 2021.sas")
    columns_file = Path("docs/columns.csv")
    
    if not test_file.exists():
        print(f"Test file not found: {test_file}")
        return 1
    
    print(f"Testing with: {test_file.name}")
    print(f"File size: {test_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    try:
        # Build parquet file
        print("\n1. Building parquet dataset...")
        parquet_path = ncdb_tools.build_dataset(
            input_file=test_file,
            sas_labels_file=sas_file,
            columns_file=columns_file,  # Using columns.csv since we have it
        )
        print(f"   Created: {parquet_path}")
        
        # Generate data dictionary
        print("\n2. Generating data dictionary...")
        dict_paths = ncdb_tools.generate_data_dictionary(
            dataset_path=parquet_path,
            formats=["csv", "json", "html"],
            include_stats=True,
            sample_size=1000,  # Small sample for testing
            sas_labels_file=sas_file,
        )
        print("   Created:")
        for fmt, path in dict_paths.items():
            print(f"   - {fmt}: {path}")
        
        # Test querying
        print("\n3. Testing query interface...")
        query = ncdb_tools.load_data(parquet_path)
        print(f"   Dataset has {query.count():,} rows")
        
        # Get a small sample
        sample = query.head(5)
        print("\n   First 5 rows:")
        print(sample)
        
        print("\n✅ All tests passed!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())