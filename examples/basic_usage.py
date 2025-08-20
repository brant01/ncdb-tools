#!/usr/bin/env python3
"""Basic usage examples for NCDB Tools."""

import ncdb_tools
import polars as pl
from pathlib import Path


def main():
    """Demonstrate basic NCDB Tools usage."""
    
    # Example 1: Build parquet dataset from NCDB data files
    print("=== Example 1: Building Dataset ===")
    
    # Build parquet dataset from NCDB data files
    # result = ncdb_tools.build_parquet_dataset(
    #     data_dir="/path/to/ncdb/data",
    #     generate_dictionary=True,
    #     memory_limit="4GB"
    # )
    # 
    # print(f"Dataset created at: {result['parquet_dir']}")
    # print(f"Data dictionary at: {result['dictionary']}")
    
    # Example 2: Query existing parquet dataset
    print("\n=== Example 2: Querying Data ===")
    
    # Using the existing parquet files from the data directory
    data_dir = Path(__file__).parent.parent / "data" / "ncdb_parquet_20250603"
    
    if data_dir.exists():
        print(f"Loading data from: {data_dir}")
        
        # Basic filtering with NCDB-specific methods
        print("\n--- Basic Filtering ---")
        query = ncdb_tools.load_data(data_dir)
        print(f"Total records: {query.count():,}")
        
        # Filter by year
        recent_data = query.filter_by_year([2020, 2021])
        print(f"Records from 2020-2021: {recent_data.count():,}")
        
        # Filter by primary site (breast cancer)
        breast_cases = query.filter_by_primary_site(["C509", "C500", "C501"])
        print(f"Breast cancer cases: {breast_cases.count():,}")
        
        # Example 3: Advanced querying with Polars integration
        print("\n--- Advanced Querying ---")
        
        # Mix NCDB-specific and Polars methods
        df = (ncdb_tools.load_data(data_dir)
              .filter_by_primary_site("C509")  # Breast, NOS
              .filter_by_year([2021])
              .drop_missing_vital_status()
              .filter(pl.col("AGE") >= 50)  # Standard Polars filter
              .select([
                  "PUF_CASE_ID",
                  "AGE", 
                  "SEX",
                  "RACE",
                  "PRIMARY_SITE",
                  "HISTOLOGY",
                  "YEAR_OF_DIAGNOSIS"
              ])
              .collect())
        
        print(f"Filtered dataset shape: {df.shape}")
        print("\nSample data:")
        print(df.head(3))
        
        # Example 4: Demographic analysis
        print("\n--- Demographic Analysis ---")
        
        demo_summary = (ncdb_tools.load_data(data_dir)
                       .filter_by_primary_site("C509")
                       .filter_by_year([2021])
                       .lazy_frame
                       .group_by(["SEX", "RACE"])
                       .agg([
                           pl.count().alias("count"),
                           pl.col("AGE").cast(pl.Int64, strict=False).mean().alias("mean_age")
                       ])
                       .sort("count", descending=True)
                       .collect())
        
        print("Demographics summary:")
        print(demo_summary.head(10))
        
        # Example 5: Using convenience methods
        print("\n--- Convenience Methods ---")
        
        # Get just demographic columns
        demographics = (ncdb_tools.load_data(data_dir)
                       .select_demographics()
                       .sample(n=1000)
                       .select(["AGE", "SEX", "RACE", "INSURANCE_STATUS"]))
        
        print("Demographics sample:")
        print(demographics.head(5))
        
    else:
        print(f"Data directory not found: {data_dir}")
        print("Please run the build_database script first or update the path.")
    
    # Example 6: Memory management
    print("\n=== Example 6: Memory Management ===")
    
    # Check system memory
    mem_info = ncdb_tools.get_memory_info()
    print(f"Total RAM: {mem_info['total']}")
    print(f"Available: {mem_info['available']}")
    print(f"Recommended limit: {mem_info['recommended_limit']}")


if __name__ == "__main__":
    main()