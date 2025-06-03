#!/usr/bin/env python
"""Test the high-level build_database function."""

import ncdb_tools

# Just pass the directory - the function handles everything else
data_dir = "/Volumes/jabrant/Jason/NCDB/NCDB_PUF_DATA_Sep-14-2024"

print("Building NCDB database...")
print("This will process all .dat files in the directory\n")

try:
    paths = ncdb_tools.build_database(data_dir)
    
    print("\n" + "="*60)
    print("COMPLETE! Database created successfully")
    print(f"Location: {paths['output_dir']}")
    print(f"Open the HTML dictionary to explore: {paths['data_dictionary_html']}")
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()