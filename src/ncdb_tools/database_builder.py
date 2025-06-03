"""High-level database builder for NCDB data."""

from pathlib import Path
from typing import Union, Optional, Dict
import datetime

from .dataset_builder import build_dataset
from .data_dictionary import generate_data_dictionary
from .constants import DATA_FILE_PATTERN


def build_database(
    data_dir: Union[str, Path],
    output_subdir: Optional[str] = None,
    batch_size: int = 10000,
) -> Dict[str, Path]:
    """
    Build complete NCDB database from directory of .dat files.
    
    This function:
    1. Finds all .dat files in the directory
    2. Locates the SAS labels file
    3. Creates a new subdirectory for output
    4. Converts all .dat files to .parquet
    5. Generates a comprehensive data dictionary
    
    Args:
        data_dir: Directory containing NCDB .dat files and SAS labels file
        output_subdir: Name for output subdirectory (defaults to ncdb_parquet_YYYYMMDD)
        batch_size: Number of rows to process at once during conversion
        
    Returns:
        Dictionary with paths to:
        - output_dir: Path to created output directory
        - parquet_files: List of created parquet files
        - data_dictionary_csv: Path to CSV dictionary
        - data_dictionary_json: Path to JSON dictionary
        - data_dictionary_html: Path to HTML dictionary
        
    Example:
        >>> # Simple usage - just pass the directory
        >>> paths = build_database("/path/to/NCDB_DATA/")
        >>> print(f"Created database in: {paths['output_dir']}")
    """
    data_dir = Path(data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    # Find all .dat files
    dat_files = list(data_dir.glob("*.dat"))
    if not dat_files:
        raise FileNotFoundError(f"No .dat files found in {data_dir}")
    
    print(f"Found {len(dat_files)} data files")
    
    # Find SAS labels file
    sas_files = list(data_dir.glob("*.sas"))
    if not sas_files:
        raise FileNotFoundError(f"No SAS labels file found in {data_dir}")
    if len(sas_files) > 1:
        # Pick the one with "label" in the name, or the first one
        sas_file = next((f for f in sas_files if "label" in f.name.lower()), sas_files[0])
        print(f"Multiple SAS files found, using: {sas_file.name}")
    else:
        sas_file = sas_files[0]
    
    print(f"Using SAS labels file: {sas_file.name}")
    
    # Create output directory
    if output_subdir is None:
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        output_subdir = f"ncdb_parquet_{date_str}"
    
    output_dir = data_dir / output_subdir
    output_dir.mkdir(exist_ok=True)
    print(f"\nCreating output in: {output_dir}")
    
    # Convert each .dat file to parquet
    parquet_files = []
    print("\nConverting data files:")
    
    for i, dat_file in enumerate(dat_files, 1):
        print(f"\n[{i}/{len(dat_files)}] Processing {dat_file.name}")
        print(f"      Size: {dat_file.stat().st_size / 1024 / 1024:.1f} MB")
        
        try:
            # Build parquet file in output directory
            output_file = output_dir / dat_file.with_suffix(".parquet").name
            
            parquet_path = build_dataset(
                input_file=dat_file,
                sas_labels_file=sas_file,
                output_file=output_file,
                batch_size=batch_size,
            )
            
            parquet_files.append(parquet_path)
            print(f"      ✓ Created: {parquet_path.name}")
            
        except Exception as e:
            print(f"      ✗ Error: {e}")
            continue
    
    if not parquet_files:
        raise RuntimeError("No parquet files were created successfully")
    
    print(f"\n✓ Converted {len(parquet_files)} files successfully")
    
    # Generate data dictionary from all parquet files
    print("\nGenerating data dictionary...")
    
    # Use the output directory as the dataset path (it contains all parquet files)
    dict_paths = generate_data_dictionary(
        dataset_path=output_dir,
        output_dir=output_dir,
        formats=["csv", "json", "html"],
        include_stats=True,
        sample_size=10000,
        sas_labels_file=sas_file,
    )
    
    print("✓ Data dictionary created")
    
    # Create summary report
    summary_path = output_dir / "conversion_summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"NCDB Data Conversion Summary\n")
        f.write(f"============================\n")
        f.write(f"Date: {datetime.datetime.now()}\n")
        f.write(f"Source directory: {data_dir}\n")
        f.write(f"Output directory: {output_dir}\n")
        f.write(f"SAS labels file: {sas_file.name}\n")
        f.write(f"\nFiles converted:\n")
        for pf in parquet_files:
            f.write(f"  - {pf.name}\n")
        f.write(f"\nTotal files: {len(parquet_files)}\n")
    
    print(f"\n✓ Summary written to: {summary_path.name}")
    
    # Return paths
    result = {
        "output_dir": output_dir,
        "parquet_files": parquet_files,
        "data_dictionary_csv": dict_paths.get("csv"),
        "data_dictionary_json": dict_paths.get("json"),
        "data_dictionary_html": dict_paths.get("html"),
        "summary": summary_path,
    }
    
    print(f"\n✅ Database build complete!")
    print(f"   Output directory: {output_dir}")
    print(f"   Files converted: {len(parquet_files)}")
    print(f"   Data dictionary: {dict_paths['html'].name}")
    
    return result