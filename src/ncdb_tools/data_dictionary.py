"""Data dictionary generation for NCDB datasets."""

from pathlib import Path
from typing import Union, Optional, List, Literal, Dict, Any

import polars as pl

from ._internal.sas_parser import parse_sas_labels


def generate_data_dictionary(
    dataset_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    formats: List[Literal["csv", "json", "html"]] = ["csv", "json", "html"],
    include_stats: bool = True,
    sample_size: int = 10000,
    batch_size: int = 50,
    sas_labels_file: Optional[Union[str, Path]] = None,
) -> Dict[str, Path]:
    """
    Generate comprehensive data dictionary from NCDB parquet dataset.
    
    Creates detailed documentation of all variables including:
    - Variable names and descriptions
    - Data types and formats
    - Missing data patterns
    - Value distributions
    - Years when variable was collected (if applicable)
    
    Args:
        dataset_path: Path to parquet dataset file or directory
        output_dir: Where to save dictionary files (defaults to dataset directory)
        formats: Output formats to generate
        include_stats: Whether to calculate statistics
        sample_size: Number of rows to sample for statistics
        batch_size: Number of columns to process at once
        sas_labels_file: Optional SAS file for variable descriptions
        
    Returns:
        Dictionary mapping format to output file path
        
    Example:
        >>> paths = generate_data_dictionary(
        ...     "path/to/dataset.parquet",
        ...     formats=["csv", "html"],
        ...     include_stats=True
        ... )
        >>> print(paths["csv"])  # Path to CSV dictionary
    """
    dataset_path = Path(dataset_path)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")
    
    # Set output directory
    if output_dir is None:
        output_dir = dataset_path.parent if dataset_path.is_file() else dataset_path
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load variable labels if available
    variable_labels = {}
    value_formats = {}
    if sas_labels_file:
        sas_path = Path(sas_labels_file)
        if sas_path.exists():
            variable_labels, value_formats = parse_sas_labels(sas_path)
    
    # Load dataset
    if dataset_path.is_file():
        df = pl.scan_parquet(dataset_path)
    else:
        df = pl.scan_parquet(dataset_path / "*.parquet")
    
    schema = df.collect_schema()
    
    # Process columns in batches
    all_entries = []
    column_names = list(schema.keys())
    
    for i in range(0, len(column_names), batch_size):
        batch_cols = column_names[i:i + batch_size]
        
        # Sample data for statistics
        sample_df = None
        if include_stats:
            sample_df = df.select(batch_cols).head(sample_size).collect()
        
        for col_name in batch_cols:
            # Skip internal columns
            if col_name.startswith("_"):
                continue
            
            entry: Dict[str, Any] = {
                "variable": col_name,
                "type": str(schema[col_name]),
                "description": variable_labels.get(col_name, ""),
            }
            
            if include_stats and sample_df is not None and col_name in sample_df.columns:
                col_series = sample_df[col_name]
                
                # Basic statistics
                entry["missing_count"] = int(col_series.null_count())
                entry["missing_pct"] = round(100 * col_series.null_count() / len(col_series), 2)
                entry["unique_values"] = int(col_series.n_unique())
                
                # Add value counts for categorical variables
                if entry["unique_values"] <= 20 and str(schema[col_name]) in ["Utf8", "Categorical"]:
                    value_counts = (
                        col_series.value_counts()
                        .sort("counts", descending=True)
                        .head(10)
                    )
                    values_list = []
                    for row in value_counts.iter_rows():
                        val, count = row
                        # Add label if available
                        if col_name in value_formats and str(val) in value_formats[col_name]:
                            label = value_formats[col_name][str(val)]
                            values_list.append(f"{val} ({label}): {count}")
                        else:
                            values_list.append(f"{val}: {count}")
                    entry["top_values"] = "; ".join(values_list)
                
                # Add numeric statistics
                elif str(schema[col_name]) in ["Int64", "Int32", "Float64", "Float32"]:
                    try:
                        min_val = col_series.min()
                        max_val = col_series.max()
                        mean_val = col_series.mean()
                        median_val = col_series.median()
                        
                        # Only convert numeric types to float
                        if min_val is not None:
                            try:
                                entry["min"] = float(min_val)  # type: ignore
                            except (TypeError, ValueError):
                                entry["min"] = str(min_val)
                        
                        if max_val is not None:
                            try:
                                entry["max"] = float(max_val)  # type: ignore
                            except (TypeError, ValueError):
                                entry["max"] = str(max_val)
                        
                        if mean_val is not None:
                            try:
                                entry["mean"] = round(float(mean_val), 2)  # type: ignore
                            except (TypeError, ValueError):
                                pass
                        
                        if median_val is not None:
                            try:
                                entry["median"] = float(median_val)  # type: ignore
                            except (TypeError, ValueError):
                                pass
                    except Exception:
                        pass
            
            all_entries.append(entry)
    
    # Create dictionary dataframe
    dict_df = pl.DataFrame(all_entries)
    
    # Generate output files
    output_paths = {}
    
    if "csv" in formats:
        csv_path = output_dir / "data_dictionary.csv"
        dict_df.write_csv(csv_path)
        output_paths["csv"] = csv_path
    
    if "json" in formats:
        json_path = output_dir / "data_dictionary.json"
        dict_df.write_json(json_path)
        output_paths["json"] = json_path
    
    if "html" in formats:
        html_path = output_dir / "data_dictionary.html"
        _write_html_dictionary(dict_df, html_path)
        output_paths["html"] = html_path
    
    return output_paths


def _write_html_dictionary(df: pl.DataFrame, output_path: Path) -> None:
    """Write HTML version of data dictionary with styling."""
    html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>NCDB Data Dictionary</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #333;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        th {
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
        }
        td {
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .numeric {
            text-align: right;
        }
        .missing-high {
            color: #d32f2f;
            font-weight: bold;
        }
        .description {
            font-style: italic;
            color: #666;
        }
        .search-box {
            margin: 20px 0;
            padding: 10px;
            width: 300px;
            font-size: 16px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
    </style>
    <script>
        function filterTable() {
            var input = document.getElementById("searchInput");
            var filter = input.value.toUpperCase();
            var table = document.getElementById("dataTable");
            var tr = table.getElementsByTagName("tr");
            
            for (var i = 1; i < tr.length; i++) {
                var td = tr[i].getElementsByTagName("td")[0];
                if (td) {
                    var txtValue = td.textContent || td.innerText;
                    if (txtValue.toUpperCase().indexOf(filter) > -1) {
                        tr[i].style.display = "";
                    } else {
                        tr[i].style.display = "none";
                    }
                }
            }
        }
    </script>
</head>
<body>
    <h1>NCDB Data Dictionary</h1>
    <input type="text" id="searchInput" class="search-box" 
           onkeyup="filterTable()" placeholder="Search for variables...">
    <table id="dataTable">
        <thead>
            <tr>
"""
    
    # Add column headers
    for col in df.columns:
        html_content += f"                <th>{col.replace('_', ' ').title()}</th>\n"
    
    html_content += """            </tr>
        </thead>
        <tbody>
"""
    
    # Add data rows
    for row in df.iter_rows():
        html_content += "            <tr>\n"
        for i, value in enumerate(row):
            col_name = df.columns[i]
            
            # Format cell based on content
            if col_name == "missing_pct" and value is not None:
                css_class = "numeric missing-high" if value > 50 else "numeric"
                html_content += f'                <td class="{css_class}">{value}%</td>\n'
            elif col_name == "description":
                html_content += f'                <td class="description">{value or ""}</td>\n'
            elif isinstance(value, (int, float)) and value is not None:
                html_content += f'                <td class="numeric">{value}</td>\n'
            else:
                html_content += f'                <td>{value or ""}</td>\n'
        
        html_content += "            </tr>\n"
    
    html_content += """        </tbody>
    </table>
</body>
</html>
"""
    
    with open(output_path, 'w') as f:
        f.write(html_content)