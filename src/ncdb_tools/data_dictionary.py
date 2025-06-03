"""Data dictionary generation for NCDB datasets."""

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

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
    dataset_summary: Optional[Dict[str, Any]] = None,
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
        dataset_summary: Optional summary information for HTML output
        
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
        _write_html_dictionary(dict_df, html_path, dataset_summary)
        output_paths["html"] = html_path

    return output_paths


def _write_html_dictionary(df: pl.DataFrame, output_path: Path, dataset_summary: Optional[Dict[str, Any]] = None) -> None:
    """Write HTML version of data dictionary with tabs and styling."""

    # Generate summary information if provided
    summary_content = ""
    if dataset_summary:
        summary_content = f"""
            <h2>Dataset Overview</h2>
            <div class="summary-grid">
                <div class="summary-item">
                    <h3>Total Records</h3>
                    <p class="big-number">{dataset_summary.get('total_rows', 'N/A'):,}</p>
                </div>
                <div class="summary-item">
                    <h3>Files Processed</h3>
                    <p class="big-number">{dataset_summary.get('file_count', 'N/A')}</p>
                </div>
                <div class="summary-item">
                    <h3>Data Year</h3>
                    <p class="big-number">{dataset_summary.get('year', 'N/A')}</p>
                </div>
                <div class="summary-item">
                    <h3>Total Variables</h3>
                    <p class="big-number">{len(df)}</p>
                </div>
            </div>
            
            <h3>Tumor Types Included</h3>
            <div class="tumor-types">
"""

        if 'files' in dataset_summary:
            for i, file_info in enumerate(dataset_summary['files']):
                if i % 5 == 0:
                    summary_content += "<div class='tumor-row'>"

                tumor_type = file_info['tumor_type']
                row_count = file_info['rows']
                summary_content += f"<span class='tumor-tag'>{tumor_type} ({row_count:,})</span>"

                if (i + 1) % 5 == 0 or i == len(dataset_summary['files']) - 1:
                    summary_content += "</div>"

        summary_content += """
            </div>
            
            <h3>Processing Details</h3>
            <ul class="details-list">
"""

        if 'processing_time' in dataset_summary:
            summary_content += f"<li>Processing completed in: {dataset_summary['processing_time']}</li>"
        if 'compressed_size' in dataset_summary:
            summary_content += f"<li>Total compressed size: {dataset_summary['compressed_size']:.1f} MB</li>"
        if 'output_directory' in dataset_summary:
            summary_content += f"<li>Output directory: <code>{dataset_summary['output_directory']}</code></li>"

        summary_content += "</ul>"

    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>NCDB Data Dictionary</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #4CAF50, #45a049);
            color: white;
            padding: 20px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
        }}
        .header p {{
            margin: 5px 0 0 0;
            font-size: 1.2em;
            opacity: 0.9;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        .tabs {{
            display: flex;
            background-color: white;
            border-radius: 8px 8px 0 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .tab-button {{
            padding: 15px 30px;
            background: none;
            border: none;
            cursor: pointer;
            font-size: 16px;
            font-weight: 500;
            color: #666;
            transition: all 0.3s;
        }}
        .tab-button:first-child {{
            border-radius: 8px 0 0 0;
        }}
        .tab-button.active {{
            background-color: #4CAF50;
            color: white;
        }}
        .tab-button:hover:not(.active) {{
            background-color: #f0f0f0;
        }}
        .tab-content {{
            display: none;
            background-color: white;
            padding: 20px;
            border-radius: 0 0 8px 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .tab-content.active {{
            display: block;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-item {{
            text-align: center;
            padding: 20px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 8px;
            border-left: 4px solid #4CAF50;
        }}
        .summary-item h3 {{
            margin: 0 0 10px 0;
            color: #333;
            font-size: 1.1em;
        }}
        .big-number {{
            font-size: 2.2em;
            font-weight: bold;
            color: #4CAF50;
            margin: 0;
        }}
        .tumor-types {{
            margin: 20px 0;
        }}
        .tumor-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 10px;
        }}
        .tumor-tag {{
            background: linear-gradient(135deg, #e3f2fd, #bbdefb);
            padding: 8px 12px;
            border-radius: 20px;
            font-size: 0.9em;
            color: #1565c0;
            font-weight: 500;
            border: 1px solid #90caf9;
        }}
        .details-list {{
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #4CAF50;
        }}
        .details-list li {{
            margin-bottom: 8px;
        }}
        .details-list code {{
            background-color: #e9ecef;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: monospace;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .numeric {{
            text-align: right;
        }}
        .missing-high {{
            color: #d32f2f;
            font-weight: bold;
        }}
        .description {{
            font-style: italic;
            color: #666;
        }}
        .search-box {{
            margin: 20px 0;
            padding: 12px;
            width: 400px;
            font-size: 16px;
            border: 2px solid #ddd;
            border-radius: 25px;
            outline: none;
            transition: border-color 0.3s;
        }}
        .search-box:focus {{
            border-color: #4CAF50;
        }}
        .search-container {{
            text-align: center;
            margin-bottom: 20px;
        }}
    </style>
    <script>
        function showTab(tabName) {{
            // Hide all tab contents
            var contents = document.querySelectorAll('.tab-content');
            contents.forEach(function(content) {{
                content.classList.remove('active');
            }});
            
            // Remove active class from all buttons
            var buttons = document.querySelectorAll('.tab-button');
            buttons.forEach(function(button) {{
                button.classList.remove('active');
            }});
            
            // Show selected tab content
            document.getElementById(tabName).classList.add('active');
            
            // Add active class to clicked button
            event.target.classList.add('active');
        }}
        
        function filterTable() {{
            var input = document.getElementById("searchInput");
            var filter = input.value.toUpperCase();
            var table = document.getElementById("dataTable");
            var tr = table.getElementsByTagName("tr");
            
            for (var i = 1; i < tr.length; i++) {{
                var td = tr[i].getElementsByTagName("td")[0];
                if (td) {{
                    var txtValue = td.textContent || td.innerText;
                    if (txtValue.toUpperCase().indexOf(filter) > -1) {{
                        tr[i].style.display = "";
                    }} else {{
                        tr[i].style.display = "none";
                    }}
                }}
            }}
        }}
        
        // Set default active tab
        window.onload = function() {{
            document.querySelector('.tab-button').classList.add('active');
            document.querySelector('.tab-content').classList.add('active');
        }}
    </script>
</head>
<body>
    <div class="header">
        <h1>NCDB Data Dictionary</h1>
        <p>National Cancer Database Public Use File - Data Year 2021</p>
    </div>
    
    <div class="container">
        <div class="tabs">
            <button class="tab-button" onclick="showTab('summary')">Dataset Summary</button>
            <button class="tab-button" onclick="showTab('dictionary')">Data Dictionary</button>
        </div>
        
        <div id="summary" class="tab-content">
            {summary_content if summary_content else '<p>No summary information available.</p>'}
        </div>
        
        <div id="dictionary" class="tab-content">
            <div class="search-container">
                <input type="text" id="searchInput" class="search-box" 
                       onkeyup="filterTable()" placeholder="🔍 Search for variables...">
            </div>
            <table id="dataTable">
                <thead>
                    <tr>
"""

    # Add column headers
    for col in df.columns:
        html_content += f"                        <th>{col.replace('_', ' ').title()}</th>\n"

    html_content += """                    </tr>
                </thead>
                <tbody>
"""

    # Add data rows
    for row in df.iter_rows():
        html_content += "                    <tr>\n"
        for i, value in enumerate(row):
            col_name = df.columns[i]

            # Format cell based on content
            if col_name == "missing_pct" and value is not None:
                css_class = "numeric missing-high" if value > 50 else "numeric"
                html_content += f'                        <td class="{css_class}">{value}%</td>\n'
            elif col_name == "description":
                html_content += f'                        <td class="description">{value or ""}</td>\n'
            elif isinstance(value, (int, float)) and value is not None:
                html_content += f'                        <td class="numeric">{value}</td>\n'
            else:
                html_content += f'                        <td>{value or ""}</td>\n'

        html_content += "                    </tr>\n"

    html_content += """                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""

    with open(output_path, 'w') as f:
        f.write(html_content)
