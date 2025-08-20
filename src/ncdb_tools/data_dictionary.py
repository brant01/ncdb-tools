"""Enhanced data dictionary generation for NCDB datasets."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import polars as pl

logger = logging.getLogger(__name__)


class DataDictionaryGenerator:
    """Enhanced data dictionary generator for NCDB datasets."""

    def __init__(self) -> None:
        """Initialize the data dictionary generator."""
        self.variable_descriptions: Dict[str, str] = {}

    def generate_from_parquet(
        self,
        parquet_path: Union[str, Path],
        output_file: Optional[Union[str, Path]] = None,
        formats: List[Literal["csv", "json", "html"]] = ["csv"],
        include_stats: bool = True,
        sample_size: int = 100000,
        sas_labels_file: Optional[Union[str, Path]] = None,
    ) -> Path:
        """Generate comprehensive data dictionary from parquet dataset.

        Args:
            parquet_path: Path to parquet file or directory
            output_file: Output file path (extension determines format if single format)
            formats: Output formats to generate
            include_stats: Whether to calculate detailed statistics
            sample_size: Number of rows to sample for statistics
            sas_labels_file: Optional SAS labels file for descriptions

        Returns:
            Path to the primary output file
        """
        parquet_path = Path(parquet_path)

        # Load SAS labels if provided
        if sas_labels_file:
            self._load_sas_labels(Path(sas_labels_file))

        # Determine output directory
        if output_file:
            output_file = Path(output_file)
            output_dir = output_file.parent
        else:
            output_dir = parquet_path.parent if parquet_path.is_file() else parquet_path
            output_file = output_dir / "data_dictionary.csv"

        logger.info(f"Generating data dictionary for {parquet_path}")

        # Load data
        if parquet_path.is_file():
            df = pl.scan_parquet(parquet_path)
        else:
            parquet_files = list(parquet_path.glob("*.parquet"))
            df = pl.scan_parquet(parquet_files)

        # Generate dictionary data
        dict_data = self._generate_dictionary_data(df, include_stats, sample_size)

        # Write output files
        output_paths = {}
        primary_output = None

        for fmt in formats:
            if fmt == "csv":
                if output_file and output_file.suffix == ".csv":
                    path = self._write_csv(dict_data, output_file)
                else:
                    path = self._write_csv(
                        dict_data, output_dir / "data_dictionary.csv"
                    )
            elif fmt == "json":
                if output_file and output_file.suffix == ".json":
                    path = self._write_json(dict_data, output_file)
                else:
                    path = self._write_json(
                        dict_data, output_dir / "data_dictionary.json"
                    )
            elif fmt == "html":
                if output_file and output_file.suffix == ".html":
                    path = self._write_html(dict_data, output_file)
                else:
                    path = self._write_html(
                        dict_data, output_dir / "data_dictionary.html"
                    )
            else:
                raise ValueError(f"Unsupported format: {fmt}")

            output_paths[fmt] = path
            if primary_output is None:
                primary_output = path

        logger.info(f"Data dictionary saved to: {primary_output}")
        return primary_output or output_file

    def _load_sas_labels(self, sas_file: Path) -> None:
        """Load variable descriptions from SAS labels file."""
        try:
            from ._internal.sas_parser import parse_sas_labels
            variable_labels, _ = parse_sas_labels(sas_file)  # Unpack tuple
            self.variable_descriptions = variable_labels
            logger.info(
                f"Loaded {len(self.variable_descriptions)} variable descriptions"
            )
        except ImportError:
            logger.warning("SAS parser not available, skipping labels")
        except Exception as e:
            logger.warning(f"Could not load SAS labels: {e}")

    def _generate_dictionary_data(
        self,
        df: pl.LazyFrame,
        include_stats: bool,
        sample_size: int
    ) -> List[Dict[str, Any]]:
        """Generate the core dictionary data."""
        schema = df.collect_schema()
        total_rows = df.select(pl.len()).collect().item()

        logger.info(f"Processing {len(schema)} columns with {total_rows:,} total rows")

        # Get null counts for all columns at once
        null_counts = df.null_count().collect()

        dict_data = []

        for col_name, data_type in schema.items():
            logger.debug(f"Processing column: {col_name}")

            # Basic information
            null_count = null_counts[col_name].item()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

            col_info = {
                'variable': col_name,
                'type': str(data_type),
                'description': self.variable_descriptions.get(col_name, ''),
                'missing_count': null_count,
                'missing_pct': round(null_pct, 2),
                'unique_values': None,
                'min': None,
                'max': None,
                'mean': None,
                'median': None
            }

            if include_stats:
                # Calculate detailed statistics
                stats = self._calculate_column_stats(
                    df, col_name, data_type, sample_size
                )
                col_info.update(stats)

            dict_data.append(col_info)

        return dict_data

    def _calculate_column_stats(
        self,
        df: pl.LazyFrame,
        col_name: str,
        data_type: pl.DataType,
        sample_size: int
    ) -> Dict[str, Any]:
        """Calculate detailed statistics for a column."""
        stats = {}

        try:
            # Sample data for efficiency with large datasets
            if sample_size and sample_size < df.select(pl.len()).collect().item():
                # Collect first, then sample (since LazyFrame doesn't have sample)
                full_df = df.collect()
                sample_df = pl.LazyFrame(full_df.sample(n=sample_size))
            else:
                sample_df = df

            # Count unique values
            unique_count = (
                sample_df.select(pl.col(col_name).n_unique()).collect().item()
            )
            stats['unique_values'] = unique_count

            # Numeric statistics
            if data_type.is_numeric():
                numeric_stats = (sample_df
                    .select([
                        pl.col(col_name).min().alias('min'),
                        pl.col(col_name).max().alias('max'),
                        pl.col(col_name).mean().alias('mean'),
                        pl.col(col_name).median().alias('median')
                    ])
                    .collect())

                if numeric_stats.height > 0:
                    row = numeric_stats.row(0)
                    stats.update({
                        'min': row[0],
                        'max': row[1],
                        'mean': round(row[2], 2) if row[2] is not None else None,
                        'median': row[3]
                    })

        except Exception as e:
            logger.debug(f"Could not calculate stats for {col_name}: {e}")

        return stats

    def _write_csv(self, dict_data: List[Dict[str, Any]], output_path: Path) -> Path:
        """Write dictionary data to CSV format."""
        df = pl.DataFrame(dict_data)
        df.write_csv(output_path)
        return output_path

    def _write_json(self, dict_data: List[Dict[str, Any]], output_path: Path) -> Path:
        """Write dictionary data to JSON format."""
        with open(output_path, 'w') as f:
            json.dump(dict_data, f, indent=2, default=str)
        return output_path

    def _write_html(self, dict_data: List[Dict[str, Any]], output_path: Path) -> Path:
        """Write dictionary data to HTML format."""
        html_content = self._generate_html_template(dict_data)
        with open(output_path, 'w') as f:
            f.write(html_content)
        return output_path

    def _generate_html_template(self, dict_data: List[Dict[str, Any]]) -> str:
        """Generate HTML template for data dictionary."""
        total_vars = len(dict_data)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Calculate summary statistics
        numeric_vars = sum(1 for d in dict_data if d['type'] in ['Int64', 'Float64'])
        string_vars = sum(
            1 for d in dict_data if 'Utf8' in d['type'] or 'String' in d['type']
        )
        missing_vars = sum(1 for d in dict_data if d['missing_count'] > 0)

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>NCDB Data Dictionary</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 30px; }}
        .summary {{ display: flex; gap: 30px; margin: 20px 0; }}
        .summary-box {{ background-color: #e9ecef; padding: 15px; border-radius: 5px; flex: 1; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        .numeric {{ background-color: #fff3cd; }}
        .string {{ background-color: #d1ecf1; }}
        .missing {{ background-color: #f8d7da; }}
        .description {{ max-width: 300px; word-wrap: break-word; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>NCDB Data Dictionary</h1>
        <p><strong>Generated:</strong> {timestamp}</p>
        <p><strong>Total Variables:</strong> {total_vars}</p>
    </div>

    <div class="summary">
        <div class="summary-box">
            <h3>Variable Types</h3>
            <p><strong>Numeric:</strong> {numeric_vars}</p>
            <p><strong>String:</strong> {string_vars}</p>
        </div>
        <div class="summary-box">
            <h3>Data Quality</h3>
            <p><strong>Variables with Missing Data:</strong> {missing_vars}</p>
            <p><strong>Completeness:</strong> {((total_vars - missing_vars) / total_vars * 100):.1f}%</p>
        </div>
    </div>

    <table>
        <thead>
            <tr>
                <th>Variable</th>
                <th>Type</th>
                <th>Description</th>
                <th>Missing Count</th>
                <th>Missing %</th>
                <th>Unique Values</th>
                <th>Min</th>
                <th>Max</th>
                <th>Mean</th>
                <th>Median</th>
            </tr>
        </thead>
        <tbody>
"""

        for row in dict_data:
            # Determine row class based on data type and missing data
            row_class = ""
            if row['missing_count'] > 0:
                row_class = "missing"
            elif row['type'] in ['Int64', 'Float64']:
                row_class = "numeric"
            elif 'Utf8' in row['type'] or 'String' in row['type']:
                row_class = "string"

            html += f"""
            <tr class="{row_class}">
                <td><strong>{row['variable']}</strong></td>
                <td>{row['type']}</td>
                <td class="description">{row['description']}</td>
                <td>{row['missing_count']:,}</td>
                <td>{row['missing_pct']:.1f}%</td>
                <td>{row['unique_values'] if row['unique_values'] is not None else 'N/A'}</td>  # noqa: E501
                <td>{row['min'] if row['min'] is not None else 'N/A'}</td>
                <td>{row['max'] if row['max'] is not None else 'N/A'}</td>
                <td>{row['mean'] if row['mean'] is not None else 'N/A'}</td>
                <td>{row['median'] if row['median'] is not None else 'N/A'}</td>
            </tr>"""

        html += """
        </tbody>
    </table>

    <div style="margin-top: 30px; padding: 15px; background-color: #f8f9fa; border-radius: 5px;">  # noqa: E501
        <h3>Legend</h3>
        <p><span class="numeric" style="padding: 2px 8px;">Numeric variables</span></p>
        <p><span class="string" style="padding: 2px 8px;">String variables</span></p>
        <p><span class="missing" style="padding: 2px 8px;">Variables with missing data</span></p>  # noqa: E501
    </div>
</body>
</html>
        """

        return html


# Legacy function for backward compatibility
def generate_data_dictionary(
    dataset_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    formats: List[Literal["csv", "json", "html"]] = ["csv", "json", "html"],
    include_stats: bool = True,
    sample_size: int = 10000,
    sas_labels_file: Optional[Union[str, Path]] = None,
) -> Dict[str, Path]:
    """Generate comprehensive data dictionary from NCDB parquet dataset.

    Legacy function - use DataDictionaryGenerator class for new code.
    """
    generator = DataDictionaryGenerator()

    output_paths: Dict[str, Path] = {}
    for fmt in formats:
        fmt_output_file = None
        if output_dir:
            output_dir = Path(output_dir)
            fmt_output_file = output_dir / f"data_dictionary.{fmt}"

        path = generator.generate_from_parquet(
            parquet_path=dataset_path,
            output_file=fmt_output_file,
            formats=[fmt],
            include_stats=include_stats,
            sample_size=sample_size,
            sas_labels_file=sas_labels_file
        )
        output_paths[fmt] = path

    return output_paths
