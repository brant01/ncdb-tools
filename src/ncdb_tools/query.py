"""Query interface for NCDB datasets."""

from pathlib import Path
from typing import List, Optional, Union

import polars as pl


def load_data(
    dataset_path: Union[str, Path],
    years: Optional[Union[int, List[int]]] = None,
    columns: Optional[List[str]] = None,
) -> "NCDBQuery":
    """
    Load NCDB parquet dataset for querying.
    
    Args:
        dataset_path: Path to parquet dataset directory or single parquet file
        years: Optional year(s) to filter on (uses YEAR_OF_DIAGNOSIS)
        columns: Optional columns to select
        
    Returns:
        NCDBQuery object for filtering and analysis
        
    Example:
        >>> # Load all data
        >>> query = load_data("path/to/dataset.parquet")
        >>> 
        >>> # Load specific years and get LazyFrame
        >>> df = (
        ...     load_data("path/to/dataset.parquet", years=[2019, 2020])
        ...     .filter_by_primary_site("C509")  # Breast
        ...     .drop_missing_vital_status()
        ...     .lazy_frame()
        ... )
        >>> 
        >>> # Use standard Polars operations
        >>> results = df.filter(pl.col("AGE") > 50).collect()
    """
    return NCDBQuery(dataset_path, years, columns)


class NCDBQuery:
    """
    Query interface for NCDB parquet datasets.
    
    Provides NCDB-specific filters that can be chained, then access
    the underlying Polars LazyFrame for further operations.
    """

    def __init__(
        self,
        dataset_path: Union[str, Path],
        years: Optional[Union[int, List[int]]] = None,
        columns: Optional[List[str]] = None,
    ):
        """Initialize query with dataset."""
        self.dataset_path = Path(dataset_path)

        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")

        # Load as lazy frame
        if self.dataset_path.is_file():
            self._df = pl.scan_parquet(self.dataset_path)
        else:
            # Assume directory with parquet files
            self._df = pl.scan_parquet(self.dataset_path / "*.parquet")

        # Cache schema for performance
        self._schema = self._df.collect_schema()

        # Apply initial filters
        if years is not None:
            self.filter_by_year(years)

        if columns is not None:
            self._df = self._df.select(columns)

    def filter_by_year(self, years: Union[int, List[int]]) -> "NCDBQuery":
        """Filter data by year(s) of diagnosis."""
        if isinstance(years, int):
            years = [years]

        # Try common year column names
        year_columns = ["YEAR_OF_DIAGNOSIS", "YEAR", "DX_YEAR"]

        for col in year_columns:
            if col in self._schema:
                self._df = self._df.filter(pl.col(col).is_in(years))
                return self

        raise ValueError("No year column found in dataset")

    def filter_by_primary_site(self, sites: Union[str, List[str]]) -> "NCDBQuery":
        """Filter by primary site code(s)."""
        if isinstance(sites, str):
            sites = [sites]

        if "PRIMARY_SITE" in self._schema:
            self._df = self._df.filter(pl.col("PRIMARY_SITE").is_in(sites))
        else:
            raise ValueError("PRIMARY_SITE column not found")

        return self

    def filter_by_histology(self, codes: Union[int, List[int], str, List[str]]) -> "NCDBQuery":
        """Filter by histology code(s). Accepts both integers and strings."""
        if isinstance(codes, (int, str)):
            codes = [codes]

        # Convert all codes to strings for consistency
        str_codes = [str(code) for code in codes]

        histology_cols = ["HISTOLOGY", "HISTOLOGY_ICDO3"]

        for col in histology_cols:
            if col in self._schema:
                # Cast column to string and compare
                self._df = self._df.filter(pl.col(col).cast(pl.Utf8).is_in(str_codes))
                return self

        raise ValueError("No histology column found")

    def drop_missing_vital_status(self) -> "NCDBQuery":
        """Drop rows where PUF_VITAL_STATUS is null/missing."""
        if "PUF_VITAL_STATUS" in self._schema:
            self._df = self._df.filter(pl.col("PUF_VITAL_STATUS").is_not_null())
        else:
            raise ValueError("PUF_VITAL_STATUS column not found")

        return self

    def lazy_frame(self) -> pl.LazyFrame:
        """
        Get the underlying Polars LazyFrame for custom operations.
        
        Example:
            >>> # Get LazyFrame and use Polars operations
            >>> df = (
            ...     query
            ...     .filter_by_year(2020)
            ...     .lazy_frame()
            ...     .filter(pl.col("AGE") > 50)
            ...     .group_by("PRIMARY_SITE")
            ...     .agg(pl.count())
            ...     .collect()
            ... )
        """
        return self._df

    def __repr__(self) -> str:
        """String representation."""
        return f"NCDBQuery(dataset={self.dataset_path.name})"
