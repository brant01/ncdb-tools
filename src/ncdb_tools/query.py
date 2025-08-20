"""Query and filtering functions for NCDB data.

This module provides a fluent API for filtering NCDB data that integrates
seamlessly with Polars LazyFrame operations.
"""
from pathlib import Path
from typing import Any, List, Optional, Union

import polars as pl
from typing_extensions import Self

from .constants import (
    DEMOGRAPHIC_COLUMNS,
    HISTOLOGY_COLUMN,
    OUTCOME_COLUMNS,
    PRIMARY_SITE_COLUMN,
    VITAL_STATUS_COLUMN,
    YEAR_COLUMN,
)


class NCDBQuery:
    """A query builder for NCDB data that behaves like a Polars LazyFrame.

    This class provides a fluent interface for filtering NCDB data and
    transparently delegates all LazyFrame methods, allowing seamless integration
    with Polars operations.

    Examples:
        >>> # Basic filtering with direct collect()
        >>> df = (NCDBQuery("path/to/parquet/dir")
        ...       .filter_by_primary_site(["C509", "C500"])  # Breast sites
        ...       .filter_by_year([2020, 2021])
        ...       .collect())

        >>> # Use any Polars LazyFrame method directly
        >>> df = (NCDBQuery("path/to/parquet/dir")
        ...       .filter_by_histology([8140, 8500])  # Adenocarcinoma codes
        ...       .filter(pl.col("AGE") > 50)
        ...       .select(["PUF_CASE_ID", "AGE", "YEAR_OF_DIAGNOSIS", "PRIMARY_SITE"])
        ...       .collect())

        >>> # Mix NCDB-specific and Polars methods
        >>> df = (NCDBQuery("path/to/parquet/dir")
        ...       .filter_by_primary_site("C509")
        ...       .drop_missing_vital_status()
        ...       .filter(pl.col("AGE") > 50)
        ...       .with_columns(pl.col("AGE").alias("patient_age"))
        ...       .group_by("YEAR_OF_DIAGNOSIS")
        ...       .agg(pl.count())
        ...       .collect())
    """

    def __init__(self, parquet_path: Union[str, Path]):
        """Initialize the query with a parquet dataset.

        Args:
            parquet_path: Path to parquet file or directory containing parquet files
        """
        self.parquet_path = Path(parquet_path)

        if not self.parquet_path.exists():
            raise FileNotFoundError(f"Parquet path does not exist: {parquet_path}")

        # Initialize LazyFrame
        if self.parquet_path.is_file():
            self._lf = pl.scan_parquet(self.parquet_path)
        else:
            # Directory containing parquet files
            parquet_files = list(self.parquet_path.glob("*.parquet"))
            if not parquet_files:
                raise ValueError(f"No parquet files found in directory: {parquet_path}")
            self._lf = pl.scan_parquet(parquet_files)

    # NCDB-specific filter methods

    def filter_by_year(self, years: Union[int, List[int]]) -> Self:
        """Filter by year(s) of diagnosis.

        Args:
            years: Single year or list of years to include

        Returns:
            Self for method chaining
        """
        if isinstance(years, int):
            years = [years]

        self._lf = self._lf.filter(pl.col(YEAR_COLUMN).is_in(years))
        return self

    def filter_by_primary_site(self, sites: Union[str, List[str]]) -> Self:
        """Filter by primary site code(s).

        Args:
            sites: Single site code or list of ICD-O-3 primary site codes

        Returns:
            Self for method chaining
        """
        if isinstance(sites, str):
            sites = [sites]

        self._lf = self._lf.filter(pl.col(PRIMARY_SITE_COLUMN).is_in(sites))
        return self

    def filter_by_histology(
        self, codes: Union[int, str, List[Union[int, str]]]
    ) -> Self:
        """Filter by histology code(s).

        Args:
            codes: Single histology code or list of histology codes
                   (accepts integers or strings)

        Returns:
            Self for method chaining
        """
        if not isinstance(codes, list):
            codes = [codes]

        # Convert all codes to strings for consistency
        str_codes = [str(code) for code in codes]

        self._lf = self._lf.filter(pl.col(HISTOLOGY_COLUMN).is_in(str_codes))
        return self

    def drop_missing_vital_status(self) -> Self:
        """Remove cases with missing vital status.

        Returns:
            Self for method chaining
        """
        self._lf = self._lf.filter(pl.col(VITAL_STATUS_COLUMN).is_not_null())
        return self

    def filter_active_variables(self) -> Self:
        """Keep only variables that have data in the most recent year.

        This helps reduce the dataset size by removing columns that are
        no longer collected or were only collected in older years.

        Returns:
            Self for method chaining
        """
        # Get the most recent year in the dataset
        recent_year_lf = self._lf.select(pl.col(YEAR_COLUMN).max())
        recent_year = recent_year_lf.collect().item()

        # Get data from most recent year to check which columns have data
        recent_data = self._lf.filter(pl.col(YEAR_COLUMN) == recent_year)

        # Find columns with non-null data in recent year
        null_counts = recent_data.null_count().collect()
        total_rows = recent_data.select(pl.len()).collect().item()

        # Select columns that have at least some data (not 100% null)
        active_columns = [
            col for col in null_counts.columns
            if null_counts[col].item() < total_rows
        ]

        self._lf = self._lf.select(active_columns)
        return self

    def select_demographics(self) -> Self:
        """Select common demographic variables.

        Returns:
            Self for method chaining
        """
        available_columns = [col for col in DEMOGRAPHIC_COLUMNS if col in self.columns]
        self._lf = self._lf.select(available_columns)
        return self

    def select_outcomes(self) -> Self:
        """Select common outcome variables.

        Returns:
            Self for method chaining
        """
        available_columns = [col for col in OUTCOME_COLUMNS if col in self.columns]
        self._lf = self._lf.select(available_columns)
        return self

    # Convenience methods for data access

    @property
    def lazy_frame(self) -> pl.LazyFrame:
        """Access the underlying Polars LazyFrame directly.

        Returns:
            The LazyFrame for custom operations
        """
        return self._lf

    @property
    def columns(self) -> List[str]:
        """Get column names from the LazyFrame schema.

        Returns:
            List of column names
        """
        return list(self._lf.collect_schema().names())

    def count(self) -> int:
        """Get the count of rows without collecting the full dataset.

        Returns:
            Number of rows in the filtered dataset
        """
        result = self._lf.select(pl.len()).collect().item(0, 0)
        assert isinstance(result, int)  # Type guard
        return result

    def sample(self, n: int = 1000, seed: Optional[int] = None) -> pl.DataFrame:
        """Get a random sample of rows.

        Args:
            n: Number of rows to sample
            seed: Random seed for reproducibility

        Returns:
            Polars DataFrame with sampled rows
        """
        # LazyFrame doesn't have sample method, so collect first then sample
        df = self._lf.collect()
        if seed is not None:
            return df.sample(n=n, seed=seed)
        return df.sample(n=n)

    def describe(self) -> dict:
        """Get summary information about the query.

        Returns:
            Dictionary with query information
        """
        total_rows = self.count()
        columns = self.columns

        return {
            'total_rows': total_rows,
            'columns': len(columns),
            'column_names': columns[:10] + (['...'] if len(columns) > 10 else []),
            'parquet_path': str(self.parquet_path)
        }

    # Delegate all other methods to the LazyFrame
    def __getattr__(self, name: str) -> Any:
        """Delegate any missing methods to the underlying LazyFrame."""
        attr = getattr(self._lf, name)

        # If it's a method that returns a LazyFrame, wrap it
        if callable(attr):
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                if isinstance(result, pl.LazyFrame):
                    # Create a new instance with the updated LazyFrame
                    new_query = self.__class__.__new__(self.__class__)
                    new_query.parquet_path = self.parquet_path
                    new_query._lf = result
                    return new_query
                return result
            return wrapper

        return attr

    def __repr__(self) -> str:
        """String representation of the query."""
        try:
            count = self.count()
            return f"NCDBQuery(path={self.parquet_path.name}, rows={count:,})"
        except Exception:
            return f"NCDBQuery(path={self.parquet_path.name})"


def load_data(parquet_path: Union[str, Path]) -> NCDBQuery:
    """Load NCDB data from a parquet dataset for querying.

    Args:
        parquet_path: Path to parquet file or directory containing parquet files

    Returns:
        NCDBQuery instance for filtering and analysis

    Examples:
        >>> # Load and filter data
        >>> df = (load_data("/path/to/ncdb_parquet/")
        ...       .filter_by_primary_site(["C509"])  # Breast
        ...       .filter_by_year([2020, 2021])
        ...       .collect())

        >>> # Chain with Polars operations
        >>> df = (load_data("/path/to/ncdb_parquet/")
        ...       .filter_by_histology([8140])  # Adenocarcinoma
        ...       .filter(pl.col("AGE") > 50)
        ...       .group_by("PRIMARY_SITE")
        ...       .agg(pl.count())
        ...       .collect())
    """
    return NCDBQuery(parquet_path)
