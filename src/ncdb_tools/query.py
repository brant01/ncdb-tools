"""Query interface for NCDB datasets."""

from pathlib import Path
from typing import Optional, Union, List

import polars as pl



def load_data(
    dataset_path: Union[str, Path],
    years: Optional[Union[int, List[int]]] = None,
    columns: Optional[List[str]] = None,
    memory_limit: Optional[str] = None,
) -> "NCDBQuery":
    """
    Load NCDB parquet dataset for querying.
    
    Args:
        dataset_path: Path to parquet dataset directory or single parquet file
        years: Optional year(s) to load (filters on YEAR_OF_DIAGNOSIS)
        columns: Optional columns to select
        memory_limit: Memory limit for collect operations (e.g., "8GB")
        
    Returns:
        NCDBQuery object for filtering and analysis
        
    Example:
        >>> # Load all data
        >>> query = load_data("path/to/dataset.parquet")
        >>> 
        >>> # Load specific years
        >>> query = load_data("path/to/dataset.parquet", years=[2018, 2019, 2020])
        >>> 
        >>> # Chain filters
        >>> results = (
        ...     query
        ...     .filter_by_year(2019)
        ...     .filter_by_primary_site("C509")  # Breast
        ...     .collect()
        ... )
    """
    return NCDBQuery(dataset_path, years, columns, memory_limit)


class NCDBQuery:
    """
    Query interface for NCDB parquet datasets.
    
    Provides a fluent interface for filtering and analyzing NCDB data
    using Polars LazyFrames for memory efficiency.
    """
    
    def __init__(
        self,
        dataset_path: Union[str, Path],
        years: Optional[Union[int, List[int]]] = None,
        columns: Optional[List[str]] = None,
        memory_limit: Optional[str] = None,
    ):
        """Initialize query with dataset."""
        self.dataset_path = Path(dataset_path)
        self.memory_limit = memory_limit
        
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")
        
        # Load as lazy frame
        if self.dataset_path.is_file():
            self._df = pl.scan_parquet(self.dataset_path)
        else:
            # Assume directory with parquet files
            self._df = pl.scan_parquet(self.dataset_path / "*.parquet")
        
        # Apply initial filters
        if years is not None:
            self._df = self._apply_year_filter(years)
        
        if columns is not None:
            self._df = self._df.select(columns)
    
    def _apply_year_filter(self, years: Union[int, List[int]]) -> pl.LazyFrame:
        """Apply year filter."""
        if isinstance(years, int):
            years = [years]
        
        # Try common year column names
        year_columns = ["YEAR_OF_DIAGNOSIS", "YEAR", "DX_YEAR"]
        
        for col in year_columns:
            if col in self._df.columns:
                return self._df.filter(pl.col(col).is_in(years))
        
        raise ValueError("No year column found in dataset")
    
    def filter_by_year(self, years: Union[int, List[int]]) -> "NCDBQuery":
        """Filter data by year(s)."""
        self._df = self._apply_year_filter(years)
        return self
    
    def filter_by_primary_site(self, sites: Union[str, List[str]]) -> "NCDBQuery":
        """Filter by primary site code(s)."""
        if isinstance(sites, str):
            sites = [sites]
        
        if "PRIMARY_SITE" in self._df.columns:
            self._df = self._df.filter(pl.col("PRIMARY_SITE").is_in(sites))
        else:
            raise ValueError("PRIMARY_SITE column not found")
        
        return self
    
    def filter_by_histology(self, codes: Union[int, List[int]]) -> "NCDBQuery":
        """Filter by histology code(s)."""
        if isinstance(codes, int):
            codes = [codes]
        
        histology_cols = ["HISTOLOGY", "HISTOLOGY_ICDO3"]
        
        for col in histology_cols:
            if col in self._df.columns:
                self._df = self._df.filter(pl.col(col).is_in(codes))
                return self
        
        raise ValueError("No histology column found")
    
    def filter_by_stage(self, stages: Union[str, List[str]]) -> "NCDBQuery":
        """Filter by clinical or pathologic stage."""
        if isinstance(stages, str):
            stages = [stages]
        
        # Try both clinical and pathologic stage columns
        stage_cols = ["TNM_CLIN_STAGE_GROUP", "TNM_PATH_STAGE_GROUP", "ANALYTIC_STAGE_GROUP"]
        
        filters = []
        for col in stage_cols:
            if col in self._df.columns:
                filters.append(pl.col(col).is_in(stages))
        
        if filters:
            # OR the filters together
            combined_filter = filters[0]
            for f in filters[1:]:
                combined_filter = combined_filter | f
            self._df = self._df.filter(combined_filter)
        else:
            raise ValueError("No stage columns found")
        
        return self
    
    def select_demographics(self) -> "NCDBQuery":
        """Select common demographic variables."""
        demo_cols = [
            "AGE", "SEX", "RACE", "SPANISH_HISPANIC_ORIGIN",
            "INSURANCE_STATUS", "CDCC_TOTAL_BEST"
        ]
        
        # Only select columns that exist
        available_cols = [col for col in demo_cols if col in self._df.columns]
        
        if available_cols:
            self._df = self._df.select(available_cols)
        else:
            raise ValueError("No demographic columns found")
        
        return self
    
    def select_treatment(self) -> "NCDBQuery":
        """Select treatment-related variables."""
        treatment_cols = [
            "RX_SUMM_SURG_PRIM_SITE", "RX_SUMM_RADIATION",
            "RX_SUMM_CHEMO", "RX_SUMM_HORMONE", "RX_SUMM_IMMUNOTHERAPY",
            "RX_SUMM_SYSTEMIC_SUR_SEQ", "RX_SUMM_TREATMENT_STATUS"
        ]
        
        available_cols = [col for col in treatment_cols if col in self._df.columns]
        
        if available_cols:
            self._df = self._df.select(available_cols)
        else:
            raise ValueError("No treatment columns found")
        
        return self
    
    def select_outcomes(self) -> "NCDBQuery":
        """Select outcome variables."""
        outcome_cols = [
            "PUF_VITAL_STATUS", "DX_LASTCONTACT_DEATH_MONTHS",
            "READM_HOSP_30_DAYS", "REASON_FOR_NO_SURGERY"
        ]
        
        available_cols = [col for col in outcome_cols if col in self._df.columns]
        
        if available_cols:
            self._df = self._df.select(available_cols)
        else:
            raise ValueError("No outcome columns found")
        
        return self
    
    def select(self, columns: Union[str, List[str]]) -> "NCDBQuery":
        """Select specific columns."""
        if isinstance(columns, str):
            columns = [columns]
        
        # Validate columns exist
        missing = set(columns) - set(self._df.columns)
        if missing:
            raise ValueError(f"Columns not found: {missing}")
        
        self._df = self._df.select(columns)
        return self
    
    def filter(self, *expressions: pl.Expr) -> "NCDBQuery":
        """Apply custom filter expressions."""
        for expr in expressions:
            self._df = self._df.filter(expr)
        return self
    
    def lazy_frame(self) -> pl.LazyFrame:
        """Get the underlying Polars LazyFrame for custom operations."""
        return self._df
    
    def collect(self, streaming: bool = False) -> pl.DataFrame:
        """
        Execute query and return results as DataFrame.
        
        Args:
            streaming: Whether to use streaming mode for large datasets
            
        Returns:
            Polars DataFrame with query results
        """
        if streaming:
            return self._df.collect(streaming=True)
        else:
            # Estimate result size and warn if large
            estimated_rows = self._df.select(pl.count()).collect().item()
            
            if estimated_rows > 1_000_000:
                import warnings
                warnings.warn(
                    f"Query will return {estimated_rows:,} rows. "
                    "Consider using streaming=True or adding more filters.",
                    UserWarning
                )
            
            return self._df.collect()
    
    def head(self, n: int = 10) -> pl.DataFrame:
        """Get first n rows."""
        return self._df.limit(n).collect()
    
    def count(self) -> int:
        """Get count of rows matching current filters."""
        return self._df.select(pl.count()).collect().item()
    
    def describe(self) -> pl.DataFrame:
        """Get basic statistics for numeric columns."""
        return self._df.collect().describe()
    
    def __repr__(self) -> str:
        """String representation."""
        return f"NCDBQuery({self.count():,} rows, {len(self._df.columns)} columns)"