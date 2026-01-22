"""Constants and configuration for NCDB Tools.

This module contains variable type definitions, column names, and other
constants used throughout the package.
"""

from typing import Dict, List, Set

# =============================================================================
# File Patterns and Data Specifications
# =============================================================================

DATA_FILE_PATTERN = "NCDBPUF_*.dat"
PARQUET_EXTENSION = ".parquet"

# Fixed-width file specifications
NCDB_RECORD_LENGTH = 1032
NCDB_COLUMN_COUNT = 338

# =============================================================================
# Variables That Must Remain Strings
# =============================================================================

# Identifiers and codes that look numeric but are categorical
STRING_VARIABLES: Set[str] = {
    # Identifiers
    "PUF_CASE_ID",
    "PUF_FACILITY_ID",
    # Year fields (categorical, not continuous)
    "YEAR_OF_DIAGNOSIS",
    # Geographic codes
    "ZIP",
    "FACILITY_TYPE_CD",
    "FACILITY_LOCATION_CD",
}

# Cancer classification codes - ICD-O-3 codes that must remain strings
SITE_COLUMNS: List[str] = [
    "PRIMARY_SITE",
]

HISTOLOGY_COLUMNS: List[str] = [
    "HISTOLOGY",
    "HISTOLOGY_ICDO3",
    "BEHAVIOR",
]

# Other categorical codes
CLASSIFICATION_COLUMNS: List[str] = [
    "LATERALITY",
    "CLASS_OF_CASE",
    "SEQUENCE_NUMBER",
]

# All columns that should never be converted to numeric
NEVER_NUMERIC: Set[str] = (
    STRING_VARIABLES
    | set(SITE_COLUMNS)
    | set(HISTOLOGY_COLUMNS)
    | set(CLASSIFICATION_COLUMNS)
)

# Backward compatibility alias
NEVER_NUMERIC_COLUMNS = NEVER_NUMERIC

# =============================================================================
# Standard Column Names
# =============================================================================

YEAR_COLUMN = "YEAR_OF_DIAGNOSIS"
PRIMARY_SITE_COLUMN = "PRIMARY_SITE"
HISTOLOGY_COLUMN = "HISTOLOGY"
VITAL_STATUS_COLUMN = "PUF_VITAL_STATUS"

# =============================================================================
# Age Field Handling
# =============================================================================

AGE_FIELD: str = "AGE"
AGE_AS_INT_FIELD: str = "AGE_AS_INT"
AGE_IS_90_PLUS_FIELD: str = "AGE_IS_90_PLUS"

# =============================================================================
# Derived Column Names (created during transformation)
# =============================================================================

SITE_GROUP_FIELD: str = "SITE_GROUP"
HISTOLOGY_GROUP_FIELD: str = "HISTOLOGY_GROUP"

# =============================================================================
# Column Groups for Convenience
# =============================================================================

DEMOGRAPHIC_COLUMNS: List[str] = [
    "PUF_CASE_ID",
    "AGE",
    "SEX",
    "RACE",
    "SPANISH_HISPANIC_ORIGIN",
    "INSURANCE_STATUS",
    "CDCC_TOTAL_BEST",
    "MED_INC_QUAR_00",
    "NO_HSD_QUAR_00",
    "UR_CD_03",
    "MED_INC_QUAR_12",
    "NO_HSD_QUAR_12",
    "UR_CD_13",
    "MED_INC_QUAR_2016",
    "NO_HSD_QUAR_2016",
    "MED_INC_QUAR_2020",
    "NO_HSD_QUAR_2020",
    "CROWFLY",
]

TREATMENT_COLUMNS: List[str] = [
    "RX_SUMM_SURG_PRIM_SITE",
    "RX_SUMM_CHEMO",
    "RX_SUMM_HORMONE",
    "RX_SUMM_IMMUNOTHERAPY",
    "RX_SUMM_TRNSPLNT_ENDO",
    "RX_SUMM_SYSTEMIC_SUR_SEQ",
    "RX_SUMM_TREATMENT_STATUS",
    "RX_SUMM_SCOPE_REG_LN_SUR",
    "RX_SUMM_SURG_OTH_REGDIS",
    "RX_SUMM_SURGICAL_MARGINS",
    "RAD_LOCATION_OF_RX",
    "RX_SUMM_SURGRAD_SEQ",
    "RAD_ELAPSED_RX_DAYS",
    "REASON_FOR_NO_RADIATION",
    "REASON_FOR_NO_SURGERY",
]

OUTCOME_COLUMNS: List[str] = [
    "PUF_VITAL_STATUS",
    "DX_LASTCONTACT_DEATH_MONTHS",
    "PUF_30_DAY_MORT_CD",
    "PUF_90_DAY_MORT_CD",
    "READM_HOSP_30_DAYS",
    "PALLIATIVE_CARE",
]

# =============================================================================
# Tumor Types (from NCDB file naming convention)
# =============================================================================

TUMOR_TYPES: List[str] = [
    "BoneJont",
    "Brain",
    "CNS",
    "EyeOrbit",
    "GumOtMth",
    "HodgExtr",
    "HodgNdal",
    "Hypophar",
    "Kaposi",
    "Langerhans",
    "Larynx",
    "Lip",
    "Melanoma",
    "MouthFlr",
    "Nasal",
    "Nasophar",
    "NHLExtr",
    "NHLNdal",
    "Orophary",
    "Pharynx",
    "SalivGld",
    "SoftTiss",
    "Thyroid",
    "Tongue",
    "Tonsil",
]

# =============================================================================
# Site Group Mappings (ICD-O-3 topography codes to categories)
# =============================================================================

SITE_GROUPS: Dict[str, List[str]] = {
    "Breast": ["C50"],
    "Lung": ["C34"],
    "Colon": ["C18"],
    "Rectum": ["C19", "C20"],
    "Prostate": ["C61"],
    "Bladder": ["C67"],
    "Kidney": ["C64", "C65"],
    "Pancreas": ["C25"],
    "Liver": ["C22"],
    "Stomach": ["C16"],
    "Esophagus": ["C15"],
    "Head and Neck": ["C00", "C01", "C02", "C03", "C04", "C05", "C06",
                      "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14"],
    "Thyroid": ["C73"],
    "Melanoma": ["C44"],
}
