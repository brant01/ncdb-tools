"""Constants for NCDB Tools."""

from typing import List, Set

# File patterns
DATA_FILE_PATTERN = "NCDBPUF_*.dat"
PARQUET_EXTENSION = ".parquet"

# Data specifications
NCDB_RECORD_LENGTH = 1032
NCDB_COLUMN_COUNT = 338

# Standard NCDB column names
YEAR_COLUMN = "YEAR_OF_DIAGNOSIS"
PRIMARY_SITE_COLUMN = "PRIMARY_SITE"
HISTOLOGY_COLUMN = "HISTOLOGY"
VITAL_STATUS_COLUMN = "PUF_VITAL_STATUS"

# Columns that should never be converted to numeric
NEVER_NUMERIC_COLUMNS: Set[str] = {
    "PUF_CASE_ID",
    "PUF_FACILITY_ID",
    "PRIMARY_SITE",
    "HISTOLOGY",
    "HISTOLOGY_ICDO3",
    "BEHAVIOR",
    "LATERALITY",
    "CLASS_OF_CASE",
    "YEAR_OF_DIAGNOSIS",
    "SEQUENCE_NUMBER",
    "FACILITY_TYPE_CD",
    "FACILITY_LOCATION_CD",
    "ZIP",
}

# Standard column groups for convenience (based on actual NCDB data)
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

# Tumor types found in the actual data
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
