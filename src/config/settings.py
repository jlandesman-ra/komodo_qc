"""
Configuration settings for the Komodo Data Quality Framework.
"""

# Database Configuration
DB_NAME = "commercial"  # Catalog Name
RAW_SCHEMA = "raw_komodo"
STAGING_SCHEMA = "staging_komodo"
DEV_SCHEMA = "dev_jlandesman"

# Refresh Months
REFRESH_MONTH = "2024-03"  # Current refresh month in YYYY-MM format
PREVIOUS_REFRESH_MONTH = "2024-02"  # Previous refresh month in YYYY-MM format

# Table Names
EVENTS_TABLES = ["Stg_mx_events", "Stg_rx_events"]
DEMO_TABLE = "Stg_patient_demo"
ENROLL_TABLE = "Stg_patient_enroll"
GEO_TABLE = "Stg_patient_geo"
MORTALITY_TABLE = "Stg_patient_mortality"
PROVIDERS_TABLE = "Stg_providers"

# Results Table
RESULTS_TABLE = f"{DB_NAME}.{DEV_SCHEMA}.rx_mx_events_dq_results"

# Expected Values
EXPECTED_PROC_CODE_TYPES = [
    "CPT", "HCPCS", "ICD-10-PCS", "ICD-9-PCS", "NDC", "LOINC", "OTHER",
]

EXPECTED_UNIT_TYPES = ["ML", "UN", "GR", "F2", "ME", "Other"]

# Quality Thresholds
NULL_PERCENT_THRESHOLD = 5.0  # Max acceptable percentage of NULLs
EXTREME_UNITS_THRESHOLD = 9999  # May need adjustment for RX quantity/days_supply
RECORD_COUNT_DEV_THRESHOLD = 0.5  # +/- 50% deviation threshold for record counts vs previous month
PATIENT_COUNT_DEV_THRESHOLD = 0.3  # +/- 30% deviation threshold for patient counts vs previous month
MORTALITY_DAYS_THRESHOLD = 60 # Number of days after death a procedure can be performed 