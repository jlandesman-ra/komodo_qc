# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC The framework performs automated data quality checks on Komodo data (Stg_mx_events and Stg_rx_events). To add a new QC check, create a check function below and add it to the `run_all_dq_checks()` function.
# MAGIC
# MAGIC ## Configuration
# MAGIC
# MAGIC - **Database and schema names**: Specifies the locations of the data tables
# MAGIC - **Table names**: Defines the tables to be checked (events, demographics, enrollment, etc.)
# MAGIC - **Expected values**: Lists of valid values for categorical fields (primarily for MX)
# MAGIC - **Thresholds**: Tolerance levels for data quality metrics
# MAGIC
# MAGIC ## Data Quality Check Categories
# MAGIC
# MAGIC ### 1. Completeness
# MAGIC
# MAGIC Validates that required fields are populated and assesses the percentage of NULL values in optional fields.
# MAGIC
# MAGIC Key metrics:
# MAGIC - NULL count for critical fields (medical_event_id/pharmacy_event_id, patient_id, service_date/fill_date, procedure_code/diagnosis_code, etc.)
# MAGIC - Percentage of NULL values in optional fields
# MAGIC - Logical completeness (e.g., procedure_code_type should be NULL when procedure_code is NULL - MX only)
# MAGIC
# MAGIC ### 2. Uniqueness
# MAGIC
# MAGIC Ensures that primary keys (medical_event_id/pharmacy_event_id) are unique within their context. **NOTE: Original code mentioned potential need to update to composite keys.**
# MAGIC
# MAGIC Key metrics:
# MAGIC - Duplicate count for primary event ID values
# MAGIC
# MAGIC ### 3. Validity
# MAGIC
# MAGIC Validates that field values conform to business rules and expected formats.
# MAGIC
# MAGIC Key metrics:
# MAGIC - Service/fill dates not in the future or ancient
# MAGIC - Procedure code types match expected values (MX only)
# MAGIC - NDC codes conform to expected format (11 digits)
# MAGIC - Units/Quantity are positive and not extreme
# MAGIC - Unit types match expected values (MX only)
# MAGIC - NPI numbers conform to expected format (10 digits)
# MAGIC - Fill date >= prescription written date (RX only)
# MAGIC
# MAGIC ### 4. Consistency
# MAGIC
# MAGIC Verifies logical relationships within and between tables.
# MAGIC
# MAGIC Key metrics:
# MAGIC - Service date <= service_to_date (MX only)
# MAGIC - Fill date >= prescription written date (RX only - added here, also checked in validity)
# MAGIC - Referential integrity between events and patient demographics
# MAGIC - Referential integrity between events and provider data
# MAGIC - Events occur during enrollment periods
# MAGIC - Events do not occur after patient death date
# MAGIC
# MAGIC ### 5. Distribution
# MAGIC
# MAGIC Analyzes the demographic and geographic distribution of data.
# MAGIC
# MAGIC Key metrics:
# MAGIC - Gender distribution of events
# MAGIC - Age group distribution of events
# MAGIC - Geographic distribution by state
# MAGIC
# MAGIC ### 6. Temporal
# MAGIC
# MAGIC Compares patient populations across time periods.
# MAGIC
# MAGIC Key metrics:
# MAGIC - Patient overlap between current and previous month
# MAGIC - New patient percentage
# MAGIC - Patient retention rate
# MAGIC - Significant changes in patient counts for specific medications (NDCs)
# MAGIC
# MAGIC ### 7. Volume
# MAGIC
# MAGIC Compares data volumes between current and previous months.
# MAGIC
# MAGIC Key metrics:
# MAGIC - Record count changes
# MAGIC - Distinct patient count changes
# MAGIC - Distinct procedure code / NDC11 count changes
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ### Components
# MAGIC
# MAGIC 1. **Helper Functions**:
# MAGIC    - `get_table()`: Loads data from specified tables
# MAGIC    - `save_results()`: Persists check results to a target table
# MAGIC    - `create_result_row()`: Creates standardized result records
# MAGIC    - `_combine_results()`: Combines multiple result DataFrames
# MAGIC    - `run_check()`: Executes checks with error handling
# MAGIC
# MAGIC 2. **Check Functions**:
# MAGIC    - Each category has a dedicated function (e.g., `check_completeness()`, `check_validity()`)
# MAGIC    - Functions accept standard parameters (spark session, refresh month, database info)
# MAGIC    - Returns results in a standardized DataFrame format
# MAGIC
# MAGIC 3. **Orchestrator**:
# MAGIC    - `run_all_dq_checks()`: Main entry point that executes all checks
# MAGIC    - `main()`: Determines refresh months and launches the process
# MAGIC
# MAGIC ### Result Schema
# MAGIC
# MAGIC Each check result is recorded with:
# MAGIC - run_id
# MAGIC - run_start_time
# MAGIC - events_table_name
# MAGIC - refresh_month
# MAGIC - check_category
# MAGIC - check_name
# MAGIC - metric_name
# MAGIC - metric_value
# MAGIC - status
# MAGIC - details
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC The framework can be executed by calling the `main()` function, which:
# MAGIC 1. Determines the current and previous refresh months
# MAGIC 2. Validates the existence of data for comparison
# MAGIC 3. Executes the appropriate checks for each table in `EVENTS_TABLES`
# MAGIC 4. Saves results to the designated results table
# MAGIC
# MAGIC ## Extending the Framework
# MAGIC
# MAGIC To add new checks:
# MAGIC 1. Create a new check function following the existing pattern (accepting `spark`, `run_id`, `run_start_time`, `events_table_name`, `current_refresh_month`, `db_name`, `raw_schema`, and optionally `previous_refresh_month`).
# MAGIC 2. Ensure the function calls `create_result_row` with all required arguments.
# MAGIC 3. Add the function call to the appropriate list (`standard_checks` or `temporal_checks`) in `run_all_dq_checks()`.
# MAGIC
# MAGIC ## Dependencies
# MAGIC
# MAGIC - PySpark SQL
# MAGIC - Python datetime module
# MAGIC - uuid
# MAGIC - dateutil (optional, for more robust month calculations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config and Helper Functions

# COMMAND ----------

import datetime
import uuid # Import uuid library
from typing import Optional, List, Any, Dict
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession, Row
# Removed StorageLevel as persist/cache are removed
# from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import (
    abs as spark_abs,
    avg as spark_avg,
    broadcast,
    col,
    count,
    countDistinct,
    current_date,
    date_format,
    datediff,
    expr,
    isnull, # Added isnull
    length,
    lit,
    max as spark_max,
    min as spark_min,
    regexp_replace,
    round as spark_round,
    sum as spark_sum,
    upper,
    when,
    year,
    coalesce # Added coalesce
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --- Configuration ---

DB_NAME = "commercial" # Catalog Name
RAW_SCHEMA = "raw_komodo"
DEV_SCHEMA = "dev_jlandesman"
# List of event tables to process
EVENTS_TABLES = ["Stg_mx_events", "Stg_rx_events"] # Updated
# Other relevant tables
DEMO_TABLE = "Stg_patient_demo"
ENROLL_TABLE = "Stg_patient_enroll"
GEO_TABLE = "Stg_patient_geo"
MORTALITY_TABLE = "Stg_patient_mortality"
PROVIDERS_TABLE = "Stg_providers"
# Ensure the results table path includes the catalog name
RESULTS_TABLE = f"{DB_NAME}.{DEV_SCHEMA}.mx_events_dq_results" # Consider renaming if storing RX results too

# Define expected values (Primarily for MX)
# Example list - Use upper case for comparison internally
EXPECTED_PROC_CODE_TYPES = [
    "CPT", "HCPCS", "ICD-10-PCS", "ICD-9-PCS", "NDC", "LOINC", "OTHER",
]
# Define expected unit types (add actual expected values here - MX only)
EXPECTED_UNIT_TYPES = ["ML", "UN", "GR", "F2", "ME", "Other"] # Example values

# NPI Columns will be determined dynamically in functions based on table_name

# Define thresholds
NULL_PERCENT_THRESHOLD = 5.0  # Max acceptable percentage of NULLs
EXTREME_UNITS_THRESHOLD = 9999 # May need adjustment for RX quantity/days_supply
RECORD_COUNT_DEV_THRESHOLD = (
    0.5  # +/- 50% deviation threshold for record counts vs previous month
)
PATIENT_COUNT_DEV_THRESHOLD = (
    0.3  # +/- 30% deviation threshold for patient counts vs previous month
)
# Define expected values for RX checks (if available)
# EXPECTED_TRANSACTION_STATUSES = [...]
# EXPECTED_DAW_CODES = [...]

# --- Result Schema (Final schema for the output table) ---
# This schema must match the target table and the output of create_result_row
result_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_start_time", TimestampType(), False),
        StructField("events_table_name", StringType(), False),
        StructField("refresh_month", StringType(), False),
        StructField("check_category", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_value", StringType(), True), # Using String for flexibility
        StructField("status", StringType(), False), # e.g., PASS, FAIL, WARN, INFO, ERROR, SKIPPED
        StructField("details", StringType(), True),
    ]
)


# --- Helper Functions ---

def get_table(
    spark: SparkSession, db: str, schema: str, table: str
) -> DataFrame:
    """Loads a table into a DataFrame using a three-part name."""
    full_table_name = f"{db}.{schema}.{table}"
    print(f"Loading table: {full_table_name}")
    try:
        return spark.table(full_table_name)
    except Exception as e:
        print(f"Error loading table {full_table_name}: {e}")
        raise e

def save_results(spark: SparkSession, results_df: DataFrame):
    """Appends results DataFrame to the target results table."""
    if results_df is None:
        print("No results DataFrame provided to save.")
        return
    if results_df.isEmpty():
        print("No results to save (DataFrame is empty).")
        return

    # Ensure the DataFrame schema matches the target table schema before saving
    # Select columns in the order defined by result_schema
    try:
        results_df_ordered = results_df.select(result_schema.fieldNames())
    except Exception as schema_err:
         print(f"Error matching DataFrame schema to target schema: {schema_err}")
         print("DataFrame Schema:")
         results_df.printSchema()
         print("Target Schema:")
         print(result_schema)
         # Decide how to handle: raise error, log, skip save
         print("Skipping save due to schema mismatch.")
         return # Or raise schema_err


    row_count = results_df_ordered.count() # Count only if we know we have rows
    print(f"Appending {row_count} rows to {RESULTS_TABLE}...")
    try:
        results_df_ordered.write.format("delta").mode("append").saveAsTable(
            RESULTS_TABLE # Use the fully qualified name
        )
        print("Save successful.")
    except Exception as e:
        print(f"Error saving results to {RESULTS_TABLE}: {e}")
        raise e

def create_result_row(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str,
    refresh_month: str,
    category: str,
    check: str,
    metric: str,
    value: Any, # Accept various types for value
    status: str,
    detail: Optional[str] = None,
) -> DataFrame:
    """Creates a DataFrame row for a single check result, matching the global result_schema."""
    # Ensure value is stringified appropriately
    metric_value_str = "N/A" # Default
    if value is not None:
        # Specific formatting for percentages if value is float/double and metric name suggests it
        # Check if metric name contains '%' or 'percentage' or 'pct'
        is_percentage_metric = any(indicator in metric.lower() for indicator in ['%', 'percentage', 'pct'])
        if isinstance(value, (float, int)) and is_percentage_metric:
            # Format as signed percentage, e.g., +10.50%, -5.00%
            metric_value_str = f"{value:+.2f}%"
        else:
            metric_value_str = str(value)

    # Create data tuple matching the result_schema
    data = [(run_id, run_start_time, events_table_name, refresh_month, category, check, metric, metric_value_str, status, detail)]
    # Create DataFrame using the global result_schema
    return spark.createDataFrame(data, schema=result_schema)


def _combine_results(results_list: list[Optional[DataFrame]]) -> Optional[DataFrame]:
    """Combines a list of result DataFrames into one, ensuring schema consistency."""
    if not results_list:
        return None
    # Filter out None or empty DataFrames first
    valid_results = [df for df in results_list if df is not None and not df.isEmpty()]
    if not valid_results:
        return None
    if len(valid_results) == 1:
        # Ensure the single valid DataFrame conforms to the schema
        try:
            return valid_results[0].select(result_schema.fieldNames())
        except Exception as e:
            print(f"Schema error in single result DataFrame: {e}")
            return None # Or handle error appropriately

    # Ensure all DataFrames have the same schema (matching result_schema) before union
    first_df = valid_results[0]
    try:
        # Select columns in the correct order for the first DataFrame
        final_results_df = first_df.select(result_schema.fieldNames())
    except Exception as e:
        print(f"Schema error in first DataFrame for combining: {e}")
        return None # Or handle error

    # Iterate through the rest of the list and union
    for df in valid_results[1:]:
        try:
            # Ensure subsequent DataFrames also conform and select columns
            df_ordered = df.select(result_schema.fieldNames())
            final_results_df = final_results_df.unionByName(df_ordered) # Use unionByName for robustness
        except Exception as e:
             print(f"Schema mismatch detected during combine. Expected: {result_schema.simpleString()}, Error: {e}. Skipping DataFrame.")
             # Optionally log the problematic DataFrame's schema: df.printSchema()
             continue # Skip this DataFrame

    return final_results_df


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Completeness

# COMMAND ----------

def check_completeness(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str, # Added parameter
    current_refresh_month: str,
    db_name: str,
    raw_schema: str
) -> Optional[DataFrame]:
    """
    Performs data completeness checks on a specific events table for a specific month.
    Handles differences between Stg_mx_events and Stg_rx_events.
    Removed caching/persist.
    """
    print(f"Starting Completeness checks for table '{events_table_name}', month: {current_refresh_month}")
    check_category = "Completeness"

    try:
        # Fetch the events data for the specified table and month
        events_df = get_table(
            spark, db_name, raw_schema, events_table_name # Use the passed table name
        ).filter(
            F.date_format(F.col("kh_refresh_date"), "yyyy-MM")
            == current_refresh_month
        )

        # Removed persist/cache
        # events_df.persist(StorageLevel.MEMORY_AND_DISK)

        total_records = events_df.count()

        if total_records == 0:
            print(f"No records found for table '{events_table_name}', refresh month {current_refresh_month}. Skipping completeness checks.")
            # Removed unpersist
            # events_df.unpersist()
            return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Record Count", "Total Records", 0, "INFO",
                                     "No records found for this table and month, skipping checks.")

        results = []

        # --- Check 1: Mandatory Non-NULL Columns (Table Specific) ---
        if events_table_name == "Stg_rx_events":
            # As requested: pharmacy_event_id, patient_id, fill_date, diagnosis_code, pharmacy_npi
            null_check_cols = ["pharmacy_event_id", "patient_id", "fill_date", "diagnosis_code", "pharmacy_npi"]
        else: # Default to Stg_mx_events
            null_check_cols = ["medical_event_id", "patient_id", "service_date", "procedure_code", "billing_npi"]

        for col_name in null_check_cols:
            if col_name not in events_df.columns:
                print(f"Warning: Mandatory column '{col_name}' not found in table '{events_table_name}'. Skipping NULL check.")
                results.append(create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                                 check_category, f"{col_name} Not NULL", "Check Status", "SKIPPED", "WARN",
                                                 f"Mandatory column '{col_name}' not found in table."))
                continue

            null_count = events_df.filter(F.col(col_name).isNull()).count()
            status = "PASS" if null_count == 0 else "FAIL"
            detail = f"{null_count} NULL values found out of {total_records} records."
            results.append(create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                             check_category, f"{col_name} Not NULL", "Null Count", null_count, status, detail))

        # --- Check 2: Conditional Completeness Check (MX Only) ---
        if events_table_name == "Stg_mx_events":
            if "procedure_code" in events_df.columns and "procedure_code_type" in events_df.columns:
                proc_code_null_type_not_null = events_df.filter(F.col("procedure_code").isNull() & F.col("procedure_code_type").isNotNull()).count()
                status = "PASS" if proc_code_null_type_not_null == 0 else "WARN"
                detail = f"{proc_code_null_type_not_null} records have NULL procedure_code but non-NULL procedure_code_type."
                results.append(create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                                 check_category, "Procedure Code Type Consistency", "Inconsistent Count",
                                                 proc_code_null_type_not_null, status, detail))
            else:
                 print(f"Warning: Skipping Procedure Code Type Consistency check for {events_table_name} as 'procedure_code' or 'procedure_code_type' not found.")
        else:
            print(f"Skipping Procedure Code Type Consistency check for {events_table_name} (MX specific).")


        # --- Check 3: Percentage-Based NULL Checks (Table Specific) ---
        if events_table_name == "Stg_rx_events":
            percent_null_check_cols = ["ndc11", # Moved from mandatory based on prior logic
                                        "days_supply", "quantity", "prescriber_npi",
                                        "brand_name", "generic_name", "route", "transaction_result",
                                        "reject_codes", "transaction_number", "transaction_status",
                                        "fill_number", "number_of_refills_authorized", "daw_code",
                                        "date_prescription_written", "primary_kh_plan_id",
                                        "secondary_kh_plan_id", "patient_responsibility", "patient_oop"]
        else: # Default to Stg_mx_events
            percent_null_check_cols = ["service_to_date", "units", "unit_type", "rendering_npi", "referring_npi", "ndc11"] # Keep ndc11 here for MX

        for col_name in percent_null_check_cols:
            if col_name not in events_df.columns:
                print(f"Warning: Optional column '{col_name}' not found in table '{events_table_name}'. Skipping NULL percentage check.")
                results.append(create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                                 check_category, f"Low NULL % for {col_name}", "Check Status", "SKIPPED", "WARN",
                                                 f"Optional column '{col_name}' not found in table."))
                continue

            null_count = events_df.filter(F.col(col_name).isNull()).count()
            null_percent = (null_count / total_records) * 100 if total_records > 0 else 0
            status = "PASS" if null_percent <= NULL_PERCENT_THRESHOLD else "WARN"
            detail = f"{null_count} NULLs ({null_percent:.2f}%) found. Threshold: {NULL_PERCENT_THRESHOLD:.2f}%"
            # Pass the raw percentage for formatting by create_result_row
            results.append(create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                             check_category, f"Low NULL % for {col_name}", "Null Percentage",
                                             null_percent, status, detail)) # Pass float value

        # Removed unpersist
        # events_df.unpersist()
        return _combine_results(results)

    except Exception as e:
        print(f"Error during completeness check for {events_table_name}: {e}")
        # Ensure unpersist happens even on error if df exists - Removed
        # if 'events_df' in locals() and events_df.is_cached:
        #     events_df.unpersist()
        # Return an error row
        return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                 check_category, "Execution Status", "Status", "ERROR", "FAIL",
                                 f"Error executing check: {str(e)[:500]}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Uniqueness

# COMMAND ----------

def check_uniqueness(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str, # Added parameter
    current_refresh_month: str,
    db_name: str,
    raw_schema: str
) -> DataFrame | None:
    """
    Checks for duplicate primary event IDs within a specific month
    in the specified events table (medical_event_id or pharmacy_event_id).
    Removed caching/persist.
    """
    print(f"Starting Uniqueness checks for table '{events_table_name}', month: {current_refresh_month}")
    check_category = "Uniqueness"

    # Determine primary key column based on table name
    if events_table_name == "Stg_rx_events":
        primary_key_col = "pharmacy_event_id"
    else: # Default to Stg_mx_events
        primary_key_col = "medical_event_id"

    try:
        # Retrieve the events table data
        events_df = get_table(
            spark, db_name, raw_schema, events_table_name # Use passed table name
        )

        # Filter the DataFrame to include only records for the current refresh month
        events_df_filtered = events_df.filter(
            date_format(col("kh_refresh_date"), "yyyy-MM") == current_refresh_month
        )

        # Check if the determined primary key column exists
        if primary_key_col not in events_df_filtered.columns:
             print(f"Warning: Primary key column '{primary_key_col}' not found in table '{events_table_name}'. Skipping uniqueness check.")
             return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Primary Key Uniqueness", "Check Status", "SKIPPED", "WARN",
                                     f"Primary key column '{primary_key_col}' not found.")

        # Removed persist/cache
        # events_df_filtered.persist(StorageLevel.MEMORY_AND_DISK)

        total_records = events_df_filtered.count()

        if total_records == 0:
            print(f"No records found for table '{events_table_name}', refresh month {current_refresh_month}. Skipping uniqueness checks.")
            # Removed unpersist
            # events_df_filtered.unpersist()
            # Return an INFO row indicating skipped check due to no data
            return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Primary Key Uniqueness", "Check Status", "SKIPPED", "INFO",
                                     "No records found for this table and month.")

        # Group by the primary key and count occurrences
        # Filter to find primary keys that appear more than once (duplicates)
        duplicate_counts = (
            events_df_filtered.groupBy(primary_key_col)
            .count()
            .filter(col("count") > 1)
        )

        # Count the number of distinct primary keys that have duplicates
        duplicate_pk_count = duplicate_counts.count()

        # Determine the status of the check based on duplicate count
        # Note: Consider composite keys if single ID is not truly unique per spec
        status = "PASS" if duplicate_pk_count == 0 else "FAIL"

        # Create a detailed message summarizing the findings
        detail = (
            f"{duplicate_pk_count} duplicate '{primary_key_col}' values found within "
            f"refresh month {current_refresh_month} ({total_records} total records)."
        )

        # Create a DataFrame row with the results of the uniqueness check
        result_df = create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month,
            check_category, f"Primary Key Uniqueness ({primary_key_col})", "Duplicate PK Count",
            duplicate_pk_count, status, detail
        )

        # Removed unpersist
        # events_df_filtered.unpersist()
        return result_df

    except Exception as e:
        print(f"Error during uniqueness check for {events_table_name}: {e}")
        # Removed unpersist check
        # if 'events_df_filtered' in locals() and events_df_filtered.is_cached:
        #     events_df_filtered.unpersist()
        return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                 check_category, "Execution Status", "Status", "ERROR", "FAIL",
                                 f"Error executing check: {str(e)[:500]}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Validity

# COMMAND ----------

def check_validity(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str, # Added parameter
    current_refresh_month: str,
    db_name: str,
    raw_schema: str
) -> DataFrame | None:
    """
    Performs various data validity checks, adapting logic for Stg_mx_events and Stg_rx_events.
    Uses single-pass aggregation where possible.
    Removed caching/persist.
    """
    print(f"Starting Validity checks for table '{events_table_name}', month: {current_refresh_month}")
    check_category = "Validity"

    try:
        # Retrieve the events table data
        events_df = get_table(
            spark, db_name, raw_schema, events_table_name # Use passed table name
        )

        # Filter the DataFrame to include only records for the current refresh month.
        events_df_filtered = events_df.filter(
            date_format(col("kh_refresh_date"), "yyyy-MM") == current_refresh_month
        )

        # --- Define Columns and Checks based on Table ---
        results = [] # List to hold result DataFrames for each check
        all_agg_exprs = [count("*").alias("total_count")]
        required_cols = {"kh_refresh_date"} # Base required column
        npi_cols_to_check = []

        # Table-Specific Configurations
        if events_table_name == "Stg_rx_events":
            primary_date_col = "fill_date"
            required_cols.update({"pharmacy_event_id", "patient_id", primary_date_col, "ndc11", "pharmacy_npi", "prescriber_npi", "quantity", "days_supply", "date_prescription_written"})
            npi_cols_to_check = ["pharmacy_npi", "prescriber_npi"]

            # RX Specific Aggregation Conditions
            cond_future_date = col(primary_date_col) > current_date()
            cond_past_date = year(col(primary_date_col)) < 1900
            cond_invalid_ndc_format = col("ndc11").isNotNull() & (length(regexp_replace(col("ndc11"), "-", "")) != 11)
            cond_negative_quantity = col("quantity").isNotNull() & (col("quantity") <= 0)
            cond_extreme_quantity = col("quantity").isNotNull() & (col("quantity") > EXTREME_UNITS_THRESHOLD)
            cond_negative_days_supply = col("days_supply").isNotNull() & (col("days_supply") <= 0)
            # Note: Add extreme check for days_supply if needed, e.g., > 365?
            # cond_extreme_days_supply = col("days_supply").isNotNull() & (col("days_supply") > 365)
            cond_fill_before_written = col("date_prescription_written").isNotNull() & (col(primary_date_col) < col("date_prescription_written"))

            all_agg_exprs.extend([
                spark_sum(when(cond_future_date, 1).otherwise(0)).alias("future_date_count"),
                spark_sum(when(cond_past_date, 1).otherwise(0)).alias("past_date_count"),
                spark_sum(when(cond_invalid_ndc_format, 1).otherwise(0)).alias("invalid_ndc_format_count"),
                spark_sum(when(cond_negative_quantity, 1).otherwise(0)).alias("negative_quantity_count"),
                spark_sum(when(cond_extreme_quantity, 1).otherwise(0)).alias("extreme_quantity_count"),
                spark_sum(when(cond_negative_days_supply, 1).otherwise(0)).alias("negative_days_supply_count"),
                # spark_sum(when(cond_extreme_days_supply, 1).otherwise(0)).alias("extreme_days_supply_count"),
                spark_sum(when(cond_fill_before_written, 1).otherwise(0)).alias("fill_before_written_count"),
            ])
            # Add checks for transaction_status, daw_code etc. if expected values are known (would likely require separate groupBy/agg)

        else: # Default to Stg_mx_events
            primary_date_col = "service_date"
            required_cols.update({"medical_event_id", "patient_id", primary_date_col, "service_to_date", "procedure_code_type", "ndc11", "units", "unit_type", "billing_npi", "rendering_npi", "referring_npi"})
            npi_cols_to_check = ["billing_npi", "rendering_npi", "referring_npi"]

            # MX Specific Aggregation Conditions
            cond_future_date = col(primary_date_col) > current_date()
            cond_past_date = year(col(primary_date_col)) < 1900
            cond_future_service_to_date = col("service_to_date").isNotNull() & (col("service_to_date") > current_date())
            cond_invalid_proc_code_type = col("procedure_code_type").isNotNull() & ~upper(col("procedure_code_type")).isin(EXPECTED_PROC_CODE_TYPES)
            cond_invalid_ndc_format = col("ndc11").isNotNull() & (length(regexp_replace(col("ndc11"), "-", "")) != 11) # Assuming ndc11 also relevant for MX
            cond_negative_units = col("units").isNotNull() & (col("units") <= 0)
            cond_extreme_units = col("units").isNotNull() & (col("units") > EXTREME_UNITS_THRESHOLD)
            cond_invalid_unit_type = col("unit_type").isNotNull() & ~upper(col("unit_type")).isin(EXPECTED_UNIT_TYPES)

            all_agg_exprs.extend([
                spark_sum(when(cond_future_date, 1).otherwise(0)).alias("future_date_count"),
                spark_sum(when(cond_past_date, 1).otherwise(0)).alias("past_date_count"),
                spark_sum(when(cond_future_service_to_date, 1).otherwise(0)).alias("future_service_to_date_count"),
                spark_sum(when(cond_invalid_proc_code_type, 1).otherwise(0)).alias("invalid_proc_code_type_count"),
                spark_sum(when(cond_invalid_ndc_format, 1).otherwise(0)).alias("invalid_ndc_format_count"),
                spark_sum(when(cond_negative_units, 1).otherwise(0)).alias("negative_units_count"),
                spark_sum(when(cond_extreme_units, 1).otherwise(0)).alias("extreme_units_count"),
                spark_sum(when(cond_invalid_unit_type, 1).otherwise(0)).alias("invalid_unit_type_count"),
            ])


        # --- Check Column Existence ---
        missing_cols = [c for c in required_cols if c not in events_df_filtered.columns]
        if missing_cols:
            print(f"Warning: Missing essential columns for validity checks in {events_table_name}: {missing_cols}. Skipping checks.")
            return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Column Existence", "Check Status", "SKIPPED", "WARN",
                                     f"Missing columns: {', '.join(missing_cols)}")

        # --- Add NPI Aggregations Dynamically ---
        for npi_col in npi_cols_to_check:
            alias = f"invalid_{npi_col}_format_count"
            # NPI check: 10 digits and only numeric characters
            condition = col(npi_col).isNotNull() & ((length(col(npi_col)) != 10) | (col(npi_col).rlike("[^0-9]")))
            all_agg_exprs.append(spark_sum(when(condition, 1).otherwise(0)).alias(alias))

        # --- Execute Single Pass Aggregation ---
        print(f"Performing single-pass aggregation for {events_table_name}...")
        aggregated_counts_row: Optional[Row] = events_df_filtered.agg(*all_agg_exprs).first()

        if aggregated_counts_row is None or aggregated_counts_row["total_count"] == 0:
             print(f"No records found for table '{events_table_name}', refresh month {current_refresh_month}. Skipping validity checks.")
             return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Record Count", "Total Records", 0, "INFO",
                                     "No records found for this table and month, skipping checks.")

        aggregated_counts: Dict[str, Any] = aggregated_counts_row.asDict()
        total_records = aggregated_counts["total_count"]
        print(f"Aggregation results for {events_table_name}: {aggregated_counts}")


        # --- Create Result Rows using Aggregated Counts ---

        # Check 1: Primary Date not in the Future
        count_val = aggregated_counts["future_date_count"]
        status = "PASS" if count_val == 0 else "FAIL"
        detail = f"{count_val} of {total_records} records have {primary_date_col} in the future."
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            f"{primary_date_col.replace('_',' ').title()} Not Future", "Future Date Count", count_val, status, detail
        ))

        # Check 2: Primary Date not Ancient
        count_val = aggregated_counts["past_date_count"]
        status = "PASS" if count_val == 0 else "WARN" # Warn for potentially old but valid dates
        detail = f"{count_val} of {total_records} records have {primary_date_col} before 1900."
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            f"{primary_date_col.replace('_',' ').title()} Not Ancient", "Ancient Date Count", count_val, status, detail
        ))

        # Check 3: NDC11 Format
        count_val = aggregated_counts["invalid_ndc_format_count"]
        status = "PASS" if count_val == 0 else "WARN"
        detail = (f"{count_val} of {total_records} records have ndc11 values not conforming to 11 digits (hyphens ignored).")
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "NDC11 Format (11 digits)", "Invalid Format Count", count_val, status, detail
        ))

        # Check 4: NPI Format (Loop through relevant NPI columns)
        for npi_col in npi_cols_to_check:
            alias = f"invalid_{npi_col}_format_count"
            count_val = aggregated_counts[alias]
            status = "PASS" if count_val == 0 else "WARN"
            detail = (f"{count_val} of {total_records} records have {npi_col} values not conforming to 10 numeric digits.")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                f"{npi_col.replace('_',' ').title()} Format (10 digits)", "Invalid Format Count", count_val, status, detail
            ))

        # --- MX Specific Results ---
        if events_table_name != "Stg_rx_events":
            # Service To Date not in the Future
            count_val = aggregated_counts["future_service_to_date_count"]
            status = "PASS" if count_val == 0 else "FAIL"
            detail = f"{count_val} of {total_records} records have service_to_date in the future."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Service To Date Not Future", "Future Date Count", count_val, status, detail
            ))
            # Procedure Code Type Validity
            count_val = aggregated_counts["invalid_proc_code_type_count"]
            status = "PASS" if count_val == 0 else "WARN"
            expected_types_str = ", ".join(sorted(list(EXPECTED_PROC_CODE_TYPES)))
            detail = (f"{count_val} of {total_records} records have unexpected procedure_code_type values. "
                      f"Expected (case-insensitive): {expected_types_str}")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Procedure Code Type Values", "Invalid Type Count", count_val, status, detail
            ))
            # Units Positive
            count_val = aggregated_counts["negative_units_count"]
            status = "PASS" if count_val == 0 else "FAIL"
            detail = f"{count_val} of {total_records} records have non-positive units values."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Units Positive", "Non-Positive Count", count_val, status, detail
            ))
            # Units Not Extreme
            count_val = aggregated_counts["extreme_units_count"]
            status = "PASS" if count_val == 0 else "WARN"
            detail = f"{count_val} of {total_records} records have units > {EXTREME_UNITS_THRESHOLD}."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Units Not Extreme", "Extreme Units Count", count_val, status, detail
            ))
            # Unit Type Validity
            count_val = aggregated_counts["invalid_unit_type_count"]
            status = "PASS" if count_val == 0 else "WARN"
            expected_units_str = ", ".join(sorted(list(EXPECTED_UNIT_TYPES)))
            detail = (f"{count_val} of {total_records} records have unexpected unit_type values. "
                      f"Expected (case-insensitive): {expected_units_str}")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Unit Type Values", "Invalid Type Count", count_val, status, detail
            ))

        # --- RX Specific Results ---
        if events_table_name == "Stg_rx_events":
            # Quantity Positive
            count_val = aggregated_counts["negative_quantity_count"]
            status = "PASS" if count_val == 0 else "FAIL"
            detail = f"{count_val} of {total_records} records have non-positive quantity values."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Quantity Positive", "Non-Positive Count", count_val, status, detail
            ))
            # Quantity Not Extreme
            count_val = aggregated_counts["extreme_quantity_count"]
            status = "PASS" if count_val == 0 else "WARN"
            detail = f"{count_val} of {total_records} records have quantity > {EXTREME_UNITS_THRESHOLD}."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Quantity Not Extreme", "Extreme Quantity Count", count_val, status, detail
            ))
            # Days Supply Positive
            count_val = aggregated_counts["negative_days_supply_count"]
            status = "PASS" if count_val == 0 else "FAIL"
            detail = f"{count_val} of {total_records} records have non-positive days_supply values."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Days Supply Positive", "Non-Positive Count", count_val, status, detail
            ))
            # Fill Date vs Written Date
            count_val = aggregated_counts["fill_before_written_count"]
            status = "PASS" if count_val == 0 else "WARN" # WARN due to potential data issues with written date
            detail = f"{count_val} of {total_records} records have fill_date < date_prescription_written."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Fill Date >= Prescription Written Date", "Invalid Order Count", count_val, status, detail
            ))

        # --- Combine Results ---
        return _combine_results(results)

    except Exception as e:
        print(f"Error during validity check for {events_table_name}: {e}")
        return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                 check_category, "Execution Status", "Status", "ERROR", "FAIL",
                                 f"Error executing check: {str(e)[:500]}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Consistency

# COMMAND ----------

def check_consistency(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str, # Added parameter
    current_refresh_month: str,
    db_name: str,
    raw_schema: str
) -> DataFrame | None:
    """
    Performs data consistency checks, adapting logic for Stg_mx_events and Stg_rx_events.
    Removed caching/persist.
    """
    print(f"Starting Consistency checks for table '{events_table_name}', month: {current_refresh_month}")
    check_category = "Consistency"
    results = [] # List to hold result DataFrames

    try:
        # --- 1. Load and Prepare Data ---
        # Determine table-specific columns
        if events_table_name == "Stg_rx_events":
            primary_date_col = "fill_date"
            event_key_col = "pharmacy_event_id"
            npi_cols_in_event_table = ["pharmacy_npi", "prescriber_npi"]
            optional_cols = ["date_prescription_written"]
        else: # Default to Stg_mx_events
            primary_date_col = "service_date"
            event_key_col = "medical_event_id"
            npi_cols_in_event_table = ["billing_npi", "rendering_npi", "referring_npi"]
            optional_cols = ["service_to_date"]

        # Retrieve and filter the events table data
        base_events_df = get_table(spark, db_name, raw_schema, events_table_name) # Use passed table name

        # Check for essential columns before filtering and selecting
        essential_cols = [event_key_col, "patient_id", primary_date_col, "kh_refresh_date"]
        if not all(c in base_events_df.columns for c in essential_cols):
             missing_core = [c for c in essential_cols if c not in base_events_df.columns]
             print(f"Error: Missing core columns ({', '.join(missing_core)}) in {events_table_name}. Aborting consistency checks.")
             return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Core Column Existence", "Check Status", "ERROR", "FAIL",
                                     f"Missing core columns: {', '.join(missing_core)}")

        events_df = base_events_df.filter(date_format(col("kh_refresh_date"), "yyyy-MM") == current_refresh_month)

        # Select essential columns + optional ones + NPIs if they exist
        select_cols_expr = ["ev." + c for c in [event_key_col, "patient_id", primary_date_col]]
        for c in optional_cols:
            if c in events_df.columns:
                select_cols_expr.append("ev." + c)
        for c in npi_cols_in_event_table:
             if c in events_df.columns:
                select_cols_expr.append("ev." + c)

        events_df = events_df.alias("ev").selectExpr(*select_cols_expr)
        events_df = events_df.alias("ev") # Re-apply alias just in case

        # Removed persist/cache
        # events_df.persist(StorageLevel.MEMORY_AND_DISK)

        if events_df.isEmpty():
            print(f"No records found for table '{events_table_name}', refresh month {current_refresh_month}. Skipping consistency checks.")
            # Removed unpersist
            # events_df.unpersist()
            return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Record Count", "Total Records", 0, "INFO",
                                     "No records found for this table and month, skipping checks.")

        total_event_records = events_df.count()

        # --- Load Supporting Data (Common) ---
        latest_demo_per_patient = (
            get_table(spark, db_name, raw_schema, DEMO_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= current_refresh_month)
            .withColumn("rn", expr(f"row_number() OVER (PARTITION BY patient_id ORDER BY kh_refresh_date DESC)"))
            .filter(col("rn") == 1)
            .select("patient_id").distinct()
            .alias("dm")
        )
        latest_providers = (
            get_table(spark, db_name, raw_schema, PROVIDERS_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= current_refresh_month)
            .withColumn("rn", expr(f"row_number() OVER (PARTITION BY npi ORDER BY kh_refresh_date DESC)"))
            .filter(col("rn") == 1)
            .select("npi").distinct()
            .alias("pv")
        )
        enroll_df = (
            get_table(spark, db_name, raw_schema, ENROLL_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= current_refresh_month)
            .select("patient_id", col("start_date").alias("enroll_start"), col("end_date").alias("enroll_end"))
            .filter(col("enroll_start").isNotNull() & col("enroll_end").isNotNull())
            .alias("en")
        )
        mortality_df = (
            get_table(spark, db_name, raw_schema, MORTALITY_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= current_refresh_month)
            .filter(col("patient_death_date").isNotNull())
            .withColumn("rn", expr(f"row_number() OVER (PARTITION BY patient_id ORDER BY kh_refresh_date DESC)"))
            .filter(col("rn") == 1)
            .select("patient_id", "patient_death_date")
            .alias("mo")
        )

        # --- 2. Perform Checks ---

        # Check 1a: Service Date Order (MX Only)
        if events_table_name != "Stg_rx_events" and "service_to_date" in events_df.columns:
            print(f"Checking Service Date Order for {events_table_name}...")
            invalid_date_order_count = events_df.filter(
                col("ev.service_to_date").isNotNull() &
                (col(f"ev.{primary_date_col}") > col("ev.service_to_date"))
            ).count()
            status = "PASS" if invalid_date_order_count == 0 else "FAIL"
            detail = f"{invalid_date_order_count} of {total_event_records} records have {primary_date_col} > service_to_date."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Service Date <= Service To Date", "Invalid Order Count", invalid_date_order_count, status, detail
            ))
        elif events_table_name != "Stg_rx_events":
            print(f"Warning: Skipping Service Date Order check as 'service_to_date' not found in {events_table_name}.")

        # Check 1b: Fill Date vs Written Date (RX Only - User Request)
        if events_table_name == "Stg_rx_events" and "date_prescription_written" in events_df.columns:
            print(f"Checking Fill Date vs Prescription Written Date for {events_table_name}...")
            invalid_fill_date_count = events_df.filter(
                col("ev.date_prescription_written").isNotNull() &
                (col(f"ev.{primary_date_col}") < col("ev.date_prescription_written"))
            ).count()
            status = "PASS" if invalid_fill_date_count == 0 else "WARN" # Warn as written date might be missing/incorrect
            detail = f"{invalid_fill_date_count} of {total_event_records} records have {primary_date_col} < date_prescription_written."
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Fill Date >= Prescription Written Date", "Invalid Order Count", invalid_fill_date_count, status, detail
            ))
        elif events_table_name == "Stg_rx_events":
             print(f"Warning: Skipping Fill Date vs Written Date check as 'date_prescription_written' not found in {events_table_name}.")


        # Check 2: Referential Integrity - Patient ID (Common Logic)
        print(f"Checking Patient ID consistency for {events_table_name}...")
        event_patient_ids = events_df.select("ev.patient_id").distinct()
        missing_patient_count = event_patient_ids.join(
            broadcast(latest_demo_per_patient), # Broadcast demo if smaller
            event_patient_ids["patient_id"] == latest_demo_per_patient["patient_id"],
            "left_anti"
        ).count()
        status = "PASS" if missing_patient_count == 0 else "FAIL"
        detail = (f"{missing_patient_count} distinct patient_ids from events table '{events_table_name}' "
                  f"(month {current_refresh_month}) are missing in the latest demographics.")
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Referential Integrity: Patient ID", "Missing Patient Count", missing_patient_count, status, detail
        ))


        # Check 3: Referential Integrity - NPIs (Common Logic, Specific Columns)
        print(f"Checking NPI consistency for {events_table_name}...")
        npi_cols_in_df = [npi for npi in npi_cols_in_event_table if npi in events_df.columns]
        if npi_cols_in_df:
            # Collect all non-null NPIs from the relevant columns into a single column DataFrame
            all_event_npis_list = []
            for npi_col in npi_cols_in_df:
                all_event_npis_list.append(events_df.select(col(f"ev.{npi_col}").alias("npi")))

            if all_event_npis_list:
                 # Union all NPI dataframes
                 event_npis_unioned = all_event_npis_list[0]
                 for df_to_union in all_event_npis_list[1:]:
                     event_npis_unioned = event_npis_unioned.unionByName(df_to_union) # Use unionByName if needed, simple union if alias is 'npi'

                 distinct_event_npis = event_npis_unioned.filter(col("npi").isNotNull()).distinct()

                 # Removed persist/cache
                 # distinct_event_npis.persist(StorageLevel.MEMORY_AND_DISK)
                 total_distinct_npis = distinct_event_npis.count()

                 if total_distinct_npis > 0:
                    missing_npi_count = distinct_event_npis.join(
                        broadcast(latest_providers), # Broadcast providers if smaller
                        distinct_event_npis["npi"] == latest_providers["npi"],
                        "left_anti"
                    ).count()
                    status = "PASS" if missing_npi_count == 0 else "WARN"
                    detail = (f"{missing_npi_count} of {total_distinct_npis} distinct NPIs from '{events_table_name}' "
                              f"(month {current_refresh_month}, cols: {', '.join(npi_cols_in_df)}) are missing in the latest providers table.")
                    results.append(create_result_row(
                        spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                        "Referential Integrity: NPIs", "Missing NPI Count", missing_npi_count, status, detail
                    ))
                 else:
                     results.append(create_result_row(
                        spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                        "Referential Integrity: NPIs", "Missing NPI Count", 0, "INFO",
                        f"No non-NULL NPIs found in relevant columns ({', '.join(npi_cols_in_df)}) in '{events_table_name}' for month {current_refresh_month}."
                    ))
                 # Removed unpersist
                 # distinct_event_npis.unpersist()
            else:
                 results.append(create_result_row(
                    spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                    "Referential Integrity: NPIs", "Missing NPI Count", 0, "INFO",
                    f"No NPI columns found or no NPIs to check in '{events_table_name}' for month {current_refresh_month}."
                ))
        else:
            print(f"Warning: Skipping NPI consistency check as no relevant NPI columns ({', '.join(npi_cols_in_event_table)}) found in {events_table_name}.")


        # Check 4: Event Date vs Enrollment Periods (Common Logic)
        print(f"Checking Enrollment consistency for {events_table_name}...")
        # Use appropriate date column from events_df
        events_enroll_join = events_df.select(f"ev.{event_key_col}", "ev.patient_id", f"ev.{primary_date_col}").join(
            enroll_df, # Consider broadcast(enroll_df) if significantly smaller
            (col("ev.patient_id") == col("en.patient_id")) &
            (col(f"ev.{primary_date_col}") >= col("en.enroll_start")) &
            (col(f"ev.{primary_date_col}") <= col("en.enroll_end")),
            "left" # Keep all events, mark those without matching enrollment
        )
        events_outside_enrollment_count = events_enroll_join.filter(
            isnull(col("en.patient_id")) # Check if the join failed (enroll columns are NULL)
        ).select(f"ev.{event_key_col}").distinct().count()
        status = "PASS" if events_outside_enrollment_count == 0 else "WARN"
        detail = f"{events_outside_enrollment_count} distinct events occurred outside any known enrollment period for the patient (based on {primary_date_col})."
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Event Date vs Enrollment", "Events Outside Enrollment Count", events_outside_enrollment_count, status, detail
        ))


        # Check 5: Event Date vs Patient Death Date (Common Logic)
        print(f"Checking Mortality consistency for {events_table_name}...")
        # Use appropriate date column from events_df
        events_after_death_count = (
            events_df.select("ev.patient_id", f"ev.{primary_date_col}")
            .join(
                broadcast(mortality_df), # Broadcast mortality if smaller
                col("ev.patient_id") == col("mo.patient_id"),
                "inner" # Only consider patients with a death date
            )
            .filter(col(f"ev.{primary_date_col}") > col("mo.patient_death_date"))
            .count() # Count rows where event is after death
        )
        status = "PASS" if events_after_death_count == 0 else "FAIL"
        detail = f"{events_after_death_count} events occurred after the patient's recorded death date (based on {primary_date_col})."
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Event Date vs Death Date", "Events After Death Count", events_after_death_count, status, detail
        ))


        # Check 6: Demographic Stability (Placeholder)
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Demographic Stability", "Check Status", "NOT IMPLEMENTED", "INFO",
            "Requires comparing demo table across refreshes."
        ))

        # --- 3. Cleanup and Combine Results ---
        # Removed unpersist
        # events_df.unpersist()
        return _combine_results(results)

    except Exception as e:
        print(f"Error during consistency check for {events_table_name}: {e}")
        # Removed unpersist checks
        # if 'events_df' in locals() and events_df.is_cached:
        #     events_df.unpersist()
        # if 'distinct_event_npis' in locals() and distinct_event_npis.is_cached:
        #      distinct_event_npis.unpersist()
        return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                 check_category, "Execution Status", "Status", "ERROR", "FAIL",
                                 f"Error executing check: {str(e)[:500]}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Distribution

# COMMAND ----------

def check_distribution(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str, # Added parameter
    current_refresh_month: str,
    db_name: str,
    raw_schema: str
) -> DataFrame | None:
    """
    Analyzes demographic/geographic distributions, adapting for Stg_mx_events and Stg_rx_events.
    Removed caching/persist.
    """
    print(f"Starting Distribution checks for table '{events_table_name}', month: {current_refresh_month}")
    check_category = "Distribution"
    results = [] # List to hold result DataFrames

    try:
        # --- 1. Determine Table-Specific Columns ---
        if events_table_name == "Stg_rx_events":
            primary_date_col = "fill_date"
            event_key_col = "pharmacy_event_id"
        else: # Default to Stg_mx_events
            primary_date_col = "service_date"
            event_key_col = "medical_event_id"

        # --- 2. Load and Prepare Events Data ---
        required_event_cols = {event_key_col, "patient_id", primary_date_col, "kh_refresh_date"}

        events_df = get_table(spark, db_name, raw_schema, events_table_name)

        missing_event_cols = [c for c in required_event_cols if c not in events_df.columns]
        if missing_event_cols:
            print(f"Warning: Missing required columns in {events_table_name}: {missing_event_cols}. Skipping distribution checks.")
            return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Column Existence", "Check Status", "SKIPPED", "WARN",
                                     f"Missing columns: {', '.join(missing_event_cols)}")

        events_df_filtered = (
            events_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == current_refresh_month)
            .select(event_key_col, "patient_id", primary_date_col)
            .alias("ev")
        )

        # Removed persist/cache
        # events_df_filtered.persist(StorageLevel.MEMORY_AND_DISK)

        if events_df_filtered.isEmpty():
            print(f"No records found for table '{events_table_name}', refresh month {current_refresh_month}. Skipping distribution checks.")
            # Removed unpersist
            # events_df_filtered.unpersist()
            return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                     check_category, "Record Count", "Total Records", 0, "INFO",
                                     "No records found for this table and month, skipping checks.")

        # --- 3. Demographic Distribution (Gender, Age Group) ---
        print(f"Calculating Demographic Distributions for {events_table_name}...")
        # Define demo_df here to check caching status in error block if needed
        events_with_demo = None
        try:
            latest_demo = (
                get_table(spark, db_name, raw_schema, DEMO_TABLE)
                .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= current_refresh_month)
                .withColumn("rn", expr("row_number() OVER (PARTITION BY patient_id ORDER BY kh_refresh_date DESC)"))
                .filter(col("rn") == 1)
                .withColumn("age", when(col("patient_yob").isNotNull(), year(current_date()) - col("patient_yob")).otherwise(None))
                .withColumn("age_group",
                            when(col("age") < 18, "0-17")
                            .when((col("age") >= 18) & (col("age") < 45), "18-44")
                            .when((col("age") >= 45) & (col("age") < 65), "45-64")
                            .when(col("age") >= 65, "65+")
                            .otherwise("Unknown"))
                .select("patient_id", "patient_gender", "age_group")
                .alias("dm")
            )

            events_with_demo = events_df_filtered.join(broadcast(latest_demo), "patient_id", "inner")
            # Removed persist/cache
            # events_with_demo.persist(StorageLevel.MEMORY_AND_DISK)

            total_events_with_demo = events_with_demo.count()

            if total_events_with_demo > 0:
                demo_agg_results = events_with_demo.agg(
                    count(when(upper(col("patient_gender")) == 'F', 1)).alias("F_count"),
                    count(when(upper(col("patient_gender")) == 'M', 1)).alias("M_count"),
                    count(when(upper(col("patient_gender")).isNull() | ~upper(col("patient_gender")).isin('F','M'), 1)).alias("Other_Gender_count"),
                    count(when(col("age_group") == '0-17', 1)).alias("Age_0_17_count"),
                    count(when(col("age_group") == '18-44', 1)).alias("Age_18_44_count"),
                    count(when(col("age_group") == '45-64', 1)).alias("Age_45_64_count"),
                    count(when(col("age_group") == '65+', 1)).alias("Age_65_plus_count"),
                    count(when(col("age_group") == 'Unknown', 1)).alias("Age_Unknown_count")
                ).first()

                gender_dist_parts = []
                if demo_agg_results["F_count"] > 0: gender_dist_parts.append(f"F: {demo_agg_results['F_count']/total_events_with_demo:.1%}")
                if demo_agg_results["M_count"] > 0: gender_dist_parts.append(f"M: {demo_agg_results['M_count']/total_events_with_demo:.1%}")
                if demo_agg_results["Other_Gender_count"] > 0: gender_dist_parts.append(f"Other/Unk: {demo_agg_results['Other_Gender_count']/total_events_with_demo:.1%}")
                gender_dist_str = ", ".join(gender_dist_parts) if gender_dist_parts else "N/A"

                results.append(create_result_row(
                    spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                    "Gender Distribution", "Distribution String", gender_dist_str, "INFO",
                    f"Based on {total_events_with_demo} events with linked demo data."
                ))

                age_dist_parts = []
                age_map = { "0-17": demo_agg_results["Age_0_17_count"], "18-44": demo_agg_results["Age_18_44_count"],
                            "45-64": demo_agg_results["Age_45_64_count"], "65+": demo_agg_results["Age_65_plus_count"],
                            "Unknown": demo_agg_results["Age_Unknown_count"] }
                for age_group, count_val in age_map.items():
                    if count_val > 0: age_dist_parts.append(f"{age_group}: {count_val/total_events_with_demo:.1%}")
                age_dist_str = ", ".join(sorted(age_dist_parts)) if age_dist_parts else "N/A"

                results.append(create_result_row(
                    spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                    "Age Group Distribution", "Distribution String", age_dist_str, "INFO",
                    f"Based on {total_events_with_demo} events with linked demo data."
                ))
            else:
                results.append(create_result_row(
                    spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                    "Demographic Distribution", "Status", "SKIPPED", "INFO",
                    "No events could be linked to demographic data."
                ))

            # Removed unpersist
            # events_with_demo.unpersist()

        except Exception as demo_err:
            print(f"Error during demographic distribution for {events_table_name}: {demo_err}")
            results.append(create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                             check_category, "Demographic Distribution", "Status", "ERROR", "FAIL",
                                             f"Error calculating demo distribution: {str(demo_err)[:200]}"))
            # Removed unpersist check
            # if 'events_with_demo' in locals() and events_with_demo.is_cached:
            #     events_with_demo.unpersist()


        # --- 4. Geographic Distribution (State) ---
        print(f"Calculating Geographic Distributions for {events_table_name}...")
        # Define events_with_geo here to check caching status in error block if needed
        events_with_geo = None
        try:
            geo_df = (
                get_table(spark, db_name, raw_schema, GEO_TABLE)
                .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= current_refresh_month)
                .select("patient_id", "valid_from_date", "valid_to_date", "patient_state")
                .filter(col("valid_from_date").isNotNull() & col("valid_to_date").isNotNull() & col("patient_state").isNotNull())
                .alias("geo")
            )

            # Use the correct date column based on the event table
            events_with_geo = (
                events_df_filtered.join(
                    geo_df,
                    (col("ev.patient_id") == col("geo.patient_id")) &
                    (col(f"ev.{primary_date_col}") >= col("geo.valid_from_date")) & # Use primary_date_col
                    (col(f"ev.{primary_date_col}") <= col("geo.valid_to_date")),    # Use primary_date_col
                    "inner"
                )
                .select(f"ev.{event_key_col}", "geo.patient_state")
                .distinct()
            )
            # Removed persist/cache
            # events_with_geo.persist(StorageLevel.MEMORY_AND_DISK)

            state_counts = events_with_geo.groupBy("patient_state").agg(count("*").alias("event_count"))
            total_events_with_geo = events_with_geo.count()

            if total_events_with_geo > 0:
                state_dist = (
                    state_counts
                    .withColumn("percentage", (col("event_count") / total_events_with_geo) * 100)
                    .orderBy(col("event_count").desc())
                    .limit(10)
                    .orderBy(col("percentage").desc())
                )

                state_dist_rows = state_dist.collect()
                state_dist_str = ", ".join(
                    [f"{row.patient_state}: {row.percentage:.1f}%" for row in state_dist_rows]
                ) if state_dist_rows else "N/A"

                results.append(create_result_row(
                    spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                    "State Distribution (Top 10)", "Distribution String", state_dist_str, "INFO",
                    f"Based on {total_events_with_geo} distinct event-state occurrences linked via {primary_date_col}."
                ))
            else:
                results.append(create_result_row(
                    spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                    "State Distribution (Top 10)", "Distribution String", "N/A", "INFO",
                    f"No events could be linked to valid geo data for the {primary_date_col}."
                ))

            # Removed unpersist
            # events_with_geo.unpersist()

        except Exception as geo_err:
            print(f"Error during geographic distribution for {events_table_name}: {geo_err}")
            results.append(create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                             check_category, "Geographic Distribution", "Status", "ERROR", "FAIL",
                                             f"Error calculating geo distribution: {str(geo_err)[:200]}"))
            # Removed unpersist check
            # if 'events_with_geo' in locals() and events_with_geo.is_cached:
            #     events_with_geo.unpersist()


        # --- 5. Cleanup and Combine ---
        # Removed unpersist
        # events_df_filtered.unpersist()
        return _combine_results(results)

    except Exception as e:
        print(f"Error during distribution check for {events_table_name}: {e}")
        # Removed unpersist checks
        # if 'events_df_filtered' in locals() and events_df_filtered.is_cached: events_df_filtered.unpersist()
        # if 'events_with_demo' in locals() and events_with_demo.is_cached: events_with_demo.unpersist()
        # if 'events_with_geo' in locals() and events_with_geo.is_cached: events_with_geo.unpersist()

        return create_result_row(spark, run_id, run_start_time, events_table_name, current_refresh_month,
                                 check_category, "Execution Status", "Status", "ERROR", "FAIL",
                                 f"Error executing check: {str(e)[:500]}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Temporal

# COMMAND ----------

def check_temporal(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str, # Added parameter
    current_refresh_month: str,
    previous_refresh_month: str, # Required for this check
    db_name: str,
    raw_schema: str,
) -> DataFrame | None:
    """
    Compares key metrics between the current and previous refresh months.
    Uses patient_id and ndc11, applicable to both MX and RX.
    Removed caching/persist.
    """
    print(f"Starting Temporal checks for table '{events_table_name}', comparing {current_refresh_month} vs {previous_refresh_month}")
    check_category = "Temporal"
    results = [] # List to hold result DataFrames

    try:
        # --- 1. Data Loading ---
        # Check if ndc11 column exists, skip drug comparison if not
        base_df = get_table(spark, db_name, raw_schema, events_table_name)
        has_ndc11 = "ndc11" in base_df.columns
        select_cols = ["patient_id", "kh_refresh_date"]
        if has_ndc11:
             select_cols.append("ndc11")

        current_events = (
            base_df
            .select(*select_cols)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == current_refresh_month)
            .alias("curr")
        )
        # Removed persist/cache
        # current_events.persist(StorageLevel.MEMORY_AND_DISK)

        previous_events = (
             base_df # Reuse schema check result
            .select(*select_cols)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == previous_refresh_month)
            .alias("prev")
        )
        # Removed persist/cache
        # previous_events.persist(StorageLevel.MEMORY_AND_DISK)

        # --- 2. Patient Overlap, New, Returning Calculations ---
        print(f"Calculating Patient Overlap/New/Returning for {events_table_name}...")
        current_patients = current_events.select("patient_id").distinct().withColumnRenamed("patient_id", "curr_patient_id")
        previous_patients = previous_events.select("patient_id").distinct().withColumnRenamed("patient_id", "prev_patient_id")

        # Removed persist/cache
        # current_patients.persist(StorageLevel.MEMORY_AND_DISK)
        # previous_patients.persist(StorageLevel.MEMORY_AND_DISK)

        patient_overlap_agg = current_patients.join(
            previous_patients,
            current_patients["curr_patient_id"] == previous_patients["prev_patient_id"],
            "full_outer"
        ).agg(
            countDistinct(col("curr_patient_id")).alias("current_patient_count"),
            countDistinct(col("prev_patient_id")).alias("previous_patient_count"),
            countDistinct(when(col("curr_patient_id").isNotNull() & col("prev_patient_id").isNotNull(), col("curr_patient_id"))).alias("overlapping_patients"),
            countDistinct(when(col("curr_patient_id").isNotNull() & col("prev_patient_id").isNull(), col("curr_patient_id"))).alias("new_patients")
        ).first()

        # Removed unpersist
        # current_patients.unpersist()
        # previous_patients.unpersist()

        current_patient_count = patient_overlap_agg["current_patient_count"] if patient_overlap_agg else 0
        previous_patient_count = patient_overlap_agg["previous_patient_count"] if patient_overlap_agg else 0
        overlapping_patients = patient_overlap_agg["overlapping_patients"] if patient_overlap_agg else 0
        new_patients = patient_overlap_agg["new_patients"] if patient_overlap_agg else 0
        returning_patients = overlapping_patients

        # --- 3. Report Patient Overlap Results ---
        if current_patient_count == 0:
            print(f"No patients found for current month {current_refresh_month} in {events_table_name}. Skipping most temporal checks.")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Patient Count", "Current Month", 0, "INFO", "No patients in current refresh."
            ))
            # Removed unpersist
            # current_events.unpersist()
            # previous_events.unpersist()
            return _combine_results(results)

        results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Patient Count", "Current Month", current_patient_count, "INFO",
                f"{current_patient_count} distinct patients in current refresh."
        ))

        if previous_patient_count == 0:
            print(f"No patients found for previous month {previous_refresh_month} in {events_table_name}. Cannot calculate overlap percentages.")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Patient Count", "Previous Month", 0, "INFO",
                "No patients in previous refresh. Overlap/New/Returning checks skipped."
            ))
        else:
            overlap_percentage = (overlapping_patients / previous_patient_count) * 100
            detail_overlap = (f"{overlapping_patients} patients overlap ({overlap_percentage:.2f}%) "
                              f"between current ({current_patient_count}) and previous ({previous_patient_count}) months.")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Patient Overlap", "Overlap Count", overlapping_patients, "INFO", detail_overlap
            ))
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Patient Overlap", "Overlap Percentage", overlap_percentage, "INFO", detail_overlap # Pass float
            ))

            new_patient_percentage = (new_patients / current_patient_count) * 100
            detail_new = (f"{new_patients} new patients ({new_patient_percentage:.2f}%) appeared in current month "
                          f"(total current: {current_patient_count}).")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "New Patients", "New Patient Count", new_patients, "INFO", detail_new
            ))
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "New Patients", "New Patient Percentage", new_patient_percentage, "INFO", detail_new # Pass float
            ))

            returning_percentage = (returning_patients / previous_patient_count) * 100
            detail_returning = (f"{returning_patients} patients returned ({returning_percentage:.2f}% retention) "
                                f"from previous month (total previous: {previous_patient_count}).")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Returning Patients", "Returning Patient Count", returning_patients, "INFO", detail_returning
            ))
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Returning Patients", "Returning Patient Percentage", returning_percentage, "INFO", detail_returning # Pass float
            ))

        # --- 4. Patient Count by Drug (NDC11) Comparison ---
        print(f"Calculating NDC Patient Count Changes for {events_table_name}...")
        if has_ndc11:
            current_ndc_patients = (
                current_events.filter(col("ndc11").isNotNull())
                .groupBy("ndc11")
                .agg(countDistinct("patient_id").alias("current_patient_count"))
            )
            previous_ndc_patients = (
                previous_events.filter(col("ndc11").isNotNull())
                .groupBy("ndc11")
                .agg(countDistinct("patient_id").alias("previous_patient_count"))
            )

            ndc_comparison = (
                current_ndc_patients.join(previous_ndc_patients, "ndc11", "full_outer")
                .fillna(0, subset=["current_patient_count", "previous_patient_count"])
                .withColumn("patient_count_change", col("current_patient_count") - col("previous_patient_count"))
                .withColumn("patient_count_change_pct",
                            when(col("previous_patient_count") == 0, lit(None))
                            .otherwise(((col("current_patient_count") - col("previous_patient_count")) / col("previous_patient_count")) * 100))
            )

            significant_changes = ndc_comparison.filter(
                (spark_abs(col("patient_count_change_pct")) > 50.0) &
                (col("current_patient_count") > 10)
            ).orderBy(spark_abs(col("patient_count_change")).desc()).limit(20).collect()

            change_details = []
            for row in significant_changes:
                 pct_str = f"{row.patient_count_change_pct:+.1f}%" if row.patient_count_change_pct is not None else "New"
                 change_details.append(
                     f"NDC:{row.ndc11}(Prev:{row.previous_patient_count}, Curr:{row.current_patient_count}, Chg:{row.patient_count_change:+d} [{pct_str}])"
                 )

            status = "WARN" if significant_changes else "PASS"
            detail = (f"{len(significant_changes)} NDCs had >50% patient count change (min 10 current patients). "
                      f"Top changes: {'; '.join(change_details[:5])}")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Patient Count by Drug (NDC11)", "Significant Changes Count", len(significant_changes), status, detail
            ))
        else:
            print(f"Warning: Skipping NDC comparison for {events_table_name} as 'ndc11' column is missing.")
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                "Patient Count by Drug (NDC11)", "Check Status", "SKIPPED", "WARN",
                "ndc11 column missing."
            ))

        # --- 5. Cleanup and Combine ---
        # Removed unpersist
        # current_events.unpersist()
        # previous_events.unpersist()
        return _combine_results(results)

    except Exception as e:
        print(f"Error during temporal checks for {events_table_name}: {e}")
        # Removed unpersist checks
        # if 'current_events' in locals() and current_events.is_cached: current_events.unpersist()
        # if 'previous_events' in locals() and previous_events.is_cached: previous_events.unpersist()
        # if 'current_patients' in locals() and current_patients.is_cached: current_patients.unpersist()
        # if 'previous_patients' in locals() and previous_patients.is_cached: previous_patients.unpersist()

        error_detail = f"Error during temporal check execution: {str(e)[:500]}"
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Execution Status", "Status", "ERROR", "FAIL", error_detail
        ))
        return _combine_results(results) # Combine any results generated before the error


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Volume

# COMMAND ----------

def check_volume(
    spark: SparkSession,
    run_id: str,
    run_start_time: datetime.datetime,
    events_table_name: str, # Added parameter
    current_refresh_month: str,
    previous_refresh_month: str, # Required for this check
    db_name: str,
    raw_schema: str,
) -> DataFrame | None:
    """
    Compares data volumes, adapting count distinct logic for Stg_mx_events (procedure_code)
    and Stg_rx_events (ndc11).
    Removed caching/persist.
    """
    print(f"Starting Volume checks for table '{events_table_name}', comparing {current_refresh_month} vs {previous_refresh_month}")
    check_category = "Volume"
    results = [] # List to hold result DataFrames
    current_counts_row: Optional[Row] = None
    previous_counts_row: Optional[Row] = None

    try:
        # --- Determine Table-Specific Columns & Metrics ---
        base_df = get_table(spark, db_name, raw_schema, events_table_name)
        required_cols = ["kh_refresh_date", "patient_id"]
        agg_cols = [
            count("*").alias("record_count"),
            countDistinct("patient_id").alias("distinct_patient_count")
        ]
        code_col = None
        code_metric_name = "Distinct Code Count" # Generic name initially

        if events_table_name == "Stg_rx_events":
            if "ndc11" in base_df.columns:
                code_col = "ndc11"
                code_metric_name = "Distinct NDC11 Count"
                required_cols.append(code_col)
                agg_cols.append(countDistinct(code_col).alias("distinct_code_count"))
            else: print(f"Warning: 'ndc11' column not found in {events_table_name}, skipping distinct code count.")
        else: # Default to Stg_mx_events
            if "procedure_code" in base_df.columns:
                code_col = "procedure_code"
                code_metric_name = "Distinct Procedure Code Count"
                required_cols.append(code_col)
                agg_cols.append(countDistinct(code_col).alias("distinct_code_count"))
            else: print(f"Warning: 'procedure_code' column not found in {events_table_name}, skipping distinct code count.")

        # --- 1. Data Loading and Aggregation ---
        print(f"Aggregating counts for current month: {current_refresh_month} in {events_table_name}")
        if not all(c in base_df.columns for c in required_cols):
             missing = [c for c in required_cols if c not in base_df.columns]
             raise ValueError(f"Missing required columns ({', '.join(missing)}) in {events_table_name} for volume check.")

        current_counts_row = (
            base_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == current_refresh_month)
            .agg(*agg_cols)
            .first()
        )

        print(f"Aggregating counts for previous month: {previous_refresh_month} in {events_table_name}")
        # Assume schema is the same for previous month
        previous_counts_row = (
            base_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == previous_refresh_month)
            .agg(*agg_cols)
            .first()
        )

    except Exception as e:
        print(f"Error loading/aggregating data for volume checks on {events_table_name}: {e}")
        error_detail = f"Could not load/aggregate data: {str(e)[:200]}"
        results.append(create_result_row(
             spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
             "Data Loading", "Status", "ERROR", "FAIL", error_detail
        ))
        return _combine_results(results)


    # --- 2. Extract Counts ---
    current_rec_count = current_counts_row["record_count"] if current_counts_row else 0
    current_pat_count = current_counts_row["distinct_patient_count"] if current_counts_row else 0
    current_code_count = current_counts_row.get("distinct_code_count", 0) if current_counts_row else 0 # Use get for safety

    previous_rec_count = previous_counts_row["record_count"] if previous_counts_row else 0
    previous_pat_count = previous_counts_row["distinct_patient_count"] if previous_counts_row else 0
    previous_code_count = previous_counts_row.get("distinct_code_count", 0) if previous_counts_row else 0 # Use get for safety

    # --- 3. Record Count Comparison ---
    print(f"Comparing Record Counts for {events_table_name}...")
    results.append(create_result_row(
        spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
        "Record Count", "Current Month", current_rec_count, "INFO",
        f"Previous month: {previous_rec_count}"
    ))
    if previous_rec_count > 0:
        rec_change_pct = ((current_rec_count - previous_rec_count) / previous_rec_count) * 100
        status = "PASS" if abs(rec_change_pct / 100) <= RECORD_COUNT_DEV_THRESHOLD else "WARN"
        detail = f"Change: {rec_change_pct:+.2f}%. Threshold: +/-{RECORD_COUNT_DEV_THRESHOLD:.0%}"
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Record Count Change %", "Percentage Change", rec_change_pct, status, detail # Pass float value
        ))
    else:
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Record Count Change %", "Percentage Change", "N/A", "INFO", "Previous month had 0 records."
        ))

    # --- 4. Distinct Patient Count Comparison ---
    print(f"Comparing Distinct Patient Counts for {events_table_name}...")
    results.append(create_result_row(
        spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
        "Distinct Patient Count", "Current Month", current_pat_count, "INFO",
        f"Previous month: {previous_pat_count}"
    ))
    if previous_pat_count > 0:
        pat_change_pct = ((current_pat_count - previous_pat_count) / previous_pat_count) * 100
        status = "PASS" if abs(pat_change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN"
        detail = f"Change: {pat_change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Distinct Patient Count Change %", "Percentage Change", pat_change_pct, status, detail # Pass float value
        ))
    else:
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Distinct Patient Count Change %", "Percentage Change", "N/A", "INFO", "Previous month had 0 patients."
        ))

    # --- 5. Distinct Code Count Comparison (Procedure or NDC) ---
    if code_col: # Only run if a relevant code column was found
        print(f"Comparing {code_metric_name} for {events_table_name}...")
        results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            code_metric_name, "Current Month", current_code_count, "INFO",
            f"Previous month: {previous_code_count}"
        ))
        if previous_code_count > 0:
            code_change_pct = ((current_code_count - previous_code_count) / previous_code_count) * 100
            status = "PASS" if abs(code_change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN" # Reuse patient threshold
            detail = f"Change: {code_change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                f"{code_metric_name} Change %", "Percentage Change", code_change_pct, status, detail # Pass float value
            ))
        else:
            results.append(create_result_row(
                spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
                f"{code_metric_name} Change %", "Percentage Change", "N/A", "INFO", f"Previous month had 0 distinct {code_col}s."
            ))
    else: # Log skipped check if code column was missing
         results.append(create_result_row(
            spark, run_id, run_start_time, events_table_name, current_refresh_month, check_category,
            "Distinct Code Count Comparison", "Check Status", "SKIPPED", "WARN",
            f"Relevant code column (procedure_code/ndc11) not found in {events_table_name}."
        ))


    # --- 6. Combine and Return Results ---
    print(f"Combining volume check results for {events_table_name}...")
    return _combine_results(results)


# COMMAND ----------

# MAGIC %md
# MAGIC # Main Orchestrator

# COMMAND ----------

def run_check(
    spark: SparkSession,
    check_func,
    run_id: str,
    run_start_time: datetime.datetime,
    result_schema_arg: StructType, # Pass schema explicitly
    **kwargs):
    """
    Runs a single check function, saves results, and handles errors.
    """
    check_name = check_func.__name__
    events_table_name = kwargs.get("events_table_name", "UNKNOWN_TABLE") # Get table name from kwargs
    refresh_month = kwargs.get("current_refresh_month", "UNKNOWN_MONTH")

    try:
        print(f"Running check: {check_name} for table: {events_table_name}...")

        # Add run_id and run_start_time to kwargs passed to the check function
        kwargs_for_check = {
            **kwargs,
            "run_id": run_id,
            "run_start_time": run_start_time,
        }

        # Call the check function with all necessary arguments
        result_df = check_func(spark=spark, **kwargs_for_check)

        if result_df is not None and not result_df.isEmpty():
            save_results(spark, result_df)
        elif result_df is None:
             print(f"Check function {check_name} returned None for table {events_table_name}.")
        else: # result_df is an empty DataFrame
             print(f"No results generated by {check_name} for table {events_table_name}.")

        print(f"Finished check: {check_name} for table: {events_table_name}")

    except Exception as e:
        print(f"ERROR running check {check_name} for table {events_table_name}: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging

        # Infer category: remove "check_" and capitalize, handle potential errors
        try:
            category = check_name.replace("check_", "").capitalize() if check_name.startswith("check_") else "General"
        except:
            category = "ErrorHandling"

        # Create error DataFrame using the passed schema
        error_data = [(
            run_id, run_start_time, events_table_name, refresh_month,
            category,
            "Execution Status", "Status", "ERROR", "FAIL",
            f"Error in {check_name}: {str(e)[:500]}" # Limit error message length
        )]
        try:
            # Use the passed schema argument here
            error_df = spark.createDataFrame(error_data, schema=result_schema_arg)
            save_results(spark, error_df)
        except Exception as save_err:
            print(f"CRITICAL ERROR: Failed to save error details for check {check_name} on table {events_table_name}: {save_err}")


def run_all_dq_checks(
    spark: SparkSession, current_refresh_month: str, previous_refresh_month: str | None
):
    """Runs all defined DQ checks for the specified refresh month across all defined EVENTS_TABLES."""

    # Define standard checks (run even without previous month)
    standard_checks = [
        check_completeness,
        check_uniqueness,
        check_validity,
        check_consistency,
        check_distribution
    ]

    # Define checks that require previous month data
    temporal_checks = [
        check_temporal,
        check_volume
    ]

    # Generate unique run ID and start time for this entire run
    run_id = str(uuid.uuid4())
    run_start_time = datetime.datetime.now()
    print(f"--- Starting DQ Run ID: {run_id} at {run_start_time} ---")
    print(f"--- Refresh Month: {current_refresh_month} ---")
    if previous_refresh_month:
        print(f"--- Comparing against Previous Month: {previous_refresh_month} ---")
    else:
        print("--- No previous month provided for comparison. Temporal checks will be skipped. ---")
    print(f"--- Results will be saved to: {RESULTS_TABLE} ---")

    # Iterate through each events table defined in the config
    for table_name in EVENTS_TABLES:
        print(f"\n=== Running Checks for Events Table: {table_name} ===")

        # Define common arguments for checks on this table
        common_args = {
            "events_table_name": table_name,
            "current_refresh_month": current_refresh_month,
            "db_name": DB_NAME,
            "raw_schema": RAW_SCHEMA,
        }

        # Run all standard checks
        for check_func in standard_checks:
            # Pass the global result_schema to run_check
            run_check(spark, check_func, run_id, run_start_time, result_schema, **common_args)

        # Run Temporal and Volume checks only if previous month is available
        if previous_refresh_month:
            temporal_volume_args = {
                **common_args,  # Include common args
                "previous_refresh_month": previous_refresh_month, # Add previous month
            }

            for check_func in temporal_checks:
                 # Pass the global result_schema to run_check
                run_check(spark, check_func, run_id, run_start_time, result_schema, **temporal_volume_args)
        else:
            print(f"Skipping Temporal and Volume checks for table {table_name} as previous_refresh_month was not provided.")
            # Log skipped checks for this table directly using create_result_row
            skipped_results = []
            for check_func in temporal_checks:
                 # Infer category
                 try:
                     category = check_func.__name__.replace("check_", "").capitalize() if check_func.__name__.startswith("check_") else "General"
                 except:
                    category = "Skipped"

                 skipped_results.append(create_result_row(
                    spark, run_id, run_start_time, table_name, current_refresh_month,
                    category, "Execution Status", "Status", "SKIPPED", "INFO",
                    "Previous refresh month not provided."
                 ))
            # Combine and save the skipped records
            skip_df = _combine_results(skipped_results)
            if skip_df:
                save_results(spark, skip_df)

    print(f"\n--- Finished DQ Run ID: {run_id} at {datetime.datetime.now()} ---")


def main():
    """Main function to determine months and run DQ checks."""
    # Use global spark session if available in notebook environment
    global spark

    # --- Determine Refresh Months ---
    try:
        if not EVENTS_TABLES:
             print("Error: EVENTS_TABLES list is empty in configuration. Cannot determine refresh month.")
             return
        # Use the first table in the list as the reference for determining months
        reference_events_table = EVENTS_TABLES[0]
        print(f"Determining latest refresh month from reference table: {DB_NAME}.{RAW_SCHEMA}.{reference_events_table}...")

        latest_refresh = (
            spark.table(f"{DB_NAME}.{RAW_SCHEMA}.{reference_events_table}")
            .select(spark_max(date_format(col("kh_refresh_date"), "yyyy-MM")).alias("latest_month"))
            .first()
        )
        current_month = latest_refresh["latest_month"] if latest_refresh else None

        if not current_month:
            print(f"Could not determine the current refresh month from table {reference_events_table}. Exiting.")
            return

        # Determine previous month robustly
        previous_month = None
        try:
            from dateutil.relativedelta import relativedelta
            current_dt = datetime.datetime.strptime(current_month + "-01", "%Y-%m-%d")
            prev_dt = current_dt - relativedelta(months=1)
            previous_month = prev_dt.strftime("%Y-%m")
            print(f"Calculated previous month using dateutil: {previous_month}")
        except ImportError:
            print("Warning: dateutil not found. Attempting basic month subtraction (may be inaccurate).")
            try:
                current_dt = datetime.datetime.strptime(current_month + "-01", "%Y-%m-%d")
                prev_dt = current_dt - datetime.timedelta(days=30) # Basic fallback
                previous_month = prev_dt.strftime("%Y-%m")
                print(f"Calculated previous month using basic subtraction: {previous_month}")
            except Exception as basic_calc_err:
                 print(f"Error calculating previous month using basic method: {basic_calc_err}")
                 previous_month = None

        # Check if previous month exists in the reference table
        final_previous_month = None
        if previous_month:
            try:
                print(f"Checking existence of previous month {previous_month} in reference table {reference_events_table}...")
                prev_month_exists = (
                    spark.table(f"{DB_NAME}.{RAW_SCHEMA}.{reference_events_table}")
                    .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == previous_month)
                    .limit(1)
                    .count() > 0
                )
                if prev_month_exists:
                    final_previous_month = previous_month
                    print(f"Data found for previous month {final_previous_month}.")
                else:
                    print(f"No data found for calculated previous month {previous_month} in reference table.")
            except Exception as check_err:
                print(f"Could not check existence of previous month {previous_month} due to error: {check_err}. Assuming it doesn't exist.")
        else:
            print("Could not determine previous month.")


        if final_previous_month is None:
             print("Running checks without temporal/volume comparison.")

        print(f"\nRunning checks for Current Month: {current_month}")
        if final_previous_month:
            print(f"Comparing against Previous Month: {final_previous_month}")

        # Call the orchestrator function
        run_all_dq_checks(spark, current_month, final_previous_month)

    except Exception as e:
        print(f"\n--- ERROR in main execution ---")
        print(f"Error determining refresh months or running checks: {e}")
        import traceback
        traceback.print_exc()
        print(f"--- DQ Run Aborted ---")

# COMMAND ----------

# Execute the main function if running as a script or interactively
# In Databricks, just calling main() is usually sufficient if spark is initialized.
# if __name__ == "__main__":
#     main()

# Example of how to run it in a notebook cell:
main()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optional: Example SQL to view results
# MAGIC -- SELECT * FROM commercial.dev_jlandesman.mx_events_dq_results
# MAGIC -- ORDER BY run_start_time DESC, events_table_name, check_category, check_name;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Optional: Example SQL to add columns if needed (run only once)
# MAGIC -- Make sure the schema matches the Python `result_schema`
# MAGIC /*
# MAGIC ALTER TABLE commercial.dev_jlandesman.mx_events_dq_results ADD COLUMNS (
# MAGIC   run_id STRING,
# MAGIC   run_start_time TIMESTAMP,
# MAGIC   events_table_name STRING
# MAGIC );
# MAGIC */
# MAGIC -- Verify schema
# MAGIC -- DESCRIBE TABLE commercial.dev_jlandesman.mx_events_dq_results;