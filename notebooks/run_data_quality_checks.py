# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Komodo Data Quality Checks
# MAGIC This notebook demonstrates how to run the data quality checks framework.
# MAGIC 
# MAGIC ## Overview
# MAGIC The framework includes the following types of checks:
# MAGIC - Completeness: Check for missing values in required and optional fields
# MAGIC - Consistency: Check for data consistency across related tables
# MAGIC - Distribution: Check for expected distributions of key fields
# MAGIC - Temporal: Check for time-based patterns and patient retention
# MAGIC - Validity: Check for valid values and formats
# MAGIC - Volume: Check for expected data volumes and trends

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup
# MAGIC First, let's import the necessary modules and set up our parameters.

# COMMAND ----------
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------
import sys
import os

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Print the Python path for debugging
print("Python path:", sys.path)
print("Project root:", project_root)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Settings
# MAGIC Let's verify that we can access the settings.

# COMMAND ----------
try:
    from src.config.settings import (
        DB_NAME,
        RAW_SCHEMA,
        STAGING_SCHEMA,
        REFRESH_MONTH,
        PREVIOUS_REFRESH_MONTH
    )
    print("Successfully imported settings:")
    print(f"DB_NAME: {DB_NAME}")
    print(f"RAW_SCHEMA: {RAW_SCHEMA}")
    print(f"STAGING_SCHEMA: {STAGING_SCHEMA}")
    print(f"REFRESH_MONTH: {REFRESH_MONTH}")
    print(f"PREVIOUS_REFRESH_MONTH: {PREVIOUS_REFRESH_MONTH}")
except Exception as e:
    print(f"Error importing settings: {str(e)}")
    print("Current directory:", os.getcwd())
    print("Directory contents:", os.listdir("."))
    print("Parent directory contents:", os.listdir(".."))
    print("src directory contents:", os.listdir("../src"))
    print("config directory contents:", os.listdir("../src/config"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run All Checks
# MAGIC Run all data quality checks on the specified events table.

# COMMAND ----------
# Run all checks for MX events
%run ../src/run_checks.py --events-table Stg_mx_events

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Specific Checks
# MAGIC You can also run specific types of checks.

# COMMAND ----------
# Run only completeness and validity checks for RX events
%run ../src/run_checks.py --events-table Stg_rx_events --checks completeness validity

# COMMAND ----------
# MAGIC %md
# MAGIC ## Override Refresh Months
# MAGIC You can override the refresh months defined in settings.py.

# COMMAND ----------
# Run checks with custom refresh months
%run ../src/run_checks.py --events-table Stg_mx_events --refresh-month 2024-02 --previous-refresh-month 2024-01

# COMMAND ----------
# MAGIC %md
# MAGIC ## View Results
# MAGIC Query the results table to see the check results.

# COMMAND ----------
from src.config.settings import RESULTS_TABLE

# Display the most recent check results
spark.sql(f"""
SELECT 
    check_category,
    check_name,
    metric_name,
    metric_value,
    status,
    details
FROM {RESULTS_TABLE}
WHERE run_start_time >= (SELECT MAX(run_start_time) FROM {RESULTS_TABLE})
ORDER BY check_category, check_name
""") 