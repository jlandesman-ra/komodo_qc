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
from pathlib import Path

# Get the workspace path
workspace_path = "/Workspace/Users/jlandesman@racap.com/komodo_qc"
print(f"Workspace path: {workspace_path}")

# Add the workspace path to Python path
if workspace_path not in sys.path:
    sys.path.insert(0, workspace_path)

# Print the Python path for debugging
print("\nPython path:")
for path in sys.path:
    print(f"  - {path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Settings
# MAGIC Let's verify that we can access the settings.

# COMMAND ----------
# Try to load the settings file directly
settings_path = os.path.join(workspace_path, "src", "config", "settings.py")
print(f"Settings file path: {settings_path}")
print(f"Settings file exists: {os.path.exists(settings_path)}")

if os.path.exists(settings_path):
    # Read the file contents
    with open(settings_path, 'r') as f:
        settings_content = f.read()
    print("\nSettings file contents:")
    print(settings_content)
    
    # Create a new module
    import types
    settings_module = types.ModuleType('settings')
    
    # Execute the file contents in the module's namespace
    exec(settings_content, settings_module.__dict__)
    
    # Make the module available in sys.modules
    sys.modules['src.config.settings'] = settings_module
    
    print("\nSuccessfully loaded settings module")
    print(f"REFRESH_MONTH: {getattr(settings_module, 'REFRESH_MONTH', 'Not found')}")
    print(f"PREVIOUS_REFRESH_MONTH: {getattr(settings_module, 'PREVIOUS_REFRESH_MONTH', 'Not found')}")

# Try the normal import
try:
    from src.config.settings import (
        DB_NAME,
        RAW_SCHEMA,
        STAGING_SCHEMA,
        REFRESH_MONTH,
        PREVIOUS_REFRESH_MONTH
    )
    print("\nSuccessfully imported settings variables:")
    print(f"DB_NAME: {DB_NAME}")
    print(f"RAW_SCHEMA: {RAW_SCHEMA}")
    print(f"STAGING_SCHEMA: {STAGING_SCHEMA}")
    print(f"REFRESH_MONTH: {REFRESH_MONTH}")
    print(f"PREVIOUS_REFRESH_MONTH: {PREVIOUS_REFRESH_MONTH}")
except Exception as e:
    print(f"\nError importing settings: {str(e)}")
    print("\nCurrent directory:", os.getcwd())
    print("\nDirectory contents:", os.listdir("."))
    print("\nParent directory contents:", os.listdir(".."))
    print("\nsrc directory contents:", os.listdir("../src"))
    print("\nconfig directory contents:", os.listdir("../src/config"))
    import traceback
    print("\nTraceback:")
    print(traceback.format_exc())

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