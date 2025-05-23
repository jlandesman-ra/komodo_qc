"""
Utility functions for Spark operations in the Komodo Data Quality Framework.
"""
from src.config.settings import RESULTS_TABLE
from typing import Optional, List, Any, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
)
from datetime import datetime

from src.core.result_schema import result_schema

def get_spark_session() -> SparkSession:
    """Creates and returns a SparkSession instance."""
    try:
        # Try to get the existing Databricks SparkSession
        spark = SparkSession.builder.getOrCreate()
        # Test if we're in Databricks
        _ = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        print("Using existing Databricks SparkSession")
        return spark
    except Exception as e:
        print(f"Not running in Databricks environment: {str(e)}")
        print("Please run this script in a Databricks environment")
        raise RuntimeError("This script must be run in a Databricks environment")

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

    try:
        # Ensure the DataFrame schema matches the target table schema
        results_df = results_df.select([col(field.name) for field in result_schema.fields])
        
        # Write to the results table
        results_df.write.mode("append").saveAsTable(RESULTS_TABLE)
        print("Results saved successfully.")
    except Exception as e:
        print(f"Error saving results: {e}")
        raise e

def create_result_row(
    run_id: str,
    run_start_time_val: datetime,
    events_table_name: str,
    refresh_month: str,
    check_category: str,
    check_name: str,
    metric_name: str,
    metric_value: Any,
    status: str,
    details: Optional[str] = None,
) -> Dict:
    """Creates a standardized result row for data quality checks."""
    return {
        "run_id": run_id,
        "run_start_time": run_start_time_val,
        "events_table_name": events_table_name,
        "refresh_month": refresh_month,
        "check_category": check_category,
        "check_name": check_name,
        "metric_name": metric_name,
        "metric_value": str(metric_value) if metric_value is not None else None,
        "status": status,
        "details": details,
    } 