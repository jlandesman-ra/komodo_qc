"""
Schema definition for data quality check results.
"""

from pyspark.sql.types import (
    StringType,
    TimestampType,
    StructType,
    StructField,
)

# Result Schema for the output table
result_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_start_time", TimestampType(), False),
        StructField("events_table_name", StringType(), False),
        StructField("refresh_month", StringType(), False),
        StructField("check_category", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_value", StringType(), True),  # Using String for flexibility
        StructField("status", StringType(), False),  # e.g., PASS, FAIL, WARN, INFO, ERROR, SKIPPED
        StructField("details", StringType(), True),
    ]
) 