"""
Example script demonstrating how to run Komodo Data Quality checks.
"""

import uuid
from pyspark.sql import SparkSession
from src.checks import (
    CompletenessCheck,
    UniquenessCheck,
    ValidityCheck,
    ConsistencyCheck,
    DistributionCheck,
    TemporalCheck,
    VolumeCheck,
)

def run_all_dq_checks(
    spark: SparkSession,
    events_table: str,
    refresh_month: str,
    specific_checks: Optional[List[str]] = None,
):
    """
    Run all data quality checks or specific checks if specified.
    
    Args:
        spark: SparkSession instance
        events_table: Name of the events table to check
        refresh_month: Month of the data refresh (YYYY-MM)
        specific_checks: Optional list of specific checks to run
    """
    # Generate a unique run ID
    run_id = str(uuid.uuid4())
    
    # Load the events table
    events_df = spark.table(events_table)
    
    # Define available checks
    available_checks = {
        "completeness": CompletenessCheck,
        "uniqueness": UniquenessCheck,
        "validity": ValidityCheck,
        "consistency": ConsistencyCheck,
        "distribution": DistributionCheck,
        "temporal": TemporalCheck,
        "volume": VolumeCheck,
    }
    
    # Determine which checks to run
    checks_to_run = (
        available_checks
        if specific_checks is None
        else {k: v for k, v in available_checks.items() if k in specific_checks}
    )
    
    # Run the checks
    all_results = []
    for check_name, check_class in checks_to_run.items():
        print(f"Running {check_name} checks...")
        check = check_class(spark, events_df, refresh_month)
        results = check.run()
        all_results.extend(results)
    
    # Save results
    if all_results:
        results_df = spark.createDataFrame(all_results)
        save_results(spark, results_df)

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("KomodoDQ").getOrCreate()
    
    # Example usage
    run_all_dq_checks(
        spark=spark,
        events_table="Stg_mx_events",
        refresh_month="2024-01",
        specific_checks=["completeness", "validity"],  # Optional: run specific checks
    ) 