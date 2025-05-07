"""
Main script to run data quality checks.
"""

import argparse
import logging
from typing import List, Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from src.checks.completeness import CompletenessCheck
from src.checks.consistency import ConsistencyCheck
from src.checks.distribution import DistributionCheck
from src.checks.temporal import TemporalCheck
from src.checks.validity import ValidityCheck
from src.checks.volume import VolumeCheck
from src.checks.uniqueness import UniquenessCheck
from src.core.spark_utils import get_spark_session, get_table, save_results
from src.config.settings import (
    DB_NAME,
    RAW_SCHEMA,
    STAGING_SCHEMA,
    REFRESH_MONTH,
    PREVIOUS_REFRESH_MONTH
)

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run data quality checks')
    parser.add_argument('--checks', nargs='+', 
                      choices=['all', 'completeness', 'consistency', 'distribution', 
                              'temporal', 'validity', 'volume', 'uniqueness'],
                      default=['all'],
                      help='Specify which checks to run (default: all)')
    parser.add_argument('--refresh-month', type=str,
                      help='Override the refresh month from settings')
    parser.add_argument('--previous-refresh-month', type=str,
                      help='Override the previous refresh month from settings')
    parser.add_argument('--events-table', type=str, required=True,
                      help='Name of the events table to check')
    parser.add_argument('--sample-rows', type=int, default=0,
                      help='Number of rows to sample from the events table for faster iteration (default: 0, meaning all rows). Uses .limit() for speed.')
    return parser.parse_args()

def run_checks(
    spark: SparkSession,
    events_table_name: str,
    current_refresh_month: str,
    previous_refresh_month: str,
    checks_to_run: List[str],
    sample_rows: int=0
) -> List[Dict]:
    """
    Run all data quality checks.
    
    Args:
        spark: SparkSession instance
        events_table_name: Name of the events table to check
        current_refresh_month: Current refresh month in YYYY-MM format
        previous_refresh_month: Previous refresh month in YYYY-MM format
        checks_to_run: List of check names to run
        
    Returns:
        List of check results
    """
    # Get events table
    events_df_full = get_table(spark, DB_NAME, RAW_SCHEMA, events_table_name)
    print('table loaded')
    if events_df_full is None:
        raise ValueError(f"Events table {events_table_name} not found")
    
    if sample_rows > 0:
        print(f"Sampling {sample_rows} rows from {events_table_name} using .limit() for faster iteration.")
        events_df = events_df_full.limit(sample_rows)
    else:
        events_df = events_df_full


    # Initialize check classes
    check_classes = {
        'completeness': CompletenessCheck,
        'consistency': ConsistencyCheck,
        'uniqueness': UniquenessCheck,
        'distribution': DistributionCheck,
        'temporal': TemporalCheck,
        'validity': ValidityCheck,
        'volume': VolumeCheck
    }
    
    # Run selected checks
    all_results = []
    for check_name in checks_to_run:
        if check_name in check_classes:
            check_class = check_classes[check_name]
            # Initialize check with current refresh month
            print(f'running {check_name}, \ncurrent_refresh_month = {current_refresh_month}, \nevents_table_name = {events_table_name}')
            check = check_class(
                spark=spark,
                events_df=events_df,
                refresh_month=current_refresh_month,
                events_table_name=events_table_name
            )
            # Set previous refresh month if needed
            if hasattr(check, 'previous_refresh_month'):
                check.previous_refresh_month = previous_refresh_month
            results = check.run()
            all_results.extend(results)

        print(f'finished {check_name}')
        results_df = spark.createDataFrame(all_results)
        # Save results
        save_results(spark, results_df)
    
    return all_results

def main():
    """Main entry point."""
    # Parse command line arguments
    args = parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    # Get refresh months
    current_refresh_month = args.refresh_month or REFRESH_MONTH
    previous_refresh_month = args.previous_refresh_month or PREVIOUS_REFRESH_MONTH
    print('list of classes')
    # Define check classes
    check_classes = {
        'completeness': CompletenessCheck,
        'consistency': ConsistencyCheck,
        'distribution': DistributionCheck,
        'temporal': TemporalCheck,
        'validity': ValidityCheck,
        'volume': VolumeCheck
    }
    
    # Determine which checks to run
    checks_to_run = check_classes.keys() if 'all' in args.checks else args.checks
    
    spark = None
    try:
        print('initialize spark')
        # Initialize Spark session
        spark = get_spark_session()

        print(f'running checks, sample rows = {args.sample_rows}')
        # Run checks
        results = run_checks(
            spark=spark,
            events_table_name=args.events_table,
            current_refresh_month=current_refresh_month,
            previous_refresh_month=previous_refresh_month,
            checks_to_run=checks_to_run,
            sample_rows=args.sample_rows
        )
        
        # Log results
        for result in results:
            logger.info(
                f"Check: {result['check_category']}.{result['check_name']} - "
                f"Status: {result['status']} - "
                f"Metric: {result['metric_name']} = {result['metric_value']} - "
                f"Details: {result['details']}"
            )
        
    except Exception as e:
        logger.error(f"Error running checks: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main() 