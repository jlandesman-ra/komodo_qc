# databricks_batch_runner.py (or batch_runner.py)

import datetime
from dateutil.relativedelta import relativedelta
import sys
import os

# --- Modified section for path determination ---
try:
    # This works when the script is run directly as a .py file (e.g., in a Databricks Python Script Job task)
    script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(script_path)
    print(f"Script path detected using __file__: {script_path}")
except NameError:
    # __file__ is not defined, likely because the script is being run in an interactive
    # environment (e.g., a Databricks notebook cell using %run or exec).
    # Fallback to using the current working directory.
    # This assumes the notebook's current working directory is the project root.
    project_root = os.getcwd()
    print(f"Warning: __file__ not defined. Using current working directory as project root: {project_root}")
    print("Ensure this notebook or script is run from the root of your 'komodo_qc' project.")

src_path = os.path.join(project_root, "src")

# Add src_path to sys.path so Python can find modules within the 'src' directory
if src_path not in sys.path:
    sys.path.insert(0, src_path)
    print(f"Added to sys.path: {src_path}")
else:
    print(f"Already in sys.path: {src_path}")

# Optional: Also add project_root if you ever need to import 'src' as a package itself,
# or if you have other top-level folders (e.g., 'tests') you might want to reference from.
if project_root not in sys.path:
    sys.path.insert(0, project_root) # Allows 'from src import ...' if preferred
    print(f"Added to sys.path: {project_root}")

# --- End of modified section ---

# Now you can import from your src modules
# These imports should work if src_path is correctly added and points to your 'src' directory
try:
    from run_checks import run_checks as execute_data_quality_checks # From src/run_checks.py
    from core.spark_utils import get_spark_session                 # From src/core/spark_utils.py
    from config.settings import DB_NAME, DEV_SCHEMA              # From src/config/settings.py
    print("Successfully imported custom modules from 'src'.")
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Please check that the 'src' directory is correctly located relative to your project root,")
    print(f"and that project_root is: {project_root}")
    print(f"and src_path is: {src_path}")
    print(f"Current sys.path: {sys.path}")
    raise # Re-raise the import error to stop execution if imports fail

# ... (rest of your run_monthly_checks_databricks function and if __name__ == "__main__": block) ...

def run_monthly_checks_databricks(start_year, start_month, end_year, end_month, events_table, sample_rows=0, checks_to_run_list=None):
    """
    Runs the data quality checks for a range of months on Databricks.

    Args:
        start_year (int): The year of the first month to process.
        start_month (int): The first month to process (1-12).
        end_year (int): The year of the last month to process.
        end_month (int): The last month to process (1-12).
        events_table (str): Name of the events table to check.
        sample_rows (int): Number of rows to sample. Defaults to 0 (all rows).
        checks_to_run_list (list, optional): Specific checks to run. Defaults to ['all'].
    """
    spark = get_spark_session() 
    
    current_processing_date = datetime.date(start_year, start_month, 1)
    final_processing_date = datetime.date(end_year, end_month, 1)


    checks_to_run_list = ['completeness','consistency,','uniqueness','distribution','temporal','validity','volume']

    print(f"Starting batch run for events table: {events_table}")
    print(f"Output will be saved to the table specified by RESULTS_TABLE in src/config/settings.py.")
    print("Please ensure RESULTS_TABLE in src/config/settings.py is set to your desired NEW table name if you intend to create a new one.")
    print("-" * 30)

    while current_processing_date <= final_processing_date:
        refresh_month_str = current_processing_date.strftime("%Y-%m")
        
        previous_month_date = current_processing_date - relativedelta(months=1)
        previous_refresh_month_str = previous_month_date.strftime("%Y-%m")
        
        print(f"Running checks for REFRESH_MONTH: {refresh_month_str}")
        print(f"Using PREVIOUS_REFRESH_MONTH: {previous_refresh_month_str}")

        try:
            monthly_results = execute_data_quality_checks(
                spark=spark,
                events_table_name=events_table,
                current_refresh_month=refresh_month_str,
                previous_refresh_month=previous_refresh_month_str,
                checks_to_run=checks_to_run_list
            )
            print(f"Successfully completed checks for {refresh_month_str}.")
            
        except Exception as e:
            print(f"An error occurred while running checks for {refresh_month_str}: {e}")
            # You might want to log the full traceback here for better debugging
            import traceback
            traceback.print_exc()


        print("-" * 30)
        current_processing_date += relativedelta(months=1)

    print("Batch run finished.")


if __name__ == "__main__":
    # --- Configuration for the Databricks Job ---
    run_start_year = 2024
    run_start_month = 11 
    run_end_year = 2025
    run_end_month = 4    

    target_events_table = "Stg_mx_events" 
    num_sample_rows = 0 
    specific_checks = ['all'] 

    print("Starting Databricks batch runner script...")
    run_monthly_checks_databricks(
        run_start_year, 
        run_start_month, 
        run_end_year, 
        run_end_month,
        target_events_table,
        sample_rows=num_sample_rows,
        checks_to_run_list=specific_checks
    )
    print("Databricks batch runner script completed.")