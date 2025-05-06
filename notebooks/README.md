# Running Data Quality Checks in Databricks

This directory contains Databricks notebooks for running the Komodo Data Quality Checks framework.

## Setup Instructions

1. Upload the Project to Databricks:
   - Create a new Databricks Repo pointing to your Git repository
   - Or use the Databricks UI to upload the project files

2. Configure Your Databricks Environment:
   - Create a new cluster or use an existing one
   - Make sure the cluster has Python 3.x installed
   - The cluster should have access to the necessary databases and schemas

3. Install Dependencies:
   - The notebook will automatically install required packages using `pip install -r ../requirements.txt`
   - If you encounter any issues, you can manually install packages using the cluster's library management

## Running the Checks

### Using the Notebook

1. Open `run_data_quality_checks.py` in your Databricks workspace
2. Attach the notebook to your cluster
3. Run each cell in sequence to:
   - Set up the Python environment
   - Run various combinations of checks
   - View the results

### Available Commands

The notebook demonstrates several ways to run the checks:

1. Run all checks:
```python
%run ../src/run_checks.py --events-table Stg_mx_events
```

2. Run specific checks:
```python
%run ../src/run_checks.py --events-table Stg_rx_events --checks completeness validity
```

3. Override refresh months:
```python
%run ../src/run_checks.py --events-table Stg_mx_events --refresh-month 2024-02 --previous-refresh-month 2024-01
```

## Viewing Results

The results are stored in the table specified by `RESULTS_TABLE` in `settings.py`. The notebook includes a query to view the most recent check results.

## Troubleshooting

1. Python Path Issues:
   - The notebook automatically adds the project root to the Python path
   - If you encounter import errors, verify the project structure and paths

2. Permission Issues:
   - Ensure your Databricks user has access to the required databases and schemas
   - Check that you can read from source tables and write to the results table

3. Dependency Issues:
   - If a package is missing, you can install it using `%pip install package_name`
   - Make sure all required packages are listed in `requirements.txt` 