# Komodo QC Checks

A data quality validation framework for Komodo data processing pipelines.

## Overview

This framework provides a comprehensive set of data quality checks for Komodo data, including:
- Completeness checks
- Consistency checks
- Distribution checks
- Temporal checks
- Validity checks
- Volume checks

The initial version is on `notebooks/Komodo Quality Checks Notebook`.
The scripts here are AI-assisted translation to a script format.

## Usage

Run data quality checks using Databricks CLI:

```bash
python src/run_checks.py --events-table <table_name> [options]
```

Run the data quality checks as a job in Databricks to run all at once using `batch_runner.py`

### Options

- `--checks`: Specify which checks to run (default: all)
  - Available checks: completeness, consistency, distribution, temporal, validity, volume
  - Example: `--checks completeness volume`

- `--refresh-month`: Override the refresh month from settings
  - Format: YYYY-MM
  - Example: `--refresh-month 2024-03`

- `--previous-refresh-month`: Override the previous refresh month from settings
  - Format: YYYY-MM
  - Example: `--previous-refresh-month 2024-02`

- `--events-table`: Name of the events table to check (required)
  - Example: `--events-table Stg_rx_events`

  `--sample-rows`: For faster iteration time, use this parameter to limit the number of rows the code is running on 
   - Example: `--sample-rows 10000`

### Examples

Run all checks:
```bash
python src/run_checks.py --events-table Stg_rx_events
```

Run specific checks:
```bash
python src/run_checks.py --events-table Stg_rx_events --checks completeness volume
```

Run with custom refresh months:
```bash
python src/run_checks.py --events-table Stg_rx_events --refresh-month 2024-03 --previous-refresh-month 2024-02
```

## Project Structure

```
komodo_qc_checks/
├── src/
│   ├── checks/
│   │   ├── completeness.py
│   │   ├── consistency.py
│   │   ├── distribution.py
│   │   ├── temporal.py
│   │   ├── validity.py
│   │   └── volume.py
│   ├── config/
│   │   └── settings.py
│   ├── core/
│   │   ├── logging_utils.py
│   │   └── spark_utils.py
│   └── run_checks.py
├── notebooks/
    |── Komodo Quality Checks Notebook
├── .gitignore
├── README.md
└── requirements.txt
```

## TO DO 

1. Look through each check to make sure the logic accords with business requirements
2. Add in Rainbow Chart over time
3. Add in splits by cohort
4. Adapt thresholds for pass/fail on the tests to more reasonable levels.