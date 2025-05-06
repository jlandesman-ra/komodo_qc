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

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/komodo_qc_checks.git
cd komodo_qc_checks
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run data quality checks using the command-line interface:

```bash
python src/run_checks.py --events-table <table_name> [options]
```

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
├── tests/
├── .gitignore
├── README.md
└── requirements.txt
```

## Development

1. Create a new branch for your feature:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and commit them:
```bash
git add .
git commit -m "Description of your changes"
```

3. Push your branch and create a pull request:
```bash
git push origin feature/your-feature-name
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 