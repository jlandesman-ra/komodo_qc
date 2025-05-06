"""
Uniqueness checks for data quality validation.
"""

from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.checks.base import BaseCheck

class UniquenessCheck(BaseCheck):
    """Checks for data uniqueness in the events table."""
    
    def run(self) -> List[Dict]:
        """Runs all uniqueness checks."""
        self._check_primary_key_uniqueness()
        self._check_duplicate_records()
        return self.results
    
    def _check_primary_key_uniqueness(self):
        """Checks if primary key values are unique."""
        # Determine the primary key column based on table type
        primary_key = "medical_event_id" if "medical_event_id" in self.events_df.columns else "pharmacy_event_id"
        
        # Count total records and distinct primary keys
        total_count = self.events_df.count()
        distinct_count = self.events_df.select(primary_key).distinct().count()
        
        # Calculate duplicate percentage
        duplicate_percentage = ((total_count - distinct_count) / total_count) * 100 if total_count > 0 else 0
        
        status = "FAIL" if duplicate_percentage > 0 else "PASS"
        self.add_result(
            check_category="uniqueness",
            check_name="primary_key_uniqueness",
            metric_name=f"{primary_key}_duplicate_percentage",
            metric_value=duplicate_percentage,
            status=status,
            details=f"Percentage of duplicate {primary_key} values",
        )
    
    def _check_duplicate_records(self):
        """Checks for duplicate records based on key fields."""
        # Define key fields to check for duplicates
        key_fields = [
            "patient_id",
            "service_date" if "service_date" in self.events_df.columns else "fill_date",
            "procedure_code" if "procedure_code" in self.events_df.columns else "ndc11",
        ]
        
        # Count records with same key field values
        duplicate_df = self.events_df.groupBy(key_fields).count()
        duplicate_count = duplicate_df.filter(F.col("count") > 1).count()
        
        total_count = self.events_df.count()
        duplicate_percentage = (duplicate_count / total_count) * 100 if total_count > 0 else 0
        
        status = "WARN" if duplicate_percentage > 0 else "PASS"
        self.add_result(
            check_category="uniqueness",
            check_name="duplicate_records",
            metric_name="duplicate_record_percentage",
            metric_value=duplicate_percentage,
            status=status,
            details="Percentage of records with duplicate key field combinations",
        ) 