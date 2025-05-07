"""
Completeness checks for data quality validation.
"""

from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.checks.base import BaseCheck
from src.config.settings import NULL_PERCENT_THRESHOLD

class CompletenessCheck(BaseCheck):
    """Checks for data completeness in the events table."""
    
    def run(self) -> List[Dict]:
        """Runs all completeness checks."""
        self._check_required_fields()
        self._check_optional_fields()
        self._check_logical_completeness()
        return self.results
    
        
    def _check_null_percentage(
        self, column: str, threshold: float = NULL_PERCENT_THRESHOLD
    ) -> float:
        """Calculates the percentage of NULL values in a column."""
        total_count = self.events_df.count()
        if total_count == 0:
            return 0.0
        
        null_count = self.events_df.filter(F.col(column).isNull()).count()
        null_percentage = (null_count / total_count) * 100
        
        return null_percentage

    def _check_required_fields(self):
        """Checks for NULL values in required fields."""
        # Determine table type and required fields
        if self.events_table_name == "Stg_rx_events":
            required_fields = [
                "pharmacy_event_id",
                "patient_id",
                "fill_date",
                "diagnosis_code",
                "pharmacy_npi"
            ]
        else:  # Default to Stg_mx_events
            required_fields = [
                "medical_event_id",
                "patient_id",
                "service_date",
                "procedure_code",
                "billing_npi"
            ]
        
        for field in required_fields:
            if field not in self.events_df.columns:
                self.add_result(
                    check_category="completeness",
                    check_name="required_fields",
                    metric_name=f"{field}_not_found",
                    metric_value=0,
                    status="WARN",
                    details=f"Required field {field} not found in table",
                )
                continue
                
            null_percentage = self._check_null_percentage(field)
            status = "FAIL" if null_percentage > 0 else "PASS"
            self.add_result(
                check_category="completeness",
                check_name="required_fields",
                metric_name=f"{field}_null_percentage",
                metric_value=null_percentage,
                status=status,
                details=f"NULL percentage for required field {field}",
            )
    
    def _check_optional_fields(self):
        """Checks for NULL values in optional fields."""
        # Determine table type and optional fields
        if self.events_table_name == "Stg_rx_events":
            optional_fields = [
                "ndc11",
                "days_supply",
                "quantity",
                "prescriber_npi",
                "brand_name",
                "generic_name",
                "route",
                "transaction_result",
                "reject_codes",
                "transaction_number",
                "transaction_status",
                "fill_number",
                "number_of_refills_authorized",
                "daw_code",
                "date_prescription_written",
                "primary_kh_plan_id",
                "secondary_kh_plan_id",
                "patient_responsibility",
                "patient_oop"
            ]
        else:  # Default to Stg_mx_events
            optional_fields = [
                "service_to_date",
                "units",
                "unit_type",
                "rendering_npi",
                "referring_npi",
                "ndc11"
            ]
        
        for field in optional_fields:
            if field not in self.events_df.columns:
                continue
                
            null_percentage = self._check_null_percentage(field)
            status = "WARN" if null_percentage > NULL_PERCENT_THRESHOLD else "PASS"
            self.add_result(
                check_category="completeness",
                check_name="optional_fields",
                metric_name=f"{field}_null_percentage",
                metric_value=null_percentage,
                status=status,
                details=f"NULL percentage for optional field {field}",
            )
    
    def _check_logical_completeness(self):
        """Checks for logical completeness issues."""
        # Check procedure code type consistency (MX only)
        if self.events_table_name == "Stg_mx_events":
            if "procedure_code" in self.events_df.columns and "procedure_code_type" in self.events_df.columns:
                proc_code_null_type_not_null = self.events_df.filter(
                    F.col("procedure_code").isNull() & 
                    F.col("procedure_code_type").isNotNull()
                ).count()
                
                status = "PASS" if proc_code_null_type_not_null == 0 else "WARN"
                self.add_result(
                    check_category="completeness",
                    check_name="procedure_code_type_consistency",
                    metric_name="inconsistent_count",
                    metric_value=proc_code_null_type_not_null,
                    status=status,
                    details="Records with NULL procedure_code but non-NULL procedure_code_type",
                ) 