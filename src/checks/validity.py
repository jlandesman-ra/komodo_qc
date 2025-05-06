"""
Validity checks for data quality validation.
"""

from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_date, datediff, length, regexp_replace, year, upper

from src.checks.base import BaseCheck
from src.config.settings import (
    EXPECTED_PROC_CODE_TYPES,
    EXPECTED_UNIT_TYPES,
    EXTREME_UNITS_THRESHOLD,
)

class ValidityCheck(BaseCheck):
    """Checks for data validity in the events table."""
    
    def run(self) -> List[Dict]:
        """Runs all validity checks."""
        # Determine table type
        is_rx = self.events_df.name == "Stg_rx_events"
        
        # Common checks
        self._check_dates(is_rx)
        self._check_code_formats(is_rx)
        self._check_unit_values(is_rx)
        self._check_provider_ids(is_rx)
        
        # Table-specific checks
        if is_rx:
            self._check_rx_specific()
        else:
            self._check_mx_specific()
            
        return self.results
    
    def _check_dates(self, is_rx: bool):
        """Checks for valid dates."""
        date_column = "fill_date" if is_rx else "service_date"
        
        # Check for future dates
        future_dates = self.events_df.filter(F.col(date_column) > current_date()).count()
        total_count = self.events_df.count()
        future_date_percent = (future_dates / total_count) * 100 if total_count > 0 else 0
        
        self.add_result(
            check_category="validity",
            check_name="future_dates",
            metric_name="future_date_percent",
            metric_value=future_date_percent,
            status="FAIL" if future_date_percent > 0 else "PASS",
            details=f"Percentage of records with {date_column} in the future",
        )
        
        # Check for old dates (before 1900)
        old_dates = self.events_df.filter(year(F.col(date_column)) < 1900).count()
        old_date_percent = (old_dates / total_count) * 100 if total_count > 0 else 0
        
        self.add_result(
            check_category="validity",
            check_name="old_dates",
            metric_name="old_date_percent",
            metric_value=old_date_percent,
            status="WARN" if old_date_percent > 0 else "PASS",
            details=f"Percentage of records with {date_column} before 1900",
        )
    
    def _check_code_formats(self, is_rx: bool):
        """Checks for valid code formats."""
        # Check NDC format
        if "ndc11" in self.events_df.columns:
            invalid_ndc = self.events_df.filter(
                F.col("ndc11").isNotNull() & 
                (length(regexp_replace(F.col("ndc11"), "-", "")) != 11)
            ).count()
            total_count = self.events_df.count()
            invalid_ndc_percent = (invalid_ndc / total_count) * 100 if total_count > 0 else 0
            
            self.add_result(
                check_category="validity",
                check_name="ndc_format",
                metric_name="invalid_ndc_percent",
                metric_value=invalid_ndc_percent,
                status="WARN" if invalid_ndc_percent > 0 else "PASS",
                details="Percentage of records with invalid NDC format (should be 11 digits)",
            )
        
        # Check procedure code type (MX only)
        if not is_rx and "procedure_code_type" in self.events_df.columns:
            invalid_proc_type = self.events_df.filter(
                F.col("procedure_code_type").isNotNull() & 
                ~upper(F.col("procedure_code_type")).isin(EXPECTED_PROC_CODE_TYPES)
            ).count()
            total_count = self.events_df.count()
            invalid_proc_type_percent = (invalid_proc_type / total_count) * 100 if total_count > 0 else 0
            
            self.add_result(
                check_category="validity",
                check_name="procedure_code_type",
                metric_name="invalid_proc_type_percent",
                metric_value=invalid_proc_type_percent,
                status="WARN" if invalid_proc_type_percent > 0 else "PASS",
                details=f"Percentage of records with invalid procedure code type. Expected: {', '.join(EXPECTED_PROC_CODE_TYPES)}",
            )
    
    def _check_unit_values(self, is_rx: bool):
        """Checks for valid unit values."""
        if is_rx:
            # Check quantity
            if "quantity" in self.events_df.columns:
                negative_quantity = self.events_df.filter(
                    F.col("quantity").isNotNull() & (F.col("quantity") <= 0)
                ).count()
                total_count = self.events_df.count()
                negative_quantity_percent = (negative_quantity / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="validity",
                    check_name="quantity_positive",
                    metric_name="negative_quantity_percent",
                    metric_value=negative_quantity_percent,
                    status="FAIL" if negative_quantity_percent > 0 else "PASS",
                    details="Percentage of records with non-positive quantity",
                )
                
                # Check extreme quantity
                extreme_quantity = self.events_df.filter(
                    F.col("quantity").isNotNull() & (F.col("quantity") > EXTREME_UNITS_THRESHOLD)
                ).count()
                extreme_quantity_percent = (extreme_quantity / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="validity",
                    check_name="quantity_extreme",
                    metric_name="extreme_quantity_percent",
                    metric_value=extreme_quantity_percent,
                    status="WARN" if extreme_quantity_percent > 0 else "PASS",
                    details=f"Percentage of records with quantity > {EXTREME_UNITS_THRESHOLD}",
                )
            
            # Check days supply
            if "days_supply" in self.events_df.columns:
                negative_days = self.events_df.filter(
                    F.col("days_supply").isNotNull() & (F.col("days_supply") <= 0)
                ).count()
                total_count = self.events_df.count()
                negative_days_percent = (negative_days / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="validity",
                    check_name="days_supply_positive",
                    metric_name="negative_days_percent",
                    metric_value=negative_days_percent,
                    status="FAIL" if negative_days_percent > 0 else "PASS",
                    details="Percentage of records with non-positive days supply",
                )
        else:
            # Check units
            if "units" in self.events_df.columns:
                negative_units = self.events_df.filter(
                    F.col("units").isNotNull() & (F.col("units") <= 0)
                ).count()
                total_count = self.events_df.count()
                negative_units_percent = (negative_units / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="validity",
                    check_name="units_positive",
                    metric_name="negative_units_percent",
                    metric_value=negative_units_percent,
                    status="FAIL" if negative_units_percent > 0 else "PASS",
                    details="Percentage of records with non-positive units",
                )
                
                # Check extreme units
                extreme_units = self.events_df.filter(
                    F.col("units").isNotNull() & (F.col("units") > EXTREME_UNITS_THRESHOLD)
                ).count()
                extreme_units_percent = (extreme_units / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="validity",
                    check_name="units_extreme",
                    metric_name="extreme_units_percent",
                    metric_value=extreme_units_percent,
                    status="WARN" if extreme_units_percent > 0 else "PASS",
                    details=f"Percentage of records with units > {EXTREME_UNITS_THRESHOLD}",
                )
            
            # Check unit type
            if "unit_type" in self.events_df.columns:
                invalid_unit_type = self.events_df.filter(
                    F.col("unit_type").isNotNull() & 
                    ~upper(F.col("unit_type")).isin(EXPECTED_UNIT_TYPES)
                ).count()
                total_count = self.events_df.count()
                invalid_unit_type_percent = (invalid_unit_type / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="validity",
                    check_name="unit_type",
                    metric_name="invalid_unit_type_percent",
                    metric_value=invalid_unit_type_percent,
                    status="WARN" if invalid_unit_type_percent > 0 else "PASS",
                    details=f"Percentage of records with invalid unit type. Expected: {', '.join(EXPECTED_UNIT_TYPES)}",
                )
    
    def _check_provider_ids(self, is_rx: bool):
        """Checks for valid provider IDs."""
        npi_columns = ["pharmacy_npi", "prescriber_npi"] if is_rx else ["billing_npi", "rendering_npi", "referring_npi"]
        
        for npi_col in npi_columns:
            if npi_col in self.events_df.columns:
                invalid_npi = self.events_df.filter(
                    F.col(npi_col).isNotNull() & 
                    ((length(F.col(npi_col)) != 10) | (F.col(npi_col).rlike("[^0-9]")))
                ).count()
                total_count = self.events_df.count()
                invalid_npi_percent = (invalid_npi / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="validity",
                    check_name=f"{npi_col}_format",
                    metric_name=f"invalid_{npi_col}_percent",
                    metric_value=invalid_npi_percent,
                    status="WARN" if invalid_npi_percent > 0 else "PASS",
                    details=f"Percentage of records with invalid {npi_col} format (should be 10 digits)",
                )
    
    def _check_rx_specific(self):
        """Checks specific to RX events."""
        # Check fill date vs prescription written date
        if "fill_date" in self.events_df.columns and "date_prescription_written" in self.events_df.columns:
            invalid_order = self.events_df.filter(
                F.col("date_prescription_written").isNotNull() & 
                (F.col("fill_date") < F.col("date_prescription_written"))
            ).count()
            total_count = self.events_df.count()
            invalid_order_percent = (invalid_order / total_count) * 100 if total_count > 0 else 0
            
            self.add_result(
                check_category="validity",
                check_name="fill_date_order",
                metric_name="invalid_order_percent",
                metric_value=invalid_order_percent,
                status="WARN" if invalid_order_percent > 0 else "PASS",
                details="Percentage of records with fill_date before date_prescription_written",
            )
    
    def _check_mx_specific(self):
        """Checks specific to MX events."""
        # Check service to date
        if "service_to_date" in self.events_df.columns:
            future_service_to_date = self.events_df.filter(
                F.col("service_to_date").isNotNull() & 
                (F.col("service_to_date") > current_date())
            ).count()
            total_count = self.events_df.count()
            future_service_to_date_percent = (future_service_to_date / total_count) * 100 if total_count > 0 else 0
            
            self.add_result(
                check_category="validity",
                check_name="future_service_to_date",
                metric_name="future_service_to_date_percent",
                metric_value=future_service_to_date_percent,
                status="FAIL" if future_service_to_date_percent > 0 else "PASS",
                details="Percentage of records with service_to_date in the future",
            ) 