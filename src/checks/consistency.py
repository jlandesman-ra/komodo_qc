"""
Consistency checks for data quality validation.
"""

from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format, expr, isnull

from src.checks.base import BaseCheck
from src.core.spark_utils import get_table
from src.config.settings import (
    DB_NAME,
    RAW_SCHEMA,
    DEMO_TABLE,
    PROVIDERS_TABLE,
    ENROLL_TABLE,
    MORTALITY_TABLE,
)

class ConsistencyCheck(BaseCheck):
    """Checks for data consistency in the events table."""
    
    def run(self) -> List[Dict]:
        """Runs all consistency checks."""
        # Determine table type
        is_rx = self.events_df.name == "Stg_rx_events"
        
        # Load supporting data
        self._load_supporting_data()
        
        # Common checks
        self._check_date_relationships(is_rx)
        self._check_patient_demographics()
        self._check_enrollment_periods(is_rx)
        self._check_mortality_dates()
        self._check_provider_consistency(is_rx)
        
        # Add placeholder for demographic stability
        self.add_result(
            check_category="consistency",
            check_name="demographic_stability",
            metric_name="status",
            metric_value="NOT IMPLEMENTED",
            status="INFO",
            details="Requires comparing demo table across refreshes",
        )
        
        return self.results
    
    def _load_supporting_data(self):
        """Loads supporting data tables."""
        # Get latest demographic records per patient
        self.demo_df = (
            get_table(self.spark, DB_NAME, RAW_SCHEMA, DEMO_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= self.current_refresh_month)
            .withColumn("rn", expr("row_number() OVER (PARTITION BY patient_id ORDER BY kh_refresh_date DESC)"))
            .filter(col("rn") == 1)
            .select("patient_id").distinct()
            .alias("dm")
        )
        
        # Get latest provider records
        self.providers_df = (
            get_table(self.spark, DB_NAME, RAW_SCHEMA, PROVIDERS_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= self.current_refresh_month)
            .withColumn("rn", expr("row_number() OVER (PARTITION BY npi ORDER BY kh_refresh_date DESC)"))
            .filter(col("rn") == 1)
            .select("npi").distinct()
            .alias("pv")
        )
        
        # Get enrollment periods
        self.enroll_df = (
            get_table(self.spark, DB_NAME, RAW_SCHEMA, ENROLL_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= self.current_refresh_month)
            .select("patient_id", col("start_date").alias("enroll_start"), col("end_date").alias("enroll_end"))
            .filter(col("enroll_start").isNotNull() & col("enroll_end").isNotNull())
            .alias("en")
        )
        
        # Get mortality data
        self.mortality_df = (
            get_table(self.spark, DB_NAME, RAW_SCHEMA, MORTALITY_TABLE)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") <= self.current_refresh_month)
            .filter(col("patient_death_date").isNotNull())
            .withColumn("rn", expr("row_number() OVER (PARTITION BY patient_id ORDER BY kh_refresh_date DESC)"))
            .filter(col("rn") == 1)
            .select("patient_id", "patient_death_date")
            .alias("mo")
        )
    
    def _check_date_relationships(self, is_rx: bool):
        """Checks for valid date relationships."""
        if is_rx:
            # Check fill date vs prescription written date
            if "fill_date" in self.events_df.columns and "date_prescription_written" in self.events_df.columns:
                invalid_dates = self.events_df.filter(
                    F.col("date_prescription_written").isNotNull() & 
                    (F.col("fill_date") < F.col("date_prescription_written"))
                ).count()
                total_count = self.events_df.count()
                invalid_date_percent = (invalid_dates / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="consistency",
                    check_name="fill_date_order",
                    metric_name="invalid_date_percent",
                    metric_value=invalid_date_percent,
                    status="WARN" if invalid_date_percent > 0 else "PASS",
                    details="Percentage of records with fill_date before date_prescription_written",
                )
        else:
            # Check service date vs service to date
            if "service_date" in self.events_df.columns and "service_to_date" in self.events_df.columns:
                invalid_dates = self.events_df.filter(
                    F.col("service_to_date").isNotNull() & 
                    (F.col("service_date") > F.col("service_to_date"))
                ).count()
                total_count = self.events_df.count()
                invalid_date_percent = (invalid_dates / total_count) * 100 if total_count > 0 else 0
                
                self.add_result(
                    check_category="consistency",
                    check_name="service_date_order",
                    metric_name="invalid_date_percent",
                    metric_value=invalid_date_percent,
                    status="FAIL" if invalid_date_percent > 0 else "PASS",
                    details="Percentage of records with service_date > service_to_date",
                )
    
    def _check_patient_demographics(self):
        """Checks that all patients in events exist in demographics."""
        if "patient_id" in self.events_df.columns:
            # Join events with demographics
            events_demo_join = self.events_df.select("patient_id").distinct().join(
                self.demo_df,
                col("patient_id") == col("dm.patient_id"),
                "left"
            )
            
            # Count patients not in demographics
            missing_patients = events_demo_join.filter(
                isnull(col("dm.patient_id"))
            ).count()
            
            total_patients = self.events_df.select("patient_id").distinct().count()
            missing_patient_percent = (missing_patients / total_patients) * 100 if total_patients > 0 else 0
            
            self.add_result(
                check_category="consistency",
                check_name="patient_demographics",
                metric_name="missing_patient_percent",
                metric_value=missing_patient_percent,
                status="WARN" if missing_patient_percent > 0 else "PASS",
                details="Percentage of patients in events not found in demographics",
            )
    
    def _check_enrollment_periods(self, is_rx: bool):
        """Checks that events occur within enrollment periods."""
        if "patient_id" in self.events_df.columns:
            date_column = "fill_date" if is_rx else "service_date"
            
            # Join events with enrollment periods
            events_enroll_join = self.events_df.select("patient_id", date_column).join(
                self.enroll_df,
                (col("patient_id") == col("en.patient_id")) &
                (col(date_column) >= col("en.enroll_start")) &
                (col(date_column) <= col("en.enroll_end")),
                "left"
            )
            
            # Count events outside enrollment
            events_outside = events_enroll_join.filter(
                isnull(col("en.patient_id"))
            ).count()
            
            total_events = self.events_df.count()
            outside_percent = (events_outside / total_events) * 100 if total_events > 0 else 0
            
            self.add_result(
                check_category="consistency",
                check_name="enrollment_periods",
                metric_name="outside_enrollment_percent",
                metric_value=outside_percent,
                status="WARN" if outside_percent > 0 else "PASS",
                details=f"Percentage of events occurring outside enrollment periods",
            )
    
    def _check_mortality_dates(self):
        """Checks that events don't occur after death date."""
        if "patient_id" in self.events_df.columns:
            date_column = "fill_date" if "fill_date" in self.events_df.columns else "service_date"
            
            # Join events with mortality data
            events_mortality_join = self.events_df.select("patient_id", date_column).join(
                self.mortality_df,
                col("patient_id") == col("mo.patient_id"),
                "inner"
            )
            
            # Count events after death
            events_after_death = events_mortality_join.filter(
                col(date_column) > col("mo.patient_death_date")
            ).count()
            
            total_events = events_mortality_join.count()
            after_death_percent = (events_after_death / total_events) * 100 if total_events > 0 else 0
            
            self.add_result(
                check_category="consistency",
                check_name="mortality_dates",
                metric_name="after_death_percent",
                metric_value=after_death_percent,
                status="FAIL" if after_death_percent > 0 else "PASS",
                details="Percentage of events occurring after patient death date",
            )
    
    def _check_provider_consistency(self, is_rx: bool):
        """Checks that NPIs in events exist in provider table."""
        npi_columns = ["pharmacy_npi", "prescriber_npi"] if is_rx else ["billing_npi", "rendering_npi", "referring_npi"]
        
        for npi_col in npi_columns:
            if npi_col in self.events_df.columns:
                # Get distinct NPIs from events
                distinct_npis = self.events_df.select(npi_col).distinct().filter(
                    F.col(npi_col).isNotNull()
                )
                
                # Join with provider table
                npis_provider_join = distinct_npis.join(
                    self.providers_df,
                    col(npi_col) == col("pv.npi"),
                    "left"
                )
                
                # Count NPIs not in provider table
                missing_npis = npis_provider_join.filter(
                    isnull(col("pv.npi"))
                ).count()
                
                total_npis = distinct_npis.count()
                missing_npi_percent = (missing_npis / total_npis) * 100 if total_npis > 0 else 0
                
                self.add_result(
                    check_category="consistency",
                    check_name=f"{npi_col}_provider",
                    metric_name="missing_npi_percent",
                    metric_value=missing_npi_percent,
                    status="WARN" if missing_npi_percent > 0 else "PASS",
                    details=f"Percentage of {npi_col} values not found in provider table",
                ) 