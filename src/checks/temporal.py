"""
Temporal checks for data quality validation.
"""

from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    when,
    date_format,
    expr,
)

from src.checks.base import BaseCheck
from src.core.spark_utils import get_table
from src.config.settings import (
    DB_NAME,
    RAW_SCHEMA,
    PATIENT_COUNT_DEV_THRESHOLD,
)

class TemporalCheck(BaseCheck):
    """Checks for temporal patterns in the events table."""
    
    def run(self) -> List[Dict]:
        """Runs all temporal checks."""
        # Load data for both current and previous months
        self._load_data()
        
        # Run checks
        self._check_patient_overlap()
        self._check_new_patients()
        self._check_patient_retention()
        self._check_medication_changes()
        
        return self.results
    
    def _load_data(self):
        """Loads data for both current and previous months."""
        # Check if ndc11 column exists
        base_df = get_table(self.spark, DB_NAME, RAW_SCHEMA, self.events_df.name)
        self.has_ndc11 = "ndc11" in base_df.columns
        
        # Select columns
        select_cols = ["patient_id", "kh_refresh_date"]
        if self.has_ndc11:
            select_cols.append("ndc11")
        
        # Load current month data
        self.current_events = (
            base_df
            .select(*select_cols)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.current_refresh_month)
            .alias("curr")
        )
        
        # Load previous month data
        self.previous_events = (
            base_df
            .select(*select_cols)
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.previous_refresh_month)
            .alias("prev")
        )
    
    def _check_patient_overlap(self):
        """Checks patient overlap between current and previous months."""
        # Get distinct patients for each month
        current_patients = self.current_events.select("patient_id").distinct().withColumnRenamed("patient_id", "curr_patient_id")
        previous_patients = self.previous_events.select("patient_id").distinct().withColumnRenamed("patient_id", "prev_patient_id")
        
        # Calculate overlap metrics
        patient_overlap_agg = current_patients.join(
            previous_patients,
            current_patients["curr_patient_id"] == previous_patients["prev_patient_id"],
            "full_outer"
        ).agg(
            countDistinct(col("curr_patient_id")).alias("current_patient_count"),
            countDistinct(col("prev_patient_id")).alias("previous_patient_count"),
            countDistinct(when(col("curr_patient_id").isNotNull() & col("prev_patient_id").isNotNull(), col("curr_patient_id"))).alias("overlapping_patients"),
            countDistinct(when(col("curr_patient_id").isNotNull() & col("prev_patient_id").isNull(), col("curr_patient_id"))).alias("new_patients")
        ).first()
        
        # Extract counts
        current_patient_count = patient_overlap_agg["current_patient_count"] if patient_overlap_agg else 0
        previous_patient_count = patient_overlap_agg["previous_patient_count"] if patient_overlap_agg else 0
        overlapping_patients = patient_overlap_agg["overlapping_patients"] if patient_overlap_agg else 0
        new_patients = patient_overlap_agg["new_patients"] if patient_overlap_agg else 0
        
        # Calculate percentages
        overlap_percentage = (overlapping_patients / previous_patient_count * 100) if previous_patient_count > 0 else 0
        new_percentage = (new_patients / current_patient_count * 100) if current_patient_count > 0 else 0
        
        # Determine status
        status = "WARN" if overlap_percentage < 50 or new_percentage > 50 else "PASS"
        
        # Add results
        self.add_result(
            check_category="temporal",
            check_name="patient_overlap",
            metric_name="overlap_percentage",
            metric_value=overlap_percentage,
            status=status,
            details=f"Overlap: {overlapping_patients}/{previous_patient_count} patients ({overlap_percentage:.1f}%)",
        )
        
        self.add_result(
            check_category="temporal",
            check_name="patient_overlap",
            metric_name="new_patients_percentage",
            metric_value=new_percentage,
            status=status,
            details=f"New: {new_patients}/{current_patient_count} patients ({new_percentage:.1f}%)",
        )
    
    def _check_new_patients(self):
        """Checks for new patients in the current month."""
        # Get new patients
        new_patients = self.current_events.join(
            self.previous_events,
            self.current_events.patient_id == self.previous_events.patient_id,
            "left_anti"
        ).select("patient_id").distinct()
        
        new_patient_count = new_patients.count()
        total_patient_count = self.current_events.select("patient_id").distinct().count()
        
        # Calculate percentage
        new_patient_percentage = (new_patient_count / total_patient_count * 100) if total_patient_count > 0 else 0
        
        # Determine status
        status = "WARN" if new_patient_percentage > 50 else "PASS"
        
        # Add result
        self.add_result(
            check_category="temporal",
            check_name="new_patients",
            metric_name="new_patients_percentage",
            metric_value=new_patient_percentage,
            status=status,
            details=f"New patients: {new_patient_count}/{total_patient_count} ({new_patient_percentage:.1f}%)",
        )
    
    def _check_patient_retention(self):
        """Checks patient retention between months."""
        # Get returning patients
        returning_patients = self.current_events.join(
            self.previous_events,
            self.current_events.patient_id == self.previous_events.patient_id,
            "inner"
        ).select("patient_id").distinct()
        
        returning_count = returning_patients.count()
        previous_count = self.previous_events.select("patient_id").distinct().count()
        
        # Calculate retention rate
        retention_rate = (returning_count / previous_count * 100) if previous_count > 0 else 0
        
        # Determine status
        status = "WARN" if retention_rate < 50 else "PASS"
        
        # Add result
        self.add_result(
            check_category="temporal",
            check_name="patient_retention",
            metric_name="retention_rate",
            metric_value=retention_rate,
            status=status,
            details=f"Retention: {returning_count}/{previous_count} patients ({retention_rate:.1f}%)",
        )
    
    def _check_medication_changes(self):
        """Checks for significant changes in medication patterns."""
        if self.has_ndc11:
            # Get patient counts by NDC for both months
            current_ndc_counts = (
                self.current_events
                .groupBy("ndc11")
                .agg(countDistinct("patient_id").alias("current_patient_count"))
                .filter(col("ndc11").isNotNull())
            )
            
            previous_ndc_counts = (
                self.previous_events
                .groupBy("ndc11")
                .agg(countDistinct("patient_id").alias("previous_patient_count"))
                .filter(col("ndc11").isNotNull())
            )
            
            # Join and calculate changes
            ndc_changes = current_ndc_counts.join(
                previous_ndc_counts,
                "ndc11",
                "full_outer"
            ).fillna(0).withColumn(
                "patient_count_change",
                col("current_patient_count") - col("previous_patient_count")
            ).withColumn(
                "percent_change",
                when(col("previous_patient_count") > 0,
                     (col("patient_count_change") / col("previous_patient_count")) * 100)
                .otherwise(0)
            )
            
            # Find significant changes
            significant_changes = ndc_changes.filter(
                (abs(col("percent_change")) > 50) &  # More than 50% change
                (col("current_patient_count") >= 10)  # At least 10 current patients
            )
            
            # Format change details
            change_details = []
            for row in significant_changes.orderBy(abs(col("percent_change")).desc()).limit(5).collect():
                pct_str = f"{row.percent_change:+.1f}%"
                change_details.append(
                    f"NDC:{row.ndc11}(Prev:{row.previous_patient_count}, Curr:{row.current_patient_count}, Chg:{row.patient_count_change:+d} [{pct_str}])"
                )
            
            # Determine status
            status = "WARN" if significant_changes.count() > 0 else "PASS"
            
            # Add result
            self.add_result(
                check_category="temporal",
                check_name="medication_changes",
                metric_name="significant_changes_count",
                metric_value=significant_changes.count(),
                status=status,
                details=f"{significant_changes.count()} NDCs had >50% patient count change (min 10 current patients). Top changes: {'; '.join(change_details)}",
            )
        else:
            self.add_result(
                check_category="temporal",
                check_name="medication_changes",
                metric_name="status",
                metric_value="SKIPPED",
                status="INFO",
                details="NDC11 column not available for medication change analysis",
            ) 