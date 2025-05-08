"""
Volume checks for data quality validation.
"""

from typing import List, Dict, Optional
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    when,
    date_format,
    expr,
    desc,
)

from src.checks.base import BaseCheck
from src.core.spark_utils import get_table
from src.config.settings import (
    DB_NAME,
    RAW_SCHEMA,
    PATIENT_COUNT_DEV_THRESHOLD,
    RECORD_COUNT_DEV_THRESHOLD,
)

class VolumeCheck(BaseCheck):
    """Checks for volume patterns in the events table."""
    
    def run(self) -> List[Dict]:
        """Runs all volume checks."""
        # Load data for both current and previous months
        self._load_data()
        
        # Run checks
        self._check_record_counts()
        self._check_patient_counts()
        self._check_code_counts()
        self._check_provider_metrics()
        self._check_organization_metrics()
        self._check_payer_metrics()
        self._check_cohort_metrics()
        
        return self.results
    
    def _load_data(self):
        """Loads data for both current and previous months."""
        # Get base table
        base_df_full = get_table(self.spark, DB_NAME, RAW_SCHEMA, self.events_table_name)
        
        if self.sample_rows > 0:
            print(f"VolumeCheck: Applying .limit({self.sample_rows}) to internally loaded data for {self.events_table_name}.")
            base_df = base_df_full.limit(self.sample_rows)
        else:
            base_df = base_df_full

        # Initialize columns
        self.provider_col = None
        self.org_col = None
        self.payer_col = None
        self.cohort_col = None
        self.code_col = None
        self.code_metric_name = "Distinct Code Count"
        
        # Check for cohort column
        if "cohort_id" in base_df.columns:
            self.cohort_col = "cohort_id"
            print(f"Found cohort_id column in {self.events_table_name}, will include cohort metrics.")
        else:
            print(f"Warning: 'cohort_id' column not found in {self.events_table_name}, skipping cohort counts.")
        
        # Define table-specific columns
        if self.events_table_name == "Stg_rx_events":
            # Code column for RX events
            if "ndc11" in base_df.columns:
                self.code_col = "ndc11"
                self.code_metric_name = "Distinct NDC11 Count"
                print(f"Found ndc11 column in {self.events_table_name}, will include NDC11 metrics.")
            else:
                print(f"Warning: 'ndc11' column not found in {self.events_table_name}, skipping NDC11 counts.")
            
            # For RX events, use prescriber_npi for HCP
            if "prescriber_npi" in base_df.columns:
                self.provider_col = "prescriber_npi"
                print(f"Found prescriber_npi column in {self.events_table_name}, will include HCP metrics.")
            else:
                print(f"Warning: 'prescriber_npi' column not found in {self.events_table_name}, skipping HCP counts.")
            
            # For RX events, use pharmacy_npi for HCO
            if "pharmacy_npi" in base_df.columns:
                self.org_col = "pharmacy_npi"
                print(f"Found pharmacy_npi column in {self.events_table_name}, will include HCO metrics.")
            else:
                print(f"Warning: 'pharmacy_npi' column not found in {self.events_table_name}, skipping HCO counts.")
            
            # For RX events, there's no direct payer column identified
            print(f"Note: No payer column found in {self.events_table_name}, skipping payer counts.")
            
        else:  # Default to Stg_mx_events
            # Code column for MX events
            if "procedure_code" in base_df.columns:
                self.code_col = "procedure_code"
                self.code_metric_name = "Distinct Procedure Code Count"
                print(f"Found procedure_code column in {self.events_table_name}, will include procedure code metrics.")
            else:
                print(f"Warning: 'procedure_code' column not found in {self.events_table_name}, skipping procedure code counts.")
            
            # For MX events, use rendering_npi for HCP
            if "rendering_npi" in base_df.columns:
                self.provider_col = "rendering_npi"
                print(f"Found rendering_npi column in {self.events_table_name}, will include HCP metrics.")
            else:
                print(f"Warning: 'rendering_npi' column not found in {self.events_table_name}, skipping HCP counts.")
            
            # For MX events, use billing_npi for HCO
            if "billing_npi" in base_df.columns:
                self.org_col = "billing_npi"
                print(f"Found billing_npi column in {self.events_table_name}, will include HCO metrics.")
            else:
                print(f"Warning: 'billing_npi' column not found in {self.events_table_name}, skipping HCO counts.")
            
            # For MX events, use kh_plan_id for payer
            if "kh_plan_id" in base_df.columns:
                self.payer_col = "kh_plan_id"
                print(f"Found kh_plan_id column in {self.events_table_name}, will include payer metrics.")
            else:
                print(f"Warning: 'kh_plan_id' column not found in {self.events_table_name}, skipping payer counts.")
        
        # Build aggregation columns (this part remains the same)
        agg_cols = [
            count("*").alias("record_count"),
            countDistinct("patient_id").alias("distinct_patient_count")
        ]
        
        if self.code_col:
            agg_cols.append(countDistinct(self.code_col).alias("distinct_code_count"))
        
        if self.provider_col:
            agg_cols.append(countDistinct(self.provider_col).alias("distinct_provider_count"))
            agg_cols.append(expr(f"count(distinct case when {self.provider_col} is not null then patient_id end)").alias("patients_with_provider"))
        
        if self.org_col:
            agg_cols.append(countDistinct(self.org_col).alias("distinct_org_count"))
            agg_cols.append(expr(f"count(distinct case when {self.org_col} is not null then patient_id end)").alias("patients_with_org"))
        
        if self.payer_col:
            agg_cols.append(countDistinct(self.payer_col).alias("distinct_payer_count"))
            agg_cols.append(expr(f"count(distinct case when {self.payer_col} is not null then patient_id end)").alias("patients_with_payer"))
        
        if self.cohort_col:
            agg_cols.append(countDistinct(self.cohort_col).alias("distinct_cohort_count"))
            agg_cols.append(expr(f"count(distinct case when {self.cohort_col} is not null then patient_id end)").alias("patients_with_cohort"))
        
        # Load current month data
        self.current_counts = (
            base_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.refresh_month)
            .agg(*agg_cols)
            .first()
        )
        
        # Load previous month data
        self.previous_counts = (
            base_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.previous_refresh_month)
            .agg(*agg_cols)
            .first()
        )
        
        # Load cohort distribution if available
        if self.cohort_col:
            self.cohort_distribution_current = (
                base_df
                .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.refresh_month)
                .filter(col(self.cohort_col).isNotNull())
                .groupBy(self.cohort_col)
                .agg(
                    count("*").alias("record_count"),
                    countDistinct("patient_id").alias("patient_count")
                )
                .orderBy(desc("patient_count"))
                .collect()
            )
            
            self.cohort_distribution_previous = (
                base_df
                .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.previous_refresh_month)
                .filter(col(self.cohort_col).isNotNull())
                .groupBy(self.cohort_col)
                .agg(
                    count("*").alias("record_count"),
                    countDistinct("patient_id").alias("patient_count")
                )
                .orderBy(desc("patient_count"))
                .collect()
            )
    
    def _check_record_counts(self):
        """Checks record count changes between months."""
        current_count = self.current_counts["record_count"] if self.current_counts else 0
        previous_count = self.previous_counts["record_count"] if self.previous_counts else 0
        
        # Add current count
        self.add_result(
            check_category="volume",
            check_name="record_count",
            metric_name="current_count_rows",
            metric_value=current_count,
            status="INFO",
            details=f"Previous month: {previous_count}"
        )
        
        # Add change percentage if previous count exists
        if previous_count > 0:
            change_pct = ((current_count - previous_count) / previous_count) * 100
            status = "PASS" if abs(change_pct / 100) <= RECORD_COUNT_DEV_THRESHOLD else "WARN"
            self.add_result(
                check_category="volume",
                check_name="record_count",
                metric_name="change_percentage",
                metric_value=change_pct,
                status=status,
                details=f"Change: {change_pct:+.2f}%. Threshold: +/-{RECORD_COUNT_DEV_THRESHOLD:.0%}"
            )
        else:
            self.add_result(
                check_category="volume",
                check_name="record_count",
                metric_name="change_percentage",
                metric_value="N/A",
                status="INFO",
                details="Previous month had 0 records."
            )
    
    def _check_patient_counts(self):
        """Checks patient count changes between months."""
        current_count = self.current_counts["distinct_patient_count"] if self.current_counts else 0
        previous_count = self.previous_counts["distinct_patient_count"] if self.previous_counts else 0
        
        # Add current count
        self.add_result(
            check_category="volume",
            check_name="patient_count",
            metric_name="current_count_patients",
            metric_value=current_count,
            status="INFO",
            details=f"Previous month: {previous_count}"
        )
        
        # Add change percentage if previous count exists
        if previous_count > 0:
            change_pct = ((current_count - previous_count) / previous_count) * 100
            status = "PASS" if abs(change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN"
            self.add_result(
                check_category="volume",
                check_name="patient_count",
                metric_name="change_percentage",
                metric_value=change_pct,
                status=status,
                details=f"Change: {change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
            )
        else:
            self.add_result(
                check_category="volume",
                check_name="patient_count",
                metric_name="change_percentage",
                metric_value="N/A",
                status="INFO",
                details="Previous month had 0 patients."
            )
    
    def _check_code_counts(self):
        """Checks code count changes between months."""
        if not self.code_col:
            self.add_result(
                check_category="volume",
                check_name="code_count",
                metric_name="status",
                metric_value="SKIPPED",
                status="WARN",
                details=f"Relevant code column ({self.code_col}) not found in {self.events_table_name}."
            )
            return
        
        current_count = self.current_counts["distinct_code_count"] if self.current_counts else 0
        previous_count = self.previous_counts["distinct_code_count"] if self.previous_counts else 0
        
        # Add current count
        self.add_result(
            check_category="volume",
            check_name="code_count",
            metric_name="current_count_codes",
            metric_value=current_count,
            status="INFO",
            details=f"Previous month: {previous_count}"
        )
        
        # Add change percentage if previous count exists
        if previous_count > 0:
            change_pct = ((current_count - previous_count) / previous_count) * 100
            status = "PASS" if abs(change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN"
            self.add_result(
                check_category="volume",
                check_name="code_count",
                metric_name="change_percentage",
                metric_value=change_pct,
                status=status,
                details=f"Change: {change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
            )
        else:
            self.add_result(
                check_category="volume",
                check_name="code_count",
                metric_name="change_percentage",
                metric_value="N/A",
                status="INFO",
                details=f"Previous month had 0 distinct {self.code_col}s."
            )
    
    def _check_provider_metrics(self):
        """Checks provider-related metrics."""
        if not self.provider_col:
            return
        
        current_provider_count = self.current_counts["distinct_provider_count"] if self.current_counts else 0
        current_patients_with_provider = self.current_counts["patients_with_provider"] if self.current_counts else 0
        previous_provider_count = self.previous_counts["distinct_provider_count"] if self.previous_counts else 0
        previous_patients_with_provider = self.previous_counts["patients_with_provider"] if self.previous_counts else 0
        
        # Add provider counts
        self.add_result(
            check_category="volume",
            check_name="provider_metrics",
            metric_name="current_provider_count",
            metric_value=current_provider_count,
            status="INFO",
            details=f"Previous month: {previous_provider_count}"
        )
        
        # Add patients with provider counts
        self.add_result(
            check_category="volume",
            check_name="provider_metrics",
            metric_name="current_patients_with_provider",
            metric_value=current_patients_with_provider,
            status="INFO",
            details=f"Previous month: {previous_patients_with_provider}"
        )
        
        # Add provider count change if previous count exists
        if previous_provider_count > 0:
            change_pct = ((current_provider_count - previous_provider_count) / previous_provider_count) * 100
            status = "PASS" if abs(change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN"
            self.add_result(
                check_category="volume",
                check_name="provider_metrics",
                metric_name="provider_count_change_percentage",
                metric_value=change_pct,
                status=status,
                details=f"Change: {change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
            )
        else:
            self.add_result(
                check_category="volume",
                check_name="provider_metrics",
                metric_name="provider_count_change_percentage",
                metric_value="N/A",
                status="INFO",
                details="Previous month had 0 providers."
            )
    
    def _check_organization_metrics(self):
        """Checks organization-related metrics."""
        if not self.org_col:
            return
        
        current_org_count = self.current_counts["distinct_org_count"] if self.current_counts else 0
        current_patients_with_org = self.current_counts["patients_with_org"] if self.current_counts else 0
        previous_org_count = self.previous_counts["distinct_org_count"] if self.previous_counts else 0
        previous_patients_with_org = self.previous_counts["patients_with_org"] if self.previous_counts else 0
        
        # Add organization counts
        self.add_result(
            check_category="volume",
            check_name="organization_metrics",
            metric_name="current_org_count",
            metric_value=current_org_count,
            status="INFO",
            details=f"Previous month: {previous_org_count}"
        )
        
        # Add patients with organization counts
        self.add_result(
            check_category="volume",
            check_name="organization_metrics",
            metric_name="current_patients_with_org",
            metric_value=current_patients_with_org,
            status="INFO",
            details=f"Previous month: {previous_patients_with_org}"
        )
        
        # Add organization count change if previous count exists
        if previous_org_count > 0:
            change_pct = ((current_org_count - previous_org_count) / previous_org_count) * 100
            status = "PASS" if abs(change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN"
            self.add_result(
                check_category="volume",
                check_name="organization_metrics",
                metric_name="org_count_change_percentage",
                metric_value=change_pct,
                status=status,
                details=f"Change: {change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
            )
        else:
            self.add_result(
                check_category="volume",
                check_name="organization_metrics",
                metric_name="org_count_change_percentage",
                metric_value="N/A",
                status="INFO",
                details="Previous month had 0 organizations."
            )
    
    def _check_payer_metrics(self):
        """Checks payer-related metrics."""
        if not self.payer_col:
            return
        
        current_payer_count = self.current_counts["distinct_payer_count"] if self.current_counts else 0
        current_patients_with_payer = self.current_counts["patients_with_payer"] if self.current_counts else 0
        previous_payer_count = self.previous_counts["distinct_payer_count"] if self.previous_counts else 0
        previous_patients_with_payer = self.previous_counts["patients_with_payer"] if self.previous_counts else 0
        
        # Add payer counts
        self.add_result(
            check_category="volume",
            check_name="payer_metrics",
            metric_name="current_payer_count",
            metric_value=current_payer_count,
            status="INFO",
            details=f"Previous month: {previous_payer_count}"
        )
        
        # Add patients with payer counts
        self.add_result(
            check_category="volume",
            check_name="payer_metrics",
            metric_name="current_patients_with_payer",
            metric_value=current_patients_with_payer,
            status="INFO",
            details=f"Previous month: {previous_patients_with_payer}"
        )
        
        # Add payer count change if previous count exists
        if previous_payer_count > 0:
            change_pct = ((current_payer_count - previous_payer_count) / previous_payer_count) * 100
            status = "PASS" if abs(change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN"
            self.add_result(
                check_category="volume",
                check_name="payer_metrics",
                metric_name="payer_count_change_percentage",
                metric_value=change_pct,
                status=status,
                details=f"Change: {change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
            )
        else:
            self.add_result(
                check_category="volume",
                check_name="payer_metrics",
                metric_name="payer_count_change_percentage",
                metric_value="N/A",
                status="INFO",
                details="Previous month had 0 payers."
            )
    
    def _check_cohort_metrics(self):
        """Checks cohort-related metrics."""
        if not self.cohort_col:
            return
        
        current_cohort_count = self.current_counts["distinct_cohort_count"] if self.current_counts else 0
        current_patients_with_cohort = self.current_counts["patients_with_cohort"] if self.current_counts else 0
        previous_cohort_count = self.previous_counts["distinct_cohort_count"] if self.previous_counts else 0
        previous_patients_with_cohort = self.previous_counts["patients_with_cohort"] if self.previous_counts else 0
        
        # Add cohort counts
        self.add_result(
            check_category="volume",
            check_name="cohort_metrics",
            metric_name="current_cohort_count",
            metric_value=current_cohort_count,
            status="INFO",
            details=f"Previous month: {previous_cohort_count}"
        )
        
        # Add patients with cohort counts
        self.add_result(
            check_category="volume",
            check_name="cohort_metrics",
            metric_name="current_patients_with_cohort",
            metric_value=current_patients_with_cohort,
            status="INFO",
            details=f"Previous month: {previous_patients_with_cohort}"
        )
        
        # Add cohort count change if previous count exists
        if previous_cohort_count > 0:
            change_pct = ((current_cohort_count - previous_cohort_count) / previous_cohort_count) * 100
            status = "PASS" if abs(change_pct / 100) <= PATIENT_COUNT_DEV_THRESHOLD else "WARN"
            self.add_result(
                check_category="volume",
                check_name="cohort_metrics",
                metric_name="cohort_count_change_percentage",
                metric_value=change_pct,
                status=status,
                details=f"Change: {change_pct:+.2f}%. Threshold: +/-{PATIENT_COUNT_DEV_THRESHOLD:.0%}"
            )
        else:
            self.add_result(
                check_category="volume",
                check_name="cohort_metrics",
                metric_name="cohort_count_change_percentage",
                metric_value="N/A",
                status="INFO",
                details="Previous month had 0 cohorts."
            )
        
        # Check for significant cohort distribution changes
        if self.cohort_distribution_previous and len(self.cohort_distribution_previous) > 0:
            # Create dictionaries for easier comparison
            current_cohort_dict = {row[self.cohort_col]: row["patient_count"] for row in self.cohort_distribution_current}
            previous_cohort_dict = {row[self.cohort_col]: row["patient_count"] for row in self.cohort_distribution_previous}
            
            # Find cohorts with significant changes
            significant_changes = []
            for cohort_id, current_count in current_cohort_dict.items():
                if cohort_id in previous_cohort_dict and previous_cohort_dict[cohort_id] > 0:
                    prev_count = previous_cohort_dict[cohort_id]
                    change_pct = ((current_count - prev_count) / prev_count) * 100
                    if abs(change_pct) >= 20:  # 20% threshold for significant change
                        significant_changes.append(f"{cohort_id}: {change_pct:+.1f}%")
            
            # Find new cohorts (in current but not in previous)
            new_cohorts = [cohort_id for cohort_id in current_cohort_dict.keys()
                          if cohort_id not in previous_cohort_dict]
            
            # Find missing cohorts (in previous but not in current)
            missing_cohorts = [cohort_id for cohort_id in previous_cohort_dict.keys()
                             if cohort_id not in current_cohort_dict]
            
            # Add cohort distribution changes
            if significant_changes or new_cohorts or missing_cohorts:
                details = []
                if significant_changes:
                    details.append(f"Significant changes: {', '.join(significant_changes)}")
                if new_cohorts:
                    details.append(f"New cohorts: {', '.join(map(str, new_cohorts))}")
                if missing_cohorts:
                    details.append(f"Missing cohorts: {', '.join(map(str, missing_cohorts))}")
                
                self.add_result(
                    check_category="volume",
                    check_name="cohort_metrics",
                    metric_name="cohort_distribution_changes",
                    metric_value=len(significant_changes) + len(new_cohorts) + len(missing_cohorts),
                    status="WARN",
                    details="; ".join(details)
                ) 