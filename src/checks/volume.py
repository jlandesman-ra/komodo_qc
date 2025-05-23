from typing import List, Dict, Optional, Any
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
    """
    Calculates volume metrics for the specified refresh_month.
    Does not perform inter-month comparisons.
    """
    
    def __init__(self, spark: SparkSession, events_table_name: str, refresh_month: str, 
                 previous_refresh_month: Optional[str] = None, # Kept for BaseCheck
                 sample_rows: int = 0):
        super().__init__(spark, events_table_name, refresh_month, previous_refresh_month, sample_rows)
        self.events_df: Optional[DataFrame] = None
        self.current_counts: Optional[Row] = None
        self.cohort_distribution_current: Optional[List[Row]] = None
        self.provider_col: Optional[str] = None
        self.org_col: Optional[str] = None
        self.payer_col: Optional[str] = None
        self.cohort_col: Optional[str] = None
        self.code_col: Optional[str] = None
        self.code_metric_name: str = "Distinct Code Count"

    def run(self) -> List[Dict[str, Any]]:
        print(f"Starting Volume checks for table '{self.events_table_name}' for month {self.refresh_month}")
        try:
            self._load_and_aggregate_current_data()
            
            if self.events_df is None or self.current_counts is None:
                print(f"Error: Data not loaded or aggregated for {self.events_table_name} and month {self.refresh_month}. Skipping checks.")
                # self.add_result might have already been called in _load_and_aggregate_current_data if base_df was None
                if self.events_df is not None and self.current_counts is None: # Specific case where aggregation might have failed
                     self.add_result(
                        check_category="Volume", check_name="Setup", metric_name="Data Aggregation",
                        metric_value="ERROR", status="FAIL", details=f"Could not aggregate data for {self.refresh_month}."
                    )
                return self.results

            self._check_record_counts()
            self._check_patient_counts()
            self._check_code_counts()
            self._check_provider_metrics()
            self._check_organization_metrics()
            self._check_payer_metrics()
            self._check_cohort_metrics()
        
        except Exception as e:
            print(f"Critical error during VolumeCheck run for {self.events_table_name} (month: {self.refresh_month}): {e}")
            self.add_result(
                check_category="Volume", check_name="Overall Execution", metric_name="Run Status",
                metric_value="ERROR", status="FAIL", details=f"Unhandled exception: {str(e)[:200]}"
            )
        
        print(f"Finished volume checks for {self.events_table_name}, month {self.refresh_month}.")
        return self.results
    
    def _load_and_aggregate_current_data(self):
        """Loads data and prepares aggregations for the current refresh_month."""
        print(f"Loading base data for {self.events_table_name}...")
        base_df_full = get_table(self.spark, DB_NAME, RAW_SCHEMA, self.events_table_name)
        
        if self.sample_rows > 0:
            print(f"VolumeCheck: Applying .limit({self.sample_rows}) for {self.events_table_name}.")
            self.events_df = base_df_full.limit(self.sample_rows)
        else:
            self.events_df = base_df_full

        if not self.events_df or self.events_df.isEmpty():
            details = f"No data loaded for {self.events_table_name}."
            print(f"Warning: {details}")
            self.add_result("Volume", "Data Loading", "Source Data", "EMPTY", "WARN", details)
            self.events_df = None # Ensure it's None to skip further processing
            return

        # Determine Table-Specific Columns
        if "cohort_id" in self.events_df.columns: self.cohort_col = "cohort_id"
        if self.events_table_name == "Stg_rx_events":
            if "ndc11" in self.events_df.columns: self.code_col = "ndc11"; self.code_metric_name = "Distinct NDC11 Count"
            if "prescriber_npi" in self.events_df.columns: self.provider_col = "prescriber_npi"
            if "pharmacy_npi" in self.events_df.columns: self.org_col = "pharmacy_npi"
        else: # Stg_mx_events or other
            if "procedure_code" in self.events_df.columns: self.code_col = "procedure_code"; self.code_metric_name = "Distinct Procedure Code Count"
            if "rendering_npi" in self.events_df.columns: self.provider_col = "rendering_npi"
            if "billing_npi" in self.events_df.columns: self.org_col = "billing_npi"
            if "kh_plan_id" in self.events_df.columns: self.payer_col = "kh_plan_id"

        missing_initial_cols = [c for c in ["kh_refresh_date", "patient_id"] if c not in self.events_df.columns]
        if missing_initial_cols:
            details = f"Missing essential columns ({', '.join(missing_initial_cols)}) in {self.events_table_name}."
            self.add_result("Volume", "Data Loading", "Schema Check", "ERROR", "FAIL", details)
            self.events_df = None; return

        agg_cols = [count("*").alias("record_count"), countDistinct("patient_id").alias("distinct_patient_count")]
        if self.code_col and self.code_col in self.events_df.columns: agg_cols.append(countDistinct(self.code_col).alias("distinct_code_count"))
        if self.provider_col and self.provider_col in self.events_df.columns:
            agg_cols.append(countDistinct(self.provider_col).alias("distinct_provider_count"))
            agg_cols.append(expr(f"count(distinct case when {self.provider_col} is not null then patient_id end)").alias("patients_with_provider"))
        if self.org_col and self.org_col in self.events_df.columns:
            agg_cols.append(countDistinct(self.org_col).alias("distinct_org_count"))
            agg_cols.append(expr(f"count(distinct case when {self.org_col} is not null then patient_id end)").alias("patients_with_org"))
        if self.payer_col and self.payer_col in self.events_df.columns:
            agg_cols.append(countDistinct(self.payer_col).alias("distinct_payer_count"))
            agg_cols.append(expr(f"count(distinct case when {self.payer_col} is not null then patient_id end)").alias("patients_with_payer"))
        if self.cohort_col and self.cohort_col in self.events_df.columns:
            agg_cols.append(countDistinct(self.cohort_col).alias("distinct_cohort_count"))
            agg_cols.append(expr(f"count(distinct case when {self.cohort_col} is not null then patient_id end)").alias("patients_with_cohort"))

        print(f"Aggregating counts for current month: {self.refresh_month} in {self.events_table_name}")
        self.current_counts = (
            self.events_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.refresh_month)
            .agg(*agg_cols)
            .first()
        )
        if not self.current_counts or all(value is None for value in self.current_counts.asDict().values()): # Check if row is empty or all None
            print(f"Warning: No data found for month {self.refresh_month} after filtering {self.events_table_name}. Aggregations might be zero or None.")
            # self.current_counts might be a Row of None values if no data matched the filter.
            # The _get_count_from_row helper will handle None values from the Row.

        if self.cohort_col and self.cohort_col in self.events_df.columns:
            print(f"Calculating current cohort distribution for {self.events_table_name}...")
            self.cohort_distribution_current = (
                self.events_df
                .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.refresh_month)
                .filter(col(self.cohort_col).isNotNull())
                .groupBy(self.cohort_col)
                .agg(count("*").alias("record_count"), countDistinct("patient_id").alias("patient_count"))
                .orderBy(desc("patient_count"))
                .collect()
            )
    
    def _get_count_from_row(self, row: Optional[Row], metric_alias: str, default_value: Any = 0) -> Any:
        if row and metric_alias in row.asDict():
            val = row[metric_alias]
            return val if val is not None else default_value
        return default_value

    def _check_record_counts(self):
        current_count = self._get_count_from_row(self.current_counts, "record_count")
        self.add_result("Volume", "Record Count", "Current Month Value", current_count, "INFO", f"Total records for {self.refresh_month}.")
    
    def _check_patient_counts(self):
        current_count = self._get_count_from_row(self.current_counts, "distinct_patient_count")
        self.add_result("Volume", "Distinct Patient Count", "Current Month Value", current_count, "INFO", f"Distinct patients for {self.refresh_month}.")

    def _check_code_counts(self):
        if not self.code_col or not (self.current_counts and "distinct_code_count" in self.current_counts.asDict()):
            self.add_result("Volume", "Distinct Code Count", "Check Status", "SKIPPED", "INFO",
                            f"Code column ({self.code_col or 'N/A'}) not found or not aggregated for {self.refresh_month}.")
            return
        current_count = self._get_count_from_row(self.current_counts, "distinct_code_count")
        self.add_result("Volume", self.code_metric_name, "Current Month Value", current_count, "INFO", f"Distinct codes for {self.refresh_month}.")

    def _check_provider_metrics(self):
        if not self.provider_col or not self.events_df or not (self.current_counts and "distinct_provider_count" in self.current_counts.asDict()):
            self.add_result("Volume", "HCP Metrics", "Check Status", "SKIPPED", "INFO",
                            f"HCP column ({self.provider_col or 'N/A'}) not found, base_df not loaded, or metric not aggregated for {self.refresh_month}.")
            return

        current_provider_count = self._get_count_from_row(self.current_counts, "distinct_provider_count")
        self.add_result("Volume", "Distinct HCP Count", "Current Month Value", current_provider_count, "INFO", f"Distinct HCPs for {self.refresh_month}.")

        current_provider_patient_df = (
            self.events_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.refresh_month)
            .filter(col(self.provider_col).isNotNull())
            .groupBy(self.provider_col)
            .agg(countDistinct("patient_id").alias("patient_count"))
        )
        
        current_avg_agg = current_provider_patient_df.agg(avg("patient_count").alias("avg_patients_per_provider")).first()
        current_avg_patients_per_provider = self._get_count_from_row(current_avg_agg, "avg_patients_per_provider", 0.0)
        self.add_result("Volume", "Average Patients per HCP", "Current Month Value", round(current_avg_patients_per_provider, 2), "INFO", f"Avg patients per HCP for {self.refresh_month}.")

        top_hcps = current_provider_patient_df.orderBy(desc("patient_count")).limit(TOP_N_DISTRIBUTION).collect()
        if not top_hcps:
             print(f"No HCPs found for patient distribution in current month {self.refresh_month} for {self.events_table_name}.")
        for row_hcp in top_hcps:
            provider_id_val = row_hcp[self.provider_col]
            patient_count_val = row_hcp["patient_count"]
            self.add_result("Volume", "HCP Patient Distribution", f"Provider: {provider_id_val}", patient_count_val, "INFO", f"Patient count for {self.refresh_month}.")

    def _check_organization_metrics(self):
        if not self.org_col or not self.events_df or not (self.current_counts and "distinct_org_count" in self.current_counts.asDict()):
            self.add_result("Volume", "HCO Metrics", "Check Status", "SKIPPED", "INFO",
                            f"HCO column ({self.org_col or 'N/A'}) not found, base_df not loaded, or metric not aggregated for {self.refresh_month}.")
            return

        current_org_count = self._get_count_from_row(self.current_counts, "distinct_org_count")
        self.add_result("Volume", "Distinct HCO Count", "Current Month Value", current_org_count, "INFO", f"Distinct HCOs for {self.refresh_month}.")

        current_org_patient_df = (
            self.events_df
            .filter(date_format(col("kh_refresh_date"), "yyyy-MM") == self.refresh_month)
            .filter(col(self.org_col).isNotNull())
            .groupBy(self.org_col)
            .agg(countDistinct("patient_id").alias("patient_count"))
        )
        current_avg_agg = current_org_patient_df.agg(avg("patient_count").alias("avg_patients_per_org")).first()
        current_avg_patients_per_org = self._get_count_from_row(current_avg_agg, "avg_patients_per_org", 0.0)
        self.add_result("Volume", "Average Patients per HCO", "Current Month Value", round(current_avg_patients_per_org, 2), "INFO", f"Avg patients per HCO for {self.refresh_month}.")

        top_hcos = current_org_patient_df.orderBy(desc("patient_count")).limit(TOP_N_DISTRIBUTION).collect()
        if not top_hcos:
            print(f"No HCOs found for patient distribution in current month {self.refresh_month} for {self.events_table_name}.")
        for row_hco in top_hcos:
            org_id_val = row_hco[self.org_col]
            patient_count_val = row_hco["patient_count"]
            self.add_result("Volume", "HCO Patient Distribution", f"Organization: {org_id_val}", patient_count_val, "INFO", f"Patient count for {self.refresh_month}.")

    def _check_payer_metrics(self):
        if not self.payer_col or not (self.current_counts and "distinct_payer_count" in self.current_counts.asDict()):
            self.add_result("Volume", "Payer Metrics", "Check Status", "SKIPPED", "INFO",
                            f"Payer column ({self.payer_col or 'N/A'}) not found or metric not aggregated for {self.refresh_month}.")
            return
        current_payer_count = self._get_count_from_row(self.current_counts, "distinct_payer_count")
        current_patients_with_payer = self._get_count_from_row(self.current_counts, "patients_with_payer")
        self.add_result("Volume", "Distinct Payer Count", "Current Month Value", current_payer_count, "INFO", f"Distinct payers for {self.refresh_month}.")
        self.add_result("Volume", "Patients with Payer", "Current Month Value", current_patients_with_payer, "INFO", f"Patients with payer for {self.refresh_month}.")

    def _check_cohort_metrics(self):
        if not self.cohort_col or not (self.current_counts and "distinct_cohort_count" in self.current_counts.asDict()):
            self.add_result("Volume", "Cohort Metrics", "Check Status", "SKIPPED", "INFO",
                            f"Cohort column ({self.cohort_col or 'N/A'}) not found or metric not aggregated for {self.refresh_month}.")
            return
        current_cohort_count = self._get_count_from_row(self.current_counts, "distinct_cohort_count")
        current_patients_with_cohort = self._get_count_from_row(self.current_counts, "patients_with_cohort")
        self.add_result("Volume", "Distinct Cohort Count", "Current Month Value", current_cohort_count, "INFO", f"Distinct cohorts for {self.refresh_month}.")
        self.add_result("Volume", "Patients with Cohort", "Current Month Value", current_patients_with_cohort, "INFO", f"Patients with cohort for {self.refresh_month}.")

        if self.cohort_distribution_current and self.cohort_col:
            top_n = min(5, len(self.cohort_distribution_current))
            summary = [f"{row[self.cohort_col]}: {row['patient_count']} patients" for row in self.cohort_distribution_current[:top_n]]
            self.add_result("Volume", "Cohort Patient Distribution", f"Top {top_n} Cohorts", ", ".join(summary)[:200], "INFO", f"Patient counts per cohort for {self.refresh_month}.")
            # Individual cohort patient counts can also be added if needed, similar to HCP/HCO
            for row_cohort in self.cohort_distribution_current[:TOP_N_DISTRIBUTION]: # Or a different TOP_N for cohorts
                cohort_id_val = row_cohort[self.cohort_col]
                patient_count_val = row_cohort["patient_count"]
                self.add_result("Volume", "Individual Cohort Patient Count", f"Cohort: {cohort_id_val}", patient_count_val, "INFO", f"Patient count for {self.refresh_month}.")

