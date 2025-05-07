"""
Distribution checks for data quality validation.
"""

from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format, expr, isnull, upper, when, count, desc, lit, year 

from src.checks.base import BaseCheck
from src.core.spark_utils import get_table
from src.config.settings import (
    DB_NAME,
    RAW_SCHEMA,
    DEMO_TABLE,
    GEO_TABLE,
)

class DistributionCheck(BaseCheck):
    """Checks for data distributions in the events table."""
    
    def run(self) -> List[Dict]:
        """Runs all distribution checks."""
        # Determine table type
        is_rx = self.events_table_name== "Stg_rx_events"
        
        # Load supporting data
        self._load_supporting_data()
        
        # Run checks
        self._check_gender_distribution()
        self._check_age_distribution()
        self._check_geographic_distribution()
        self._check_provider_distribution()
        self._check_cohort_distribution(is_rx)
        
        return self.results
    
    def _load_supporting_data(self):
        """Loads supporting data tables."""

        self.demo_df = (
            get_table(self.spark, DB_NAME, RAW_SCHEMA, DEMO_TABLE)
            .filter(F.date_format(col("kh_refresh_date"), "yyyy-MM") <= self.refresh_month)
            .withColumn("rn", expr("row_number() OVER (PARTITION BY patient_id ORDER BY kh_refresh_date DESC)"))
            .filter(col("rn") == 1)
            # Select patient_yob to calculate age
            .select("patient_id", "patient_gender", "patient_yob", "kh_refresh_date") # Keep kh_refresh_date if needed for age calc relative to record's refresh
            .withColumn(
                "age",
                when(
                    col("patient_yob").isNotNull(),
                    year(F.current_date()) - year(col("patient_yob"))
                ).otherwise(None)
            )
            .withColumn(
                "age_group",
                when(col("age") < 18, "0-17")
                .when((col("age") >= 18) & (col("age") < 45), "18-44")
                .when((col("age") >= 45) & (col("age") < 65), "45-64")
                .when(col("age") >= 65, "65+")
                .otherwise("Unknown")
            )
            .select("patient_id", "patient_gender", "age_group")
            .alias("dm")
        )
        
        # Get geographic data
        self.geo_df = (
            get_table(self.spark, DB_NAME, RAW_SCHEMA, GEO_TABLE)
            .filter(F.date_format(col("kh_refresh_date"), "yyyy-MM") <= self.refresh_month) 
            .select("patient_id", "valid_from_date", "valid_to_date", "patient_state")
            .filter(col("valid_from_date").isNotNull() & col("valid_to_date").isNotNull() & col("patient_state").isNotNull())
            .alias("geo")
        )

    
    def _check_gender_distribution(self):
        """Checks gender distribution of events."""
        if "patient_id" in self.events_df.columns:
            # Alias the patient_id from self.events_df to avoid ambiguity
            events_base_df = self.events_df.select(col("patient_id").alias("event_patient_id_for_join")).distinct()

            events_with_demo = events_base_df.join(
                self.demo_df, 
                col("event_patient_id_for_join") == col("dm.patient_id"), # Use aliased name for the join
                "left"
            )
            
            total_events = events_with_demo.count() # This refers to total rows in the joined df
            if total_events > 0:
                # Calculate gender distribution
                # Aggregations use "dm.patient_gender", which is correctly qualified
                demo_agg_results = events_with_demo.agg(
                    F.count(F.when(F.upper(col("dm.patient_gender")) == 'F', 1)).alias("F_count"),
                    F.count(F.when(F.upper(col("dm.patient_gender")) == 'M', 1)).alias("M_count"),
                    F.count(F.when(F.upper(col("dm.patient_gender")).isNull() | ~F.upper(col("dm.patient_gender")).isin('F','M'), 1)).alias("Other_Gender_count")
                ).first()
                
                # ... rest of the method remains the same ...
                # (Ensure F is used for spark functions like F.count, F.when, F.upper if that's the convention)

    def _check_age_distribution(self):
        """Checks age group distribution of events."""
        if "patient_id" in self.events_df.columns:
            # Alias the patient_id from self.events_df to avoid ambiguity
            events_base_df = self.events_df.select(col("patient_id").alias("event_patient_id_for_join")).distinct()

            events_with_demo = events_base_df.join(
                self.demo_df, # self.demo_df is already aliased as "dm"
                col("event_patient_id_for_join") == col("dm.patient_id"), # Use aliased name for the join
                "left"
            )
            
            total_events = events_with_demo.count()
            if total_events > 0:
                # Calculate age group distribution
                # Aggregations use "dm.age_group", which is correctly qualified
                demo_agg_results = events_with_demo.agg(
                    F.count(F.when(col("dm.age_group") == '0-17', 1)).alias("Age_0_17_count"),
                    F.count(F.when(col("dm.age_group") == '18-44', 1)).alias("Age_18_44_count"),
                    F.count(F.when(col("dm.age_group") == '45-64', 1)).alias("Age_45_64_count"),
                    F.count(F.when(col("dm.age_group") == '65+', 1)).alias("Age_65_plus_count"),
                    F.count(F.when(col("dm.age_group") == 'Unknown', 1)).alias("Age_Unknown_count")
                ).first()
                
                # Format distribution string
                age_dist_parts = []
                age_map = {
                    "0-17": demo_agg_results["Age_0_17_count"],
                    "18-44": demo_agg_results["Age_18_44_count"],
                    "45-64": demo_agg_results["Age_45_64_count"],
                    "65+": demo_agg_results["Age_65_plus_count"],
                    "Unknown": demo_agg_results["Age_Unknown_count"]
                }
                
                for age_group, count_val in age_map.items():
                    if count_val > 0:
                        age_dist_parts.append(f"{age_group}: {count_val/total_events:.1%}")
                
                age_dist_str = ", ".join(sorted(age_dist_parts)) if age_dist_parts else "N/A"
                
                # Check for significant imbalances
                age_percentages = [count_val/total_events * 100 for count_val in age_map.values()]
                status = "WARN" if any(p < 5 for p in age_percentages[:-1]) or age_percentages[-1] > 10 else "PASS"
                
                self.add_result(
                    check_category="distribution",
                    check_name="age_distribution",
                    metric_name="distribution",
                    metric_value=age_dist_str,
                    status=status,
                    details=f"Based on {total_events} events with linked demo data",
                )
            else:
                self.add_result(
                    check_category="distribution",
                    check_name="age_distribution",
                    metric_name="status",
                    metric_value="SKIPPED",
                    status="INFO",
                    details="No events could be linked to demographic data",
                )
    
    def _check_geographic_distribution(self):
        """Checks geographic distribution of events."""
        if "patient_id" in self.events_df.columns:
            date_column = "fill_date" if "fill_date" in self.events_df.columns else "service_date"
            
            events_df_for_join = self.events_df.select(
                self.events_df["patient_id"],
                self.events_df[date_column]
            )
            # Join events with geographic data
            events_with_geo = events_df_for_join.join(
                self.geo_df, # self.geo_df is aliased as "geo" in _load_supporting_data
                (events_df_for_join["patient_id"] == col("geo.patient_id")) & 
                (events_df_for_join[date_column] >= col("geo.valid_from_date")) & # Also qualify date_column for safety
                (events_df_for_join[date_column] <= col("geo.valid_to_date")),
                "left"
            )
            
            # Count events by state
            state_counts = events_with_geo.groupBy(col("geo.patient_state")).agg( # Using col() as per imports
                count("*").alias("event_count")
            )
            
            total_events = events_with_geo.count()
            if total_events > 0:
                # Calculate state distribution
                state_dist = (
                    state_counts
                    .withColumn("percentage", (col("event_count") / total_events) * 100)
                    .orderBy(col("event_count").desc())
                    .limit(10)
                    .orderBy(col("percentage").desc())
                )
                
                # Format distribution string
                state_dist_rows = state_dist.collect()
                # The column from groupBy("geo.patient_state") will likely be named "patient_state"
                state_dist_str = ", ".join(
                    [f"{row.patient_state}: {row.percentage:.1f}%" for row in state_dist_rows]
                ) if state_dist_rows else "N/A"
                
                # Check for significant imbalances
                top_state_percent = state_dist_rows[0].percentage if state_dist_rows else 0
                status = "WARN" if top_state_percent > 50 else "PASS"
                
                self.add_result(
                    check_category="distribution",
                    check_name="state_distribution",
                    metric_name="distribution",
                    metric_value=state_dist_str,
                    status=status,
                    details=f"Based on {total_events} events with linked geo data",
                )
            else:
                self.add_result(
                    check_category="distribution",
                    check_name="state_distribution",
                    metric_name="status",
                    metric_value="SKIPPED",
                    status="INFO",
                    details="No events could be linked to geographic data",
                )
    def _check_provider_distribution(self):
        """Checks provider distribution of events."""
        if "patient_id" in self.events_df.columns:
            # Get provider columns based on table type
            provider_cols = ["pharmacy_npi", "prescriber_npi"] if "fill_date" in self.events_df.columns else ["billing_npi", "rendering_npi", "referring_npi"]
            
            for provider_col in provider_cols:
                if provider_col in self.events_df.columns:
                    # Count events by provider
                    provider_counts = self.events_df.groupBy(provider_col).agg(
                        count("*").alias("event_count")
                    ).filter(col(provider_col).isNotNull())
                    
                    total_events = provider_counts.agg(F.sum("event_count")).first()[0]
                    if total_events > 0:
                        # Calculate provider distribution
                        provider_dist = (
                            provider_counts
                            .withColumn("percentage", (col("event_count") / total_events) * 100)
                            .orderBy(col("event_count").desc())
                            .limit(10)
                            .orderBy(col("percentage").desc())
                        )
                        
                        # Format distribution string
                        provider_dist_rows = provider_dist.collect()
                        provider_dist_str = ", ".join(
                            [f"{row[provider_col]}: {row.percentage:.1f}%" for row in provider_dist_rows]
                        ) if provider_dist_rows else "N/A"
                        
                        # Check for significant imbalances
                        top_provider_percent = provider_dist_rows[0].percentage if provider_dist_rows else 0
                        status = "WARN" if top_provider_percent > 20 else "PASS"
                        
                        self.add_result(
                            check_category="distribution",
                            check_name=f"{provider_col}_distribution",
                            metric_name="distribution",
                            metric_value=provider_dist_str,
                            status=status,
                            details=f"Based on {total_events} events with {provider_col}",
                        )
                    else:
                        self.add_result(
                            check_category="distribution",
                            check_name=f"{provider_col}_distribution",
                            metric_name="status",
                            metric_value="SKIPPED",
                            status="INFO",
                            details=f"No events with {provider_col}",
                        )
    
    def _check_cohort_distribution(self, is_rx: bool):
        """Checks cohort distribution of events."""
        if "patient_id" in self.events_df.columns:
            # Get cohort column based on table type
            cohort_col = "rx_cohort" if is_rx else "mx_cohort"
            
            if cohort_col in self.events_df.columns:
                # Count events by cohort
                cohort_counts = self.events_df.groupBy(cohort_col).agg(
                    count("*").alias("event_count"),
                    countDistinct("patient_id").alias("patient_count")
                ).filter(col(cohort_col).isNotNull())
                
                total_events = cohort_counts.agg(F.sum("event_count")).first()[0]
                if total_events > 0:
                    # Calculate cohort distribution
                    cohort_dist = (
                        cohort_counts
                        .withColumn("percentage", (col("event_count") / total_events) * 100)
                        .orderBy(col("patient_count").desc())
                        .limit(10)
                    )
                    
                    # Format distribution string
                    cohort_dist_rows = cohort_dist.collect()
                    cohort_dist_str = ", ".join(
                        [f"{row[cohort_col]}: {row.patient_count} patients" for row in cohort_dist_rows]
                    ) if cohort_dist_rows else "N/A"
                    
                    # Check for significant imbalances
                    top_cohort_percent = cohort_dist_rows[0].percentage if cohort_dist_rows else 0
                    status = "WARN" if top_cohort_percent > 50 else "PASS"
                    
                    self.add_result(
                        check_category="distribution",
                        check_name="cohort_distribution",
                        metric_name="distribution",
                        metric_value=cohort_dist_str,
                        status=status,
                        details=f"Based on {total_events} events with {cohort_col}",
                    )
                else:
                    self.add_result(
                        check_category="distribution",
                        check_name="cohort_distribution",
                        metric_name="status",
                        metric_value="SKIPPED",
                        status="INFO",
                        details=f"No events with {cohort_col}",
                    ) 