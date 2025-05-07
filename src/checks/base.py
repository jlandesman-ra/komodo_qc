"""
Base class for all data quality checks.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import uuid
from datetime import datetime

from src.core.spark_utils import create_result_row
from src.config.settings import NULL_PERCENT_THRESHOLD

class BaseCheck(ABC):
    """Base class for all data quality checks."""
    
    def __init__(self, spark: SparkSession, events_df: DataFrame, refresh_month: str, events_table_name: str):
        self.spark = spark
        self.events_df = events_df
        self.refresh_month = refresh_month
        self.events_table_name = events_table_name
        self.sample_rows = sample_rows
        self.results = []
        # Generate a unique run ID
        self.run_start_datetime = datetime.now() 
        self.run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    def add_result(
        self,
        check_category: str,
        check_name: str,
        metric_name: str,
        metric_value: Any,
        status: str,
        details: Optional[str] = None,
    ):
        """Adds a result to the check results list."""
        result = create_result_row(
            run_id=self.run_id,
            run_start_time_val = self.run_start_datetime,
            events_table_name=self.events_table_name,
            refresh_month=self.refresh_month,
            check_category=check_category,
            check_name=check_name,
            metric_name=metric_name,
            metric_value=metric_value,
            status=status,
            details=details,
        )
        self.results.append(result)
    
    def check_null_percentage(
        self, column: str, threshold: float = NULL_PERCENT_THRESHOLD
    ) -> float:
        """Calculates the percentage of NULL values in a column."""
        total_count = self.events_df.count()
        if total_count == 0:
            return 0.0
        
        null_count = self.events_df.filter(F.col(column).isNull()).count()
        null_percentage = (null_count / total_count) * 100
        
        return null_percentage
    
    @abstractmethod
    def run(self) -> List[Dict]:
        """Runs the check and returns the results."""
        pass 