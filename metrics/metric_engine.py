"""
Metric Computation Engine - Calculates business metrics.

BUG INVENTORY:
- BUG-065: Division by zero in ratio metrics
- BUG-066: Rolling window doesn't handle gaps in data
- BUG-067: Aggregation overflow on large datasets
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import defaultdict


class MetricEngine:
    """Computes business metrics from ingested data."""

    def __init__(self):
        self.metrics_cache: Dict[str, float] = {}
        self.time_series: Dict[str, List[tuple]] = defaultdict(list)

    def compute_conversion_rate(self, visitors: int, conversions: int) -> float:
        """
        Compute conversion rate.

        BUG-065: No zero-division check when visitors is 0.
        """
        # BUG-065: Crashes if visitors == 0
        return (conversions / visitors) * 100

    def compute_churn_rate(self, start_count: int, end_count: int,
                           new_customers: int) -> float:
        """
        Compute customer churn rate.

        BUG-065: Division by zero if start_count is 0.
        """
        churned = (start_count + new_customers) - end_count
        # BUG-065: ZeroDivisionError if start_count == 0
        return (churned / start_count) * 100

    def compute_rolling_average(self, metric_name: str,
                                 window_days: int = 7) -> float:
        """
        Compute rolling average for a metric.

        BUG-066: Assumes data points are daily with no gaps.
        If data has gaps (e.g., weekends), the window is wrong.
        """
        data = self.time_series.get(metric_name, [])
        if not data:
            return 0.0

        # BUG-066: Takes last N points, not last N days
        window = data[-window_days:]  # Should filter by date range
        values = [v for _, v in window]

        return sum(values) / len(values) if values else 0.0

    def compute_revenue_metrics(self, transactions: List[Dict]) -> dict:
        """
        Compute revenue-related metrics.

        BUG-067: Sums all values as float - precision loss on large datasets.
        """
        if not transactions:
            return {"total_revenue": 0, "avg_order_value": 0, "count": 0}

        # BUG-067: Float accumulation for large sums
        total = 0.0
        for txn in transactions:
            total += txn.get("amount", 0)  # Float precision compounds

        return {
            "total_revenue": round(total, 2),
            "avg_order_value": round(total / len(transactions), 2),
            "count": len(transactions),
            "max_transaction": max(t.get("amount", 0) for t in transactions),
            "min_transaction": min(t.get("amount", 0) for t in transactions),
        }

    def add_data_point(self, metric_name: str, value: float,
                       timestamp: datetime = None):
        """Add a data point for time series analysis."""
        ts = timestamp or datetime.utcnow()
        self.time_series[metric_name].append((ts, value))

    def get_trend(self, metric_name: str, periods: int = 10) -> str:
        """Determine if metric is trending up, down, or stable."""
        data = self.time_series.get(metric_name, [])
        if len(data) < 2:
            return "insufficient_data"

        recent = [v for _, v in data[-periods:]]
        if len(recent) < 2:
            return "insufficient_data"

        mid = len(recent) // 2
        first_half_avg = sum(recent[:mid]) / mid
        second_half_avg = sum(recent[mid:]) / (len(recent) - mid)

        change_pct = ((second_half_avg - first_half_avg) / first_half_avg * 100
                       if first_half_avg != 0 else 0)

        if change_pct > 5:
            return "increasing"
        elif change_pct < -5:
            return "decreasing"
        return "stable"
