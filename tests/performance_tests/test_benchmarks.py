"""
Performance Tests — Benchmarking Pipeline Components
=====================================================
Measures execution time and resource usage.
"""

import os
import time
import json
import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("SNOWFLAKE_ACCOUNT"),
    reason="Snowflake credentials not configured",
)


@pytest.fixture(scope="module")
def snowflake_connection():
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW"),
    )
    yield conn
    conn.close()


class TestQueryPerformance:
    """Benchmark Gold layer query performance."""

    PERFORMANCE_TARGETS = {
        "simple_count": 3.0,       # seconds
        "aggregation": 5.0,
        "join_query": 10.0,
        "complex_analytics": 15.0,
    }

    def _time_query(self, cursor, sql: str) -> float:
        start = time.time()
        cursor.execute(sql)
        cursor.fetchall()
        return time.time() - start

    def test_simple_count(self, snowflake_connection):
        """Simple COUNT should complete under 3 seconds."""
        cur = snowflake_connection.cursor()
        elapsed = self._time_query(
            cur, "SELECT COUNT(*) FROM ANALYTICS.FACT_ORDERS"
        )
        assert elapsed < self.PERFORMANCE_TARGETS["simple_count"], (
            f"Simple count took {elapsed:.2f}s "
            f"(target: {self.PERFORMANCE_TARGETS['simple_count']}s)"
        )

    def test_aggregation(self, snowflake_connection):
        """GROUP BY aggregation should complete under 5 seconds."""
        cur = snowflake_connection.cursor()
        elapsed = self._time_query(
            cur,
            """
            SELECT order_status, COUNT(*), SUM(payment_value)
            FROM ANALYTICS.FACT_ORDERS
            GROUP BY order_status
            """,
        )
        assert elapsed < self.PERFORMANCE_TARGETS["aggregation"]

    def test_join_query(self, snowflake_connection):
        """Fact-Dimension join should complete under 10 seconds."""
        cur = snowflake_connection.cursor()
        elapsed = self._time_query(
            cur,
            """
            SELECT c.customer_state, COUNT(f.order_id), SUM(f.payment_value)
            FROM ANALYTICS.FACT_ORDERS f
            JOIN ANALYTICS.DIM_CUSTOMERS c ON f.customer_key = c.customer_key
            GROUP BY c.customer_state
            ORDER BY SUM(f.payment_value) DESC
            LIMIT 10
            """,
        )
        assert elapsed < self.PERFORMANCE_TARGETS["join_query"]

    def test_complex_analytics(self, snowflake_connection):
        """Complex multi-join analytics should complete under 15 seconds."""
        cur = snowflake_connection.cursor()
        elapsed = self._time_query(
            cur,
            """
            SELECT
                t.month_name,
                c.customer_state,
                p.category_name_english,
                COUNT(DISTINCT f.order_id) AS orders,
                ROUND(SUM(f.payment_value), 2) AS revenue,
                ROUND(AVG(f.avg_review_score), 2) AS avg_score
            FROM ANALYTICS.FACT_ORDERS f
            JOIN ANALYTICS.DIM_CUSTOMERS c ON f.customer_key = c.customer_key
            JOIN ANALYTICS.DIM_TIME t ON f.order_date_key = t.date_key
            JOIN ANALYTICS.DIM_PRODUCTS p ON 1=1
            WHERE t.year_number = 2018
            GROUP BY 1, 2, 3
            ORDER BY revenue DESC
            LIMIT 50
            """,
        )
        assert elapsed < self.PERFORMANCE_TARGETS["complex_analytics"]
