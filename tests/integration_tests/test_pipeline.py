"""
Integration Tests — Pipeline End-to-End
========================================
Tests require live Snowflake connection.
Mark with @pytest.mark.integration to skip in CI without creds.
"""

import os
import pytest

# Skip all tests if Snowflake creds not available
pytestmark = pytest.mark.skipif(
    not os.getenv("SNOWFLAKE_ACCOUNT"),
    reason="Snowflake credentials not configured",
)


@pytest.fixture(scope="module")
def snowflake_connection():
    """Create a Snowflake connection for integration tests."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW"),
    )
    yield conn
    conn.close()


class TestBronzeLayer:
    """Integration tests for Bronze layer tables."""

    def test_bronze_tables_exist(self, snowflake_connection):
        """All Bronze tables should exist after ingestion."""
        cur = snowflake_connection.cursor()
        cur.execute("SHOW TABLES IN SCHEMA RAW")
        tables = [row[1] for row in cur.fetchall()]

        expected = [
            "BRONZE_ORDERS", "BRONZE_CUSTOMERS", "BRONZE_PRODUCTS",
            "BRONZE_ORDER_ITEMS", "BRONZE_PAYMENTS", "BRONZE_REVIEWS",
        ]
        for table in expected:
            assert table in tables, f"Missing table: {table}"

    def test_bronze_has_data(self, snowflake_connection):
        """Bronze tables should have rows after ingestion."""
        cur = snowflake_connection.cursor()
        cur.execute("SELECT COUNT(*) FROM RAW.BRONZE_ORDERS")
        count = cur.fetchone()[0]
        assert count > 0, "bronze_orders is empty"


class TestGoldLayer:
    """Integration tests for Gold layer tables."""

    def test_fact_orders_has_data(self, snowflake_connection):
        """Fact orders table should be populated."""
        cur = snowflake_connection.cursor()
        cur.execute("SELECT COUNT(*) FROM ANALYTICS.FACT_ORDERS")
        count = cur.fetchone()[0]
        assert count > 0

    def test_dim_customers_has_data(self, snowflake_connection):
        """Customer dimension should be populated."""
        cur = snowflake_connection.cursor()
        cur.execute("SELECT COUNT(*) FROM ANALYTICS.DIM_CUSTOMERS")
        count = cur.fetchone()[0]
        assert count > 0

    def test_referential_integrity(self, snowflake_connection):
        """All fact orders should have matching customer keys."""
        cur = snowflake_connection.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM ANALYTICS.FACT_ORDERS f
            LEFT JOIN ANALYTICS.DIM_CUSTOMERS c
                ON f.customer_key = c.customer_key
            WHERE c.customer_key IS NULL
        """)
        orphans = cur.fetchone()[0]
        assert orphans == 0, f"{orphans} orphan records in fact_orders"
