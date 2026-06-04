"""
Unit Tests — Data Ingestion Module
===================================
Tests the DataIngestion class logic without requiring
live Snowflake or AWS connections.
"""

import json
import hashlib
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime, timezone

import pytest


# Test: Configuration Loading

class TestConfigLoading:
    """Test configuration loading from YAML and environment."""

    @patch.dict("os.environ", {
        "SNOWFLAKE_ACCOUNT": "test.us-east-1",
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_pass",
        "AWS_ACCESS_KEY_ID": "test_key",
        "AWS_SECRET_ACCESS_KEY": "test_secret",
    })
    @patch("snowflake.connector.connect")
    @patch("boto3.client")
    def test_loads_from_env_vars(self, mock_boto, mock_sf):
        """Config should fall back to environment variables."""
        from src.ingestion.s3_to_snowflake import DataIngestion

        ingestion = DataIngestion(config_path=None)
        assert ingestion.config["snowflake"]["account"] == "test.us-east-1"
        assert ingestion.config["aws"]["access_key_id"] == "test_key"

    @patch("snowflake.connector.connect")
    @patch("boto3.client")
    def test_loads_from_yaml(self, mock_boto, mock_sf):
        """Config should load from YAML file when provided."""
        from src.ingestion.s3_to_snowflake import DataIngestion

        yaml_content = """
snowflake:
  account: yaml_account
  user: yaml_user
  password: yaml_pass
  role: SYSADMIN
  warehouse: WH
  database: DB
  schema: RAW
aws:
  access_key_id: yaml_key
  secret_access_key: yaml_secret
  region: us-west-2
  s3_bucket: yaml-bucket
  s3_prefix: data/
ingestion:
  batch_size: 5000
  max_retries: 2
"""
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            with patch("os.path.exists", return_value=True):
                ingestion = DataIngestion(config_path="config.yaml")
                assert ingestion.config["snowflake"]["account"] == "yaml_account"
                assert ingestion.config["ingestion"]["batch_size"] == 5000


# Test: SQL Validation

class TestSQLSafety:
    """Test NL-to-SQL safety guardrails."""

    def test_blocks_insert(self):
        """INSERT statements should be blocked."""
        from src.innovations.nl_to_sql import NLToSQLEngine

        engine = NLToSQLEngine.__new__(NLToSQLEngine)
        with pytest.raises(ValueError, match="Blocked operation"):
            engine._validate_sql("INSERT INTO table VALUES (1)")

    def test_blocks_drop(self):
        """DROP statements should be blocked."""
        from src.innovations.nl_to_sql import NLToSQLEngine

        engine = NLToSQLEngine.__new__(NLToSQLEngine)
        with pytest.raises(ValueError, match="Blocked operation"):
            engine._validate_sql("DROP TABLE customers")

    def test_allows_select(self):
        """Valid SELECT should pass."""
        from src.innovations.nl_to_sql import NLToSQLEngine

        engine = NLToSQLEngine.__new__(NLToSQLEngine)
        sql = engine._validate_sql("SELECT * FROM table LIMIT 10")
        assert sql.startswith("SELECT")

    def test_adds_limit_if_missing(self):
        """LIMIT should be added if not present."""
        from src.innovations.nl_to_sql import NLToSQLEngine

        engine = NLToSQLEngine.__new__(NLToSQLEngine)
        sql = engine._validate_sql("SELECT * FROM table")
        assert "LIMIT" in sql

    def test_rejects_non_select(self):
        """Non-SELECT queries should be rejected."""
        from src.innovations.nl_to_sql import NLToSQLEngine

        engine = NLToSQLEngine.__new__(NLToSQLEngine)
        with pytest.raises(ValueError, match="Only SELECT"):
            engine._validate_sql("CALL some_procedure()")


# Test: Data Hashing

class TestDataHashing:
    """Test checksum generation for idempotency."""

    def test_checksum_consistency(self):
        """Same input should produce same checksum."""
        record = {"order_id": "123", "status": "delivered"}
        raw = json.dumps(record)
        hash1 = hashlib.sha256(raw.encode()).hexdigest()
        hash2 = hashlib.sha256(raw.encode()).hexdigest()
        assert hash1 == hash2

    def test_checksum_uniqueness(self):
        """Different inputs should produce different checksums."""
        record1 = json.dumps({"order_id": "123"})
        record2 = json.dumps({"order_id": "456"})
        assert (
            hashlib.sha256(record1.encode()).hexdigest()
            != hashlib.sha256(record2.encode()).hexdigest()
        )


# Test: SQL Extraction from LLM Response

class TestSQLExtraction:
    """Test extracting SQL from various LLM response formats."""

    def _get_engine(self):
        from src.innovations.nl_to_sql import NLToSQLEngine
        return NLToSQLEngine.__new__(NLToSQLEngine)

    def test_extract_from_code_block(self):
        """SQL wrapped in markdown code block."""
        engine = self._get_engine()
        response = "Here's the query:\n```sql\nSELECT * FROM table\n```"
        sql = engine._extract_sql(response)
        assert sql == "SELECT * FROM table"

    def test_extract_raw_select(self):
        """SQL without code block wrapping."""
        engine = self._get_engine()
        response = "SELECT customer_id FROM dim_customers"
        sql = engine._extract_sql(response)
        assert "SELECT" in sql

    def test_extract_failure(self):
        """Non-SQL response should raise error."""
        engine = self._get_engine()
        with pytest.raises(ValueError, match="Could not extract"):
            engine._extract_sql("I don't know how to answer that")
