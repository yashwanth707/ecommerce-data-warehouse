# __init__.py for ingestion module
from .s3_to_snowflake import DataIngestion

__all__ = ["DataIngestion"]
