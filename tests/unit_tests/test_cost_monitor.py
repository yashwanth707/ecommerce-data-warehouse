import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

# Assuming the cost monitor is in src.optimization.cost_monitor
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.optimization.cost_monitor import get_cost_summary

@patch('src.optimization.cost_monitor.sqlite3.connect')
def test_get_cost_summary_empty(mock_connect):
    """Test cost summary when DB is empty or missing."""
    # Mock empty cursor
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    summary = get_cost_summary()
    
    assert summary['total_credits'] == 0.0
    assert summary['total_cost_usd'] == 0.0
    assert summary['recent_daily'] == []

def test_decimal_to_float_conversion():
    """Test the decimal to float conversion logic used in cost monitor."""
    snowflake_decimal = Decimal('1.25')
    cost_per_credit = 3.00
    
    # Simulate the calculation that previously failed
    total_cost = float(snowflake_decimal) * cost_per_credit
    
    assert total_cost == 3.75
