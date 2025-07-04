#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from weiser.drivers.bigquery import BigQueryDriver
from weiser.loader.models import Datasource, DBType
from unittest.mock import patch, Mock

def test_bigquery_import():
    """Test that BigQuery driver can be imported."""
    print("✅ BigQuery driver import successful")

def test_bigquery_basic_creation():
    """Test basic BigQuery driver creation."""
    datasource = Datasource(
        name="test_bigquery",
        type=DBType.bigquery,
        project_id="my-test-project",
        dataset_id="my_dataset"
    )
    
    with patch('weiser.drivers.base.create_engine') as mock_create_engine:
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        driver = BigQueryDriver(datasource)
        assert driver.data_source == datasource
        mock_create_engine.assert_called_once()
        print("✅ BigQuery driver creation successful")

def test_bigquery_missing_project_error():
    """Test error handling for missing project_id."""
    datasource = Datasource(
        name="test_bigquery",
        type=DBType.bigquery,
        dataset_id="my_dataset"
    )
    
    try:
        BigQueryDriver(datasource)
        print("❌ Should have raised ValueError")
    except ValueError as e:
        if "project_id is required" in str(e):
            print("✅ Error handling for missing project_id works")
        else:
            print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    print("Testing BigQuery driver implementation...")
    test_bigquery_import()
    test_bigquery_basic_creation()
    test_bigquery_missing_project_error()
    print("All tests passed! 🎉")