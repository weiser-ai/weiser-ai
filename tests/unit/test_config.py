import pytest
import yaml
import tempfile
import os
from unittest.mock import patch, mock_open

from weiser.loader.config import load_config, update_namespace
from weiser.loader.models import BaseConfig, Check, Datasource, CheckType, Condition, DBType

# Import fixtures
from tests.fixtures.config_fixtures import *


class TestConfigLoading:
    """Test configuration loading functionality."""

    def test_load_simple_config(self, temp_yaml_file, sample_yaml_config):
        """Test loading a simple YAML configuration."""
        with open(temp_yaml_file, 'w') as f:
            f.write(sample_yaml_config)
        
        config = load_config(temp_yaml_file, verbose=False)
        
        assert config is not None
        assert "checks" in config
        assert "datasources" in config
        assert len(config["checks"]) == 2
        assert len(config["datasources"]) == 1

    def test_load_config_with_templating(self, temp_yaml_file, sample_yaml_config_with_templating, sample_environment_variables):
        """Test loading configuration with Jinja2 templating."""
        with open(temp_yaml_file, 'w') as f:
            f.write(sample_yaml_config_with_templating)
        
        config = load_config(temp_yaml_file, context=sample_environment_variables, verbose=False)
        
        assert config is not None
        assert config["datasources"][0]["host"] == "localhost"
        assert config["datasources"][0]["port"] == 5432  # YAML converts to int
        assert config["datasources"][0]["db_name"] == "test_db"

    def test_load_config_with_includes(self, temp_yaml_file):
        """Test loading configuration with includes."""
        # Create main config file
        main_config = """
version: 1
includes:
  - included_config.yaml
datasources:
  - name: main_db
    type: postgresql
    host: localhost
checks: []
"""
        
        # Create included config file
        included_config = """
checks:
  - name: included_check
    dataset: orders
    type: row_count
    condition: gt
    threshold: 0
"""
        
        # Write main config
        with open(temp_yaml_file, 'w') as f:
            f.write(main_config)
        
        # Write included config in same directory
        included_path = os.path.join(os.path.dirname(temp_yaml_file), 'included_config.yaml')
        with open(included_path, 'w') as f:
            f.write(included_config)
        
        try:
            config = load_config(temp_yaml_file, verbose=False)
            
            assert config is not None
            assert len(config["checks"]) == 1
            assert config["checks"][0]["name"] == "included_check"
            assert len(config["datasources"]) == 1
        finally:
            os.unlink(included_path)

    def test_update_namespace_merge_checks(self):
        """Test namespace updating merges checks correctly."""
        namespace = {
            "checks": [{"name": "check1"}],
            "datasources": [{"name": "db1"}]
        }
        
        new_file = {
            "checks": [{"name": "check2"}],
            "datasources": [{"name": "db2"}]
        }
        
        result = update_namespace(namespace, new_file, verbose=False)
        
        assert len(result["checks"]) == 2
        assert len(result["datasources"]) == 2
        assert result["checks"][0]["name"] == "check1" 
        assert result["checks"][1]["name"] == "check2"

    def test_update_namespace_empty_namespace(self):
        """Test namespace updating with empty initial namespace."""
        namespace = None
        new_file = {
            "checks": [{"name": "check1"}],
            "datasources": [{"name": "db1"}]
        }
        
        result = update_namespace(namespace, new_file, verbose=False)
        
        assert result == new_file

    def test_load_config_file_not_found(self):
        """Test error handling when config file is not found."""
        # load_config uses glob.glob() which returns empty list for non-existent files
        # This should result in no files being processed, returning None
        config = load_config("nonexistent.yaml", verbose=False)
        assert config is None

    def test_load_config_invalid_yaml(self, temp_yaml_file):
        """Test error handling with invalid YAML."""
        invalid_yaml = """
        invalid: yaml: content:
          - missing: bracket
        """
        
        with open(temp_yaml_file, 'w') as f:
            f.write(invalid_yaml)
        
        with pytest.raises(yaml.YAMLError):
            load_config(temp_yaml_file, verbose=False)


class TestBaseConfigValidation:
    """Test BaseConfig model validation."""

    def test_valid_base_config(self, sample_base_config):
        """Test creating a valid BaseConfig."""
        assert isinstance(sample_base_config, BaseConfig)
        assert len(sample_base_config.checks) == 2
        assert len(sample_base_config.datasources) == 1

    def test_config_with_missing_checks(self, sample_datasource, sample_metric_store):
        """Test BaseConfig allows empty checks list."""
        # The actual implementation allows empty checks
        config = BaseConfig(
            checks=[],  # Empty checks is allowed
            datasources=[sample_datasource],
            connections=[sample_metric_store]
        )
        assert len(config.checks) == 0

    def test_config_with_missing_datasources(self, sample_row_count_check, sample_metric_store):
        """Test BaseConfig allows empty datasources list."""
        # The actual implementation allows empty datasources
        config = BaseConfig(
            checks=[sample_row_count_check],
            datasources=[],  # Empty datasources is allowed
            connections=[sample_metric_store]
        )
        assert len(config.datasources) == 0

    def test_check_validation_missing_threshold(self):
        """Test Check validation allows missing threshold."""
        # The actual implementation allows missing threshold (defaults to None)
        check = Check(
            name="check_without_threshold",
            dataset="orders",
            type=CheckType.row_count,
            condition=Condition.gt
            # Missing threshold is allowed
        )
        assert check.threshold is None

    def test_check_validation_invalid_condition(self):
        """Test Check validation with invalid condition."""
        with pytest.raises(ValueError):
            Check(
                name="invalid_check",
                dataset="orders", 
                type=CheckType.row_count,
                condition="invalid_condition",  # Should use Condition enum
                threshold=0
            )

    def test_datasource_validation_missing_required_fields(self):
        """Test Datasource validation allows minimal configuration."""
        # The actual implementation allows minimal datasource configuration
        datasource = Datasource(
            name="minimal_db",
            type=DBType.postgresql
            # Missing optional fields is allowed
        )
        assert datasource.name == "minimal_db"
        assert datasource.type == DBType.postgresql

    def test_check_with_between_condition_list_threshold(self):
        """Test Check validation with between condition and list threshold."""
        check = Check(
            name="between_check",
            dataset="orders",
            type=CheckType.numeric,
            measure="sum(amount)",
            condition=Condition.between,
            threshold=[100.0, 200.0]
        )
        
        assert check.condition == Condition.between
        assert check.threshold == [100.0, 200.0]

    def test_check_with_dimensions(self):
        """Test Check validation with dimensions."""
        check = Check(
            name="dimension_check",
            dataset="orders",
            type=CheckType.row_count,
            dimensions=["status", "region"],
            condition=Condition.gt,
            threshold=0
        )
        
        assert check.dimensions == ["status", "region"]
        assert len(check.dimensions) == 2