"""Basic tests to verify test setup works correctly."""

import pytest
from weiser.loader.models import CheckType, Condition, DBType


def test_imports_work():
    """Test that basic imports work correctly."""
    assert CheckType.row_count == "row_count"
    assert Condition.gt == "gt"
    assert DBType.postgresql == "postgresql"


def test_pytest_markers():
    """Test that pytest markers are working."""
    assert hasattr(pytest.mark, 'unit')
    assert hasattr(pytest.mark, 'integration')


@pytest.mark.unit
def test_unit_marker():
    """Test unit marker functionality."""
    assert True


@pytest.mark.integration  
def test_integration_marker():
    """Test integration marker functionality."""
    assert True