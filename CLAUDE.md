# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Weiser is a data quality framework designed to ensure data integrity and accuracy. It provides configurable checks to validate data and detect anomalies across various data sources, with results stored in a metrics store and visualized through a dashboard.

## Core Architecture

### Main Components

1. **CLI Interface** (`weiser/main.py`): Entry point with three main commands:
   - `run`: Execute data quality checks
   - `compile`: Validate configuration without running checks
   - `sample`: Generate sample data for a specific check

2. **Configuration System** (`weiser/loader/`):
   - `config.py`: YAML configuration loading with Jinja2 templating support
   - `models.py`: Pydantic models defining the schema for checks, datasources, and connections

3. **Check System** (`weiser/checks/`):
   - `base.py`: BaseCheck abstract class with common functionality
   - `numeric.py`: Implementations for numeric, row_count, sum, min, max, and measure checks
   - `anomaly.py`: Anomaly detection checks using Z-score analysis
   - Factory pattern for check instantiation

4. **Driver System** (`weiser/drivers/`):
   - `base.py`: BaseDriver for database connections using SQLAlchemy
   - `postgres.py`: PostgreSQL-specific driver
   - `metric_stores/`: Separate drivers for metric storage (DuckDB, PostgreSQL)

5. **Runner System** (`weiser/runner/`): Orchestrates check execution, manages connections, and handles results

6. **Dashboard** (`weiser-ui/app.py`): Streamlit-based web interface for visualizing check results

### Key Design Patterns

- **Factory Pattern**: CheckFactory and DriverFactory for creating appropriate instances
- **Strategy Pattern**: Different check types inherit from BaseCheck
- **Configuration as Code**: YAML-based configuration with environment variable templating
- **SQL Generation**: Uses SQLGlot for cross-database SQL generation

## Development Commands

### Setup and Installation
```bash
# Install using pip
pip install weiser-ai

# Install dashboard dependencies
cd weiser-ui
pip install -r requirements.txt
```

### Running Checks
```bash
# Run all checks in verbose mode
weiser run examples/example.yaml -v

# Compile checks only (validation)
weiser compile examples/example.yaml -v

# Generate sample data for a specific check
weiser sample examples/example.yaml --check "check_name" -v
```

### Dashboard
```bash
cd weiser-ui
streamlit run app.py
```

### Development Setup
The project uses PDM for dependency management. Key dependencies include:
- `typer`: CLI framework
- `pydantic`: Data validation
- `sqlalchemy`: Database ORM
- `sqlglot`: SQL parsing and generation
- `streamlit`: Dashboard framework

## Configuration Structure

### Example Configuration (`examples/example.yaml`)
```yaml
version: 1
datasources:
  - name: default
    type: postgresql
    # Connection details with environment variable templating
    
checks:
  - name: test row_count
    dataset: [orders_view]
    type: row_count
    condition: gt
    threshold: 0
    
connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb  # or postgresql
```

### Check Types Available
- `row_count`: Count rows in dataset
- `numeric`: Custom SQL expressions
- `sum/min/max`: Aggregation checks
- `measure`: Pre-defined measure checks
- `anomaly`: Statistical anomaly detection

### Supported Databases
- PostgreSQL
- DuckDB (for metric storage)
- MySQL
- Cube.js integration

## Key Implementation Details

### Check ID Generation
Each check generates a unique SHA256 hash based on:
- Datasource name
- Check name  
- Dataset name

### SQL Query Building
- Uses SQLGlot for cross-database compatibility
- Supports dimensions (GROUP BY)
- Time dimension aggregation with granularity
- Custom WHERE clause filtering

### Metric Storage
Results are stored with metadata including:
- `run_id`: Unique execution identifier
- `check_id`: Unique check identifier
- `success/fail`: Boolean status
- `actual_value`: Measured value
- `run_time`: Execution timestamp

### Environment Variables
Configuration supports Jinja2 templating with environment variables:
- Database connection strings
- S3 credentials for DuckDB storage
- Slack webhook URLs for notifications

## Testing and Quality

The project includes a comprehensive test suite using pytest. 

### Running Tests
```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run integration tests only  
pytest tests/integration/

# Run with coverage
pytest --cov=weiser

# Run specific test file
pytest tests/unit/test_config.py

# Run with verbose output
pytest -v
```

### Test Structure
- `tests/unit/`: Unit tests for individual components
  - `test_config.py`: Configuration loading and validation tests
  - `test_checks.py`: Check system tests (BaseCheck, numeric, anomaly)
  - `test_drivers.py`: Driver system and SQL generation tests
  - `test_runner.py`: Runner orchestration tests
- `tests/integration/`: Integration tests with mocked dependencies
  - `test_full_workflow.py`: End-to-end workflow tests
- `tests/fixtures/`: Reusable test data and fixtures
- `tests/conftest.py`: Global pytest configuration and fixtures

### Test Coverage Areas
- **Configuration System**: YAML loading, Jinja2 templating, validation
- **Check System**: All check types, SQL generation, condition evaluation
- **Driver System**: Database connections, metric stores, factories
- **Runner System**: Check orchestration, sample data generation
- **Integration**: Full workflows with mocked databases

### Testing Best Practices
- Use fixtures for common test data (`tests/fixtures/config_fixtures.py`)
- Mock external dependencies (databases, S3, etc.)
- Test both success and failure scenarios
- Include edge cases and error conditions
- Validate SQL generation across different dialects

### Mock Strategy
Tests use extensive mocking to avoid requiring real databases:
- SQLAlchemy engines and connections are mocked
- DuckDB connections are mocked for metric storage
- S3/boto3 clients are mocked for cloud storage
- Check execution results are mocked with realistic data

## Docker Compose Support

Multiple Docker Compose configurations available:
- `docker-compose-postgres.yaml`: PostgreSQL setup
- `docker-compose-duckdb-minio.yaml`: DuckDB with MinIO
- `docker-compose.yaml`: Full stack deployment