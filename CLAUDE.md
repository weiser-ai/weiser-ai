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
   - `snowflake.py`: Snowflake-specific driver with warehouse/role support
   - `databricks.py`: Databricks-specific driver with warehouse/role support
   - `bigquery.py`: BigQuery-specific driver with project/dataset support
   - `metric_stores/`: Separate drivers for metric storage (DuckDB, PostgreSQL)

5. **Runner System** (`weiser/runner/`): Orchestrates check execution, manages connections, and handles results

6. **Dashboard** (`weiser-ui/app.py`): Streamlit-based web interface for visualizing check results

### Key Design Patterns

- **Factory Pattern**: CheckFactory and DriverFactory for creating appropriate instances
- **Strategy Pattern**: Different check types inherit from BaseCheck
- **Configuration as Code**: YAML-based configuration with environment variable templating
- **SQL Generation**: Uses SQLGlot for cross-database SQL generation

## Development Commands

### Environment Setup

**IMPORTANT**: This project requires the `weiser-cli` conda environment for all development tasks. Always activate this environment before running any commands:

```bash
# Activate the weiser-cli conda environment
source ~/miniforge3/etc/profile.d/conda.sh
conda activate weiser-cli
```

When using Claude Code, prefix all bash commands with environment activation:
```bash
source ~/.weiser_activate && [your_command]
```

### Setup and Installation

```bash
# Install using pip
pip install weiser-ai

# Install dashboard dependencies
cd weiser-ui
pip install -r requirements.txt
```

### Adding Python Dependencies

**IMPORTANT**: Never modify `pyproject.toml` directly. Use PDM for dependency management:

```bash
# Add a new dependency
source ~/.weiser_activate && pdm add <package_name>

# Install dependencies in conda environment
source ~/.weiser_activate && ./export_pdm.sh
```

This ensures dependencies are properly managed in both PDM and the conda environment.

### Running Checks

```bash
# Run all checks in verbose mode
weiser run examples/example.yaml -v

# Run Snowflake checks
weiser run examples/snowflake.yaml -v

# Compile checks only (validation)
weiser compile examples/example.yaml -v

# Generate sample data for a specific check
weiser sample examples/example.yaml --check "check_name" -v

# Skip export to S3 (local development)
weiser run examples/example.yaml -v -s
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
- `snowflake-sqlalchemy`: Snowflake connector
- `psycopg2`: PostgreSQL connector
- `pymysql`: MySQL connector
- `duckdb`: Local analytics database

## Configuration Structure

## Environment Variables

weiser will read the .env file and replace the variables in the configuration.
Environment variables already defined in your system can also be used.
They are available in the configuration as `{{VARIABLE_NAME}}`. Do not use `${VARIABLE_NAME}` syntax.

### Example Configuration (`examples/example.yaml`)

```yaml
version: 1
datasources:
  - name: default
    type: postgresql
    # Connection details with environment variable templating

  - name: snowflake_prod
    type: snowflake
    account: ACCOUNT_ID
    db_name: PRODUCTION_DB
    user: WEISER_USER
    password: {{ SNOWFLAKE_PASSWORD }}
    warehouse: COMPUTE_WH
    role: ANALYST_ROLE
    schema_name: PUBLIC

  - name: mysql_prod
    type: mysql
    host: mysql.example.com
    port: 3306
    db_name: production_db
    user: {{ MYSQL_USER }}
    password: {{ MYSQL_PASSWORD }}

checks:
  - name: test row_count
    dataset: [orders_view]
    type: row_count
    condition: gt
    threshold: 0

  - name: not_empty_check
    dataset: customers
    type: not_empty
    dimensions: [customer_id, email]
    condition: le
    threshold: 0

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb # or postgresql
```

### Check Types Available

- `row_count`: Count rows in dataset
- `numeric`: Custom SQL expressions
- `sum/min/max`: Aggregation checks
- `measure`: Pre-defined measure checks
- `not_empty`: Check for NULL values in dimensions (count-based)
- `not_empty_pct`: Check for NULL values in dimensions (percentage-based)
- `anomaly`: Statistical anomaly detection

### Supported Databases

- **PostgreSQL**: Full support for all check types
- **MySQL**: Full support for all check types with PyMySQL driver
- **Snowflake**: Full support with warehouse, role, and schema configuration
- **Databricks**: Full support with SQL warehouse integration
- **BigQuery**: Full support with GCP integration
- **Cube.js**: Semantic layer integration (PostgreSQL-compatible)
- **DuckDB**: For metric storage and local development

## Database Migrations

### PostgreSQL MetricStore Migrations

PostgreSQL V2 metricstore uses **Alembic** for database schema migrations:

```bash
# Set the weiser configuration file
export WEISER_CONFIG=examples/postgres-metricstore.yaml

# Run migrations to create/update database schema
source ~/.weiser_activate && alembic upgrade head

# Create a new migration (if needed)
source ~/.weiser_activate && alembic revision --autogenerate -m "Add new column"

# Check migration status
source ~/.weiser_activate && alembic current
```

**Key Files:**
- `alembic.ini` - Alembic configuration
- `alembic/env.py` - Environment configuration (reads weiser config)
- `alembic/versions/` - Migration files
- `weiser/drivers/metric_stores/models.py` - SQLModel schema definitions

### DuckDB MetricStore Migrations

DuckDB V2 metricstore uses a **custom migration system** since DuckDB doesn't support Alembic:

```python
from weiser.drivers.metric_stores.duckdb_v2 import DuckDBMetricStoreV2

# Initialize store (automatically runs pending migrations)
store = DuckDBMetricStoreV2(config)

# Check migration status
store.migration_status()

# Create a new migration
migration_file = store.create_migration('Add priority column')

# Apply specific migrations
store.migrate_up('20250710_000001')

# Rollback migrations
store.migrate_down('20250710_000001')
```

**Key Files:**
- `weiser/drivers/metric_stores/migrations/` - Migration framework
- `weiser/drivers/metric_stores/migrations/versions/` - Migration files
- `weiser/drivers/metric_stores/migrations/README.md` - **Complete migration guide**

**📖 For detailed DuckDB migration instructions, see:** [`weiser/drivers/metric_stores/migrations/README.md`](weiser/drivers/metric_stores/migrations/README.md)

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

#### DuckDB Metric Store Features

- **Local-first operation**: Works without S3 configuration for development
- **Conditional S3 integration**: Only attempts S3 operations when properly configured
- **Automatic schema creation**: Creates metrics table if it doesn't exist
- **Safe initialization**: Gracefully handles missing S3 data on first run
- **Export capabilities**: Supports both local storage and S3 export

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

## Git Workflow

**IMPORTANT**: The user manages all git commits. Claude Code should NEVER commit changes.

### Tracking Progress

Use these git commands to track what has been changed:

```bash
# See current status and modified files
source ~/.weiser_activate && git status

# See specific changes in a file
source ~/.weiser_activate && git diff <file-name>

# See all changes
source ~/.weiser_activate && git diff

# See files changed in the last commit
source ~/.weiser_activate && git diff --name-status HEAD~1..HEAD

# See detailed changes in the last commit
source ~/.weiser_activate && git diff HEAD~1..HEAD
```

### Git Guidelines

- **NEVER run `git commit`** - the user will commit when satisfied with changes
- Use `git status` and `git diff` to track progress and understand what has been modified
- If you lose track of your changes, use git operations to review what has been done
