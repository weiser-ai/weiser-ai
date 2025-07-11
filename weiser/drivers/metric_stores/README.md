# MetricStore Drivers

This directory contains drivers for different metric storage backends.

## Overview

The metric store drivers are responsible for persisting metric results from data quality checks. They provide a consistent interface for storing and retrieving metric data across different database systems.

## Available Drivers

### DuckDB Driver (`duckdb.py`)

- **Purpose**: Local analytics database with optional S3 integration
- **Features**:
  - Local-first operation
  - Conditional S3 integration
  - Automatic schema creation
  - Export capabilities

### PostgreSQL V2 Driver (`postgres.py`) - **NEW**

- **Purpose**: Modern PostgreSQL driver using SQLModel and Alembic
- **Features**:
  - SQLModel-based schema definition
  - Alembic migrations for schema management
  - Type-safe database operations
  - Enhanced querying capabilities
  - Proper ORM integration

## SQLModel + Alembic Implementation

The new PostgreSQL V2 driver introduces modern database management practices:

### Key Benefits

1. **Type Safety**: Uses SQLModel for type-safe database operations
2. **Schema Management**: Alembic handles database migrations automatically
3. **ORM Integration**: Proper SQLAlchemy ORM integration
4. **Maintainability**: Cleaner code structure and better error handling
5. **Extensibility**: Easy to add new fields and modify schema

### Schema Definition

The metrics table schema is defined in `models.py` using SQLModel:

```python
class MetricRecord(SQLModel, table=True):
    __tablename__ = "metrics"

    id: Optional[int] = Field(default=None, primary_key=True)
    actual_value: Optional[float] = Field(default=None)
    check_id: Optional[str] = Field(default=None)
    # ... other fields
```

### Migration Management

Database schema changes are managed through Alembic:

- **Configuration**: `alembic.ini` in project root
- **Environment**: `alembic/env.py` configures migration environment
- **Migrations**: `alembic/versions/` contains version-controlled migrations

### Usage

```python
from weiser.drivers.metric_stores import MetricStoreFactory

# Create driver instance
store = MetricStoreFactory.create_driver(metric_store_config)

# Insert metric record
store.insert_results(record_dict)

# Query metrics
metrics = store.get_metrics_for_check("check_id")
run_metrics = store.get_metrics_for_run("run_id")
```

## Migration Path

To migrate from the old PostgreSQL driver to the new V2 driver:

1. Run Alembic migrations to update your database schema
2. Test thoroughly with your existing data

## Testing

TODO: Add unit tests for the new driver implementation

Note: Requires a PostgreSQL database running locally with appropriate credentials.

## Future Enhancements

The SQLModel + Alembic foundation enables:

- Easy addition of new metric fields
- Database schema versioning
- Multi-database support expansion
- Enhanced query capabilities
- Better performance optimization
