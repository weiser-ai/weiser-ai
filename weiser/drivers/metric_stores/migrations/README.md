# DuckDB Migration System

This directory contains a custom migration system for DuckDB MetricStore since DuckDB doesn't have native Alembic support.

## Overview

The migration system provides:

- **Migration Tracking**: Tracks applied migrations in a `duckdb_migrations` table
- **Automatic Discovery**: Automatically discovers migration files in the `versions/` directory
- **Forward/Backward Migrations**: Supports both upgrade and downgrade operations
- **Safe Execution**: Transactions ensure migrations are applied safely
- **Migration Generation**: Tools to create new migration files

## Usage

### Basic Operations

```python
from weiser.drivers.metric_stores.duckdb_v2 import DuckDBMetricStoreV2
from weiser.loader.models import MetricStore, MetricStoreType

# Initialize the store (automatically runs pending migrations)
config = MetricStore(db_type=MetricStoreType.duckdb, db_name='metrics.db')
store = DuckDBMetricStoreV2(config)

# Check migration status
store.migration_status()

# Apply pending migrations manually
store.migrate_up()

# Rollback to a specific version
store.migrate_down('20250710_000001')

# Create a new migration
migration_file = store.create_migration('Add new column')
```

### Creating Migrations

1. **Generate Migration File**:
   ```python
   store.create_migration('Add priority column')
   ```

2. **Edit the Generated File**:
   ```python
   def upgrade(self, session: Session) -> None:
       """Apply the migration."""
       self.execute_sql(session, """
           ALTER TABLE metrics 
           ADD COLUMN priority INTEGER DEFAULT 0
       """)
       session.commit()
   
   def downgrade(self, session: Session) -> None:
       """Revert the migration."""
       self.execute_sql(session, """
           ALTER TABLE metrics 
           DROP COLUMN priority
       """)
       session.commit()
   ```

3. **Test the Migration**:
   ```python
   # The migration will be applied automatically on next initialization
   store = DuckDBMetricStoreV2(config)
   ```

## Migration File Structure

Migration files are stored in `versions/` and follow this naming convention:
- `YYYYMMDD_HHMMSS_description.py`
- Example: `20250710_000001_add_tags_column.py`

Each migration file contains:
- A class that inherits from `BaseMigration`
- `upgrade()` method for applying changes
- `downgrade()` method for reverting changes
- Version and description metadata

## Example Migration

```python
"""
Migration: Add tags column to metrics table
Version: 20250710_000001
"""

from sqlmodel import Session
from weiser.drivers.metric_stores.migrations.base_migration import BaseMigration


class Migration_20250710_000001(BaseMigration):
    def __init__(self):
        super().__init__(version="20250710_000001", description="Add tags column to metrics table")
    
    def upgrade(self, session: Session) -> None:
        """Apply the migration - add tags column."""
        self.execute_sql(session, """
            ALTER TABLE metrics 
            ADD COLUMN tags VARCHAR
        """)
        session.commit()
    
    def downgrade(self, session: Session) -> None:
        """Revert the migration - remove tags column."""
        self.execute_sql(session, """
            ALTER TABLE metrics 
            DROP COLUMN tags
        """)
        session.commit()
```

## Common Migration Patterns

### Adding a Column
```python
def upgrade(self, session: Session) -> None:
    self.execute_sql(session, """
        ALTER TABLE metrics 
        ADD COLUMN new_column VARCHAR DEFAULT 'default_value'
    """)
    session.commit()
```

### Removing a Column
```python
def upgrade(self, session: Session) -> None:
    self.execute_sql(session, """
        ALTER TABLE metrics 
        DROP COLUMN old_column
    """)
    session.commit()
```

### Creating an Index
```python
def upgrade(self, session: Session) -> None:
    self.execute_sql(session, """
        CREATE INDEX idx_metrics_check_id 
        ON metrics(check_id)
    """)
    session.commit()
```

### Data Migration
```python
def upgrade(self, session: Session) -> None:
    # Add new column
    self.execute_sql(session, """
        ALTER TABLE metrics 
        ADD COLUMN processed BOOLEAN DEFAULT FALSE
    """)
    
    # Update existing records
    self.execute_sql(session, """
        UPDATE metrics 
        SET processed = TRUE 
        WHERE success = TRUE
    """)
    
    session.commit()
```

## Best Practices

1. **Always Test Migrations**: Test both upgrade and downgrade operations
2. **Use Transactions**: Always commit changes at the end of upgrade/downgrade
3. **Descriptive Names**: Use clear, descriptive migration names
4. **Incremental Changes**: Keep migrations small and focused
5. **Backup Data**: Always backup important data before running migrations
6. **Version Control**: Commit migration files to version control

## Migration States

- **Pending**: Migration file exists but hasn't been applied
- **Applied**: Migration has been successfully applied and recorded
- **Failed**: Migration encountered an error during application

## Troubleshooting

### Migration Failed
If a migration fails:
1. Check the error message
2. Fix the migration file
3. Manually rollback if needed: `store.migrate_down('previous_version')`
4. Re-run the migration

### Migration Table Issues
If the migration tracking table is corrupted:
1. Drop the table: `DROP TABLE duckdb_migrations`
2. Restart the application to recreate the table
3. Manually mark migrations as applied if needed

## Integration with DuckDB V2

The migration system is automatically integrated with `DuckDBMetricStoreV2`:

- **Automatic Execution**: Pending migrations run on initialization
- **Safe Initialization**: Tables are created before migrations run
- **Error Handling**: Migration errors are caught and logged
- **Status Reporting**: Easy access to migration status

This ensures your DuckDB schema stays up-to-date automatically while maintaining the flexibility to manage migrations manually when needed.