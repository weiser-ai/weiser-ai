# Supported Datasources

Weiser supports multiple database systems and data platforms for running data quality checks. This page provides detailed configuration and setup instructions for each supported datasource.

## Overview

Currently supported datasources:

| Datasource     | Type         | Status             | Use Cases                        |
| -------------- | ------------ | ------------------ | -------------------------------- |
| **PostgreSQL** | `postgresql` | ✅ Fully Supported | OLTP, Analytics, Data Warehouses |
| **Cube.js**    | `cube`       | ✅ Fully Supported | Semantic Layer, Business Metrics |
| **MySQL**      | `mysql`      | 📋 Planned         | OLTP, Web Applications           |
| **Snowflake**  | `snowflake`  | 🚧 Coming Soon     | Cloud Data Warehouse             |
| **Databricks** | `databricks` | 🚧 Coming Soon     | Cloud Data Warehouse             |
| **BigQuery**   | `bigquery`   | 📋 Planned         | Cloud Data Warehouse             |
| **Redshift**   | `redshift`   | 📋 Planned         | Cloud Data Warehouse             |
| **Athena**     | `athena`     | 📋 Planned         | Cloud Data Warehouse             |
| **Trino**      | `trino`      | 📋 Planned         | Distributed Data Warehouse       |

## PostgreSQL

PostgreSQL is the primary supported datasource with full feature compatibility for all check types.

### Configuration

#### Basic Connection

```yaml
datasources:
  - name: postgres_prod
    type: postgresql
    host: localhost
    port: 5432
    db_name: production
    user: weiser_user
    password: secure_password
```

#### Connection URI

```yaml
datasources:
  - name: postgres_warehouse
    type: postgresql
    uri: postgresql://user:password@host:5432/database
```

#### Environment Variables (Recommended)

```yaml
datasources:
  - name: postgres_prod
    type: postgresql
    host: ${POSTGRES_HOST}
    port: ${POSTGRES_PORT}
    db_name: ${POSTGRES_DB}
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
```

### Connection Parameters

| Parameter  | Required | Default | Description                                                |
| ---------- | -------- | ------- | ---------------------------------------------------------- |
| `name`     | Yes      | -       | Unique datasource identifier                               |
| `type`     | Yes      | -       | Must be `postgresql`                                       |
| `host`     | Yes\*    | -       | Database server hostname                                   |
| `port`     | No       | 5432    | Database server port                                       |
| `db_name`  | Yes\*    | -       | Database name                                              |
| `user`     | Yes\*    | -       | Database username                                          |
| `password` | Yes\*    | -       | Database password                                          |
| `uri`      | Yes\*    | -       | Complete connection URI (alternative to individual params) |

\*Either individual parameters OR uri is required

### Setup Requirements

#### 1. Database User Permissions

```sql
-- Create dedicated user for Weiser
CREATE USER weiser_user WITH PASSWORD 'secure_password';

-- Grant read permissions
GRANT CONNECT ON DATABASE your_database TO weiser_user;
GRANT USAGE ON SCHEMA public TO weiser_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO weiser_user;

-- Grant permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO weiser_user;
```

#### 2. Network Access

Ensure your PostgreSQL server allows connections from Weiser:

```bash
# Test connectivity
psql -h your-host -U weiser_user -d your_database -c "SELECT 1;"
```

#### 3. SSL Configuration (Recommended)

```yaml
datasources:
  - name: postgres_prod
    type: postgresql
    uri: postgresql://user:password@host:5432/database?sslmode=require
```

### Supported Features

| Feature                   | PostgreSQL Support |
| ------------------------- | ------------------ |
| **Row Count Checks**      | ✅ Full Support    |
| **Numeric Checks**        | ✅ Full Support    |
| **Sum/Min/Max Checks**    | ✅ Full Support    |
| **Not Empty Checks**      | ✅ Full Support    |
| **Anomaly Detection**     | ✅ Full Support    |
| **Custom SQL**            | ✅ Full Support    |
| **Time Dimensions**       | ✅ Full Support    |
| **Dimensions/Grouping**   | ✅ Full Support    |
| **Complex Filters**       | ✅ Full Support    |
| **Window Functions**      | ✅ Full Support    |
| **Statistical Functions** | ✅ Full Support    |

### PostgreSQL-Specific Examples

#### Using PostgreSQL Date Functions

```yaml
- name: recent_data_check
  dataset: orders
  type: row_count
  condition: gt
  threshold: 100
  filter: created_at >= CURRENT_DATE - INTERVAL '7 days'
```

### Performance Optimization

#### Indexes

Ensure proper indexes for check performance:

```sql
-- Index for time-based checks
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- Index for filtered checks
CREATE INDEX idx_orders_status ON orders(status);

-- Composite index for grouped checks
CREATE INDEX idx_sales_region_date ON sales(region, sale_date);
```

### Common Issues & Solutions

#### Connection Timeout

```yaml
# Increase timeout for large queries
datasources:
  - name: postgres_prod
    type: postgresql
    uri: postgresql://user:password@host:5432/database?connect_timeout=30
```

#### SSL Certificate Issues

```yaml
# Skip SSL verification (not recommended for production)
datasources:
  - name: postgres_dev
    type: postgresql
    uri: postgresql://user:password@host:5432/database?sslmode=disable
```

#### Large Result Sets

```yaml
# Use LIMIT in checks for large tables
- name: sample_data_check
  dataset: |
    SELECT * FROM large_table 
    ORDER BY created_at DESC 
    LIMIT 100000
  type: row_count
  condition: gt
  threshold: 50000
```

## Cube

Cube is a semantic layer that allows you to define business metrics and dimensions. Weiser integrates with Cube to run data quality checks on these metrics.
Cube implements the PostgreSQL interface, so you can use the same configuration as PostgreSQL. Visit the [Cube documentation](https://cube.dev/docs) for more details on setting up Cube.

## Contributing

Help us expand datasource support! We welcome contributions for:

- New database connectors
- Performance optimizations
- Documentation improvements
- Testing and validation

See our [GitHub repository](https://github.com/weiser-ai/weiser) for contribution guidelines.

## Getting Help

- 📚 [Configuration Reference](./configuration.md)
- 🔧 [Getting Started Guide](./tutorial/getting-started.md)
- 💬 [GitHub Issues](https://github.com/weiser-ai/weiser/issues)
- 📖 [Check Types Documentation](./check-types/index.md)
