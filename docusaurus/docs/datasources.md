# Supported Datasources

Weiser supports multiple database systems and data platforms for running data quality checks. This page provides detailed configuration and setup instructions for each supported datasource.

## Overview

Currently supported datasources:

| Datasource     | Type         | Status             | Use Cases                        |
| -------------- | ------------ | ------------------ | -------------------------------- |
| **PostgreSQL** | `postgresql` | ✅ Fully Supported | OLTP, Analytics, Data Warehouses |
| **Snowflake**  | `snowflake`  | ✅ Fully Supported | Cloud Data Warehouse             |
| **Cube.js**    | `cube`       | ✅ Fully Supported | Semantic Layer, Business Metrics |
| **MySQL**      | `mysql`      | 📋 Planned         | OLTP, Web Applications           |
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
    host: {{POSTGRES_HOST}}
    port: {{POSTGRES_PORT}}
    db_name: {{POSTGRES_DB}}
    user: {{POSTGRES_USER}}
    password: {{POSTGRES_PASSWORD}}
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

## Snowflake

Snowflake is a fully-managed cloud data warehouse that supports all Weiser check types. The Snowflake connector provides native support for Snowflake-specific features like warehouses, roles, and schemas.

### Configuration

#### Basic Connection

```yaml
datasources:
  - name: snowflake_prod
    type: snowflake
    account: your-account-id
    db_name: PRODUCTION_DB
    user: WEISER_USER
    password: secure_password
    warehouse: COMPUTE_WH
    role: ANALYST_ROLE
    schema: PUBLIC
```

#### Connection URI

```yaml
datasources:
  - name: snowflake_warehouse
    type: snowflake
    uri: snowflake://user:password@account.snowflakecomputing.com/database?warehouse=WH&role=ROLE&schema=SCHEMA
```

#### Environment Variables (Recommended)

```yaml
datasources:
  - name: snowflake_prod
    type: snowflake
    account: {{SNOWFLAKE_ACCOUNT}}
    db_name: {{SNOWFLAKE_DATABASE}}
    user: {{SNOWFLAKE_USER}}
    password: {{SNOWFLAKE_PASSWORD}}
    warehouse: {{SNOWFLAKE_WAREHOUSE}}
    role: {{SNOWFLAKE_ROLE}}
    schema: {{SNOWFLAKE_SCHEMA}}
```

### Connection Parameters

| Parameter   | Required | Default | Description                                                |
| ----------- | -------- | ------- | ---------------------------------------------------------- |
| `name`      | Yes      | -       | Unique datasource identifier                               |
| `type`      | Yes      | -       | Must be `snowflake`                                        |
| `host`      | Yes\*    | -       | Snowflake account URL (e.g., account.snowflakecomputing.com) |
| `db_name`   | Yes\*    | -       | Database name                                              |
| `user`      | Yes\*    | -       | Snowflake username                                         |
| `password`  | Yes\*    | -       | Snowflake password                                         |
| `warehouse` | No       | -       | Warehouse to use for compute                               |
| `role`      | No       | -       | Role to assume for permissions                             |
| `schema`    | No       | -       | Default schema to use                                      |
| `uri`       | Yes\*    | -       | Complete connection URI (alternative to individual params) |

\*Either individual parameters OR uri is required

### Setup Requirements

#### 1. Snowflake User and Role Setup

```sql
-- Create role for Weiser
CREATE ROLE weiser_role;

-- Create user for Weiser
CREATE USER weiser_user
    PASSWORD = 'secure_password'
    DEFAULT_ROLE = weiser_role
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_NAMESPACE = 'PRODUCTION_DB.PUBLIC';

-- Grant role to user
GRANT ROLE weiser_role TO USER weiser_user;

-- Grant database and schema permissions
GRANT USAGE ON DATABASE PRODUCTION_DB TO ROLE weiser_role;
GRANT USAGE ON SCHEMA PRODUCTION_DB.PUBLIC TO ROLE weiser_role;
GRANT SELECT ON ALL TABLES IN SCHEMA PRODUCTION_DB.PUBLIC TO ROLE weiser_role;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE weiser_role;

-- Grant permissions for future tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA PRODUCTION_DB.PUBLIC TO ROLE weiser_role;
```

#### 2. Network Access

Snowflake is cloud-hosted and accessible via HTTPS. Ensure your network allows outbound HTTPS connections.

#### 3. Account Identifier

Find your Snowflake account identifier:

```sql
-- Run this in Snowflake to get your account identifier
SELECT CURRENT_ACCOUNT();
```

### Supported Features

| Feature                   | Snowflake Support |
| ------------------------- | ----------------- |
| **Row Count Checks**      | ✅ Full Support   |
| **Numeric Checks**        | ✅ Full Support   |
| **Sum/Min/Max Checks**    | ✅ Full Support   |
| **Not Empty Checks**      | ✅ Full Support   |
| **Anomaly Detection**     | ✅ Full Support   |
| **Custom SQL**            | ✅ Full Support   |
| **Time Dimensions**       | ✅ Full Support   |
| **Dimensions/Grouping**   | ✅ Full Support   |
| **Complex Filters**       | ✅ Full Support   |
| **Window Functions**      | ✅ Full Support   |
| **Statistical Functions** | ✅ Full Support   |

### Snowflake-Specific Examples

#### Using Snowflake Date Functions

```yaml
- name: recent_data_check
  dataset: orders
  type: row_count
  condition: gt
  threshold: 100
  filter: created_at >= DATEADD(day, -7, CURRENT_DATE())
```

#### Time Travel Queries

```yaml
- name: historical_comparison
  dataset: |
    SELECT COUNT(*) as current_count
    FROM orders
    WHERE created_at >= CURRENT_DATE()
  type: numeric
  measure: current_count
  condition: gt
  threshold: 1000
```

#### Using Snowflake Warehouse Scaling

```yaml
# Use larger warehouse for intensive checks
datasources:
  - name: snowflake_large
    type: snowflake
    host: account.snowflakecomputing.com
    db_name: ANALYTICS_DB
    user: weiser_user
    password: secure_password
    warehouse: LARGE_WH  # Use larger warehouse for heavy workloads
    role: ANALYST_ROLE
```

### Performance Optimization

#### Warehouse Sizing

Choose appropriate warehouse size based on check complexity:

- **XS/S**: Simple row counts and basic aggregations
- **M/L**: Complex joins and window functions
- **XL/2XL**: Large-scale statistical analysis

#### Query Optimization

```sql
-- Use clustering keys for large tables
ALTER TABLE orders CLUSTER BY (created_at);

-- Use materialized views for complex aggregations
CREATE MATERIALIZED VIEW daily_order_metrics AS
SELECT 
    DATE_TRUNC('day', created_at) as order_date,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY 1;
```

### Common Issues & Solutions

#### Authentication Errors

```yaml
# Ensure correct role and warehouse
datasources:
  - name: snowflake_prod
    type: snowflake
    host: account.snowflakecomputing.com
    db_name: PROD_DB
    user: WEISER_USER
    password: secure_password
    warehouse: COMPUTE_WH  # Must have USAGE privilege
    role: ANALYST_ROLE     # Must be granted to user
```

#### Warehouse Suspension

```yaml
# Auto-resume warehouses (default behavior)
# Warehouses automatically resume when queries are executed
# Consider using auto-suspend settings in Snowflake
```

#### Large Result Sets

```yaml
# Use LIMIT for sampling large tables
- name: sample_data_check
  dataset: |
    SELECT * FROM large_table 
    SAMPLE (1000 ROWS)  -- Snowflake sampling
  type: row_count
  condition: eq
  threshold: 1000
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
