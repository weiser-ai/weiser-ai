# Configuration

Weiser uses YAML configuration files to define data quality checks, datasources, and connections. This page provides a comprehensive guide to configuring your Weiser setup.

## Configuration Structure

```yaml
version: 1
datasources:
  - name: default
    type: postgresql
    host: localhost
    port: 5432
    db_name: mydb
    user: postgres
    password: secret

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: metricstore.db

checks:
  - name: orders_row_count
    dataset: orders
    type: row_count
    condition: gt
    threshold: 0

includes:
  - path/to/additional/config.yaml

slack_url: https://hooks.slack.com/services/...
```

## Datasources

Configure connections to your data sources:

### PostgreSQL

```yaml
datasources:
  - name: postgres_prod
    type: postgresql
    host: prod-db.company.com
    port: 5432
    db_name: production
    user: weiser_user
    password: ${POSTGRES_PASSWORD}
```

### MySQL

```yaml
datasources:
  - name: mysql_analytics
    type: mysql
    host: mysql.example.com
    port: 3306
    db_name: analytics
    user: analytics_user
    password: ${MYSQL_PASSWORD}
```

### Cube.js

```yaml
datasources:
  - name: cube_api
    type: cube
    uri: http://localhost:4000/cubejs-api/v1
```

### Connection URI

```yaml
datasources:
  - name: warehouse
    type: postgresql
    uri: postgresql://user:pass@host:5432/database
```

## Metric Store Connections

Configure where check results are stored:

### DuckDB (Default)

```yaml
connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: metricstore.db
```

### PostgreSQL Metric Store

```yaml
connections:
  - name: metricstore
    type: metricstore
    db_type: postgresql
    host: metrics-db.company.com
    port: 5432
    db_name: metrics
    user: metrics_user
    password: ${METRICS_PASSWORD}
```

### S3 Storage (DuckDB with S3)

```yaml
connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    s3_access_key: ${S3_ACCESS_KEY}
    s3_secret_access_key: ${S3_SECRET_KEY}
    s3_endpoint: s3.amazonaws.com
    s3_bucket: weiser-metrics
    s3_region: us-east-1
```

## Check Configuration

### Basic Check Structure

```yaml
checks:
  - name: unique_check_name
    dataset: table_name_or_sql
    type: check_type
    condition: comparison_operator
    threshold: value_or_range
    # Optional parameters
    measure: column_or_expression
    dimensions: [column1, column2]
    filter: where_clause
    time_dimension:
      name: date_column
      granularity: day
```

### Required Parameters

| Parameter   | Description         | Example            |
| ----------- | ------------------- | ------------------ |
| `name`      | Unique identifier   | `orders_row_count` |
| `dataset`   | Table or SQL query  | `orders`           |
| `type`      | Check type          | `row_count`        |
| `condition` | Comparison operator | `gt`               |
| `threshold` | Expected value      | `1000`             |

### Optional Parameters

| Parameter     | Description          | Example                 |
| ------------- | -------------------- | ----------------------- |
| `datasource`  | Datasource name      | `postgres_prod`         |
| `measure`     | Column/expression    | `order_amount`          |
| `dimensions`  | Group by columns     | `[region, status]`      |
| `filter`      | WHERE clause         | `status = 'active'`     |
| `description` | Check description    | `Validates order count` |
| `fail`        | Allow check failures | `false`                 |

## Conditions

| Condition | Description           | Example                                  |
| --------- | --------------------- | ---------------------------------------- |
| `gt`      | Greater than          | `actual > threshold`                     |
| `ge`      | Greater than or equal | `actual >= threshold`                    |
| `lt`      | Less than             | `actual < threshold`                     |
| `le`      | Less than or equal    | `actual <= threshold`                    |
| `eq`      | Equal to              | `actual == threshold`                    |
| `neq`     | Not equal to          | `actual != threshold`                    |
| `between` | Between range         | `threshold[0] <= actual <= threshold[1]` |

## Threshold Types

### Single Value

```yaml
threshold: 1000
```

### Range (for between condition)

```yaml
condition: between
threshold: [100, 500]
```

### Decimal Values

```yaml
threshold: 0.05 # 5% for percentage checks
```

## Dimensions

Group checks by specific columns:

```yaml
# Single dimension
dimensions: [region]

# Multiple dimensions
dimensions: [region, product_category, quarter]

# No dimensions (aggregate entire dataset)
dimensions: []
```

## Time Dimensions

Add time-based grouping:

```yaml
time_dimension:
  name: created_at
  granularity:
    day # millennium, century, decade, year, quarter,
    # month, week, day, hour, minute, second
```

## Filters

Add WHERE clause conditions:

```yaml
# Simple filter
filter:
  - status = 'active'

# Complex filter
filter:
  - status IN ('active', 'pending') AND created_at >= '2024-01-01'

# Multiple filters, will be combined with AND
filter:
  - status = 'active'
  - amount > 0
```

## Dataset Options

### Table Name

```yaml
dataset: orders
```

### Multiple Tables

```yaml
dataset: [orders, returns, refunds]
```

### Custom SQL

```yaml
dataset: >
  SELECT o.*, c.customer_tier
  FROM orders o
  JOIN customers c ON o.customer_id = c.id
  WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
```

## Environment Variables

Use environment variables for sensitive data:

```yaml
datasources:
  - name: prod
    type: postgresql
    host: ${DB_HOST}
    user: ${DB_USER}
    password: ${DB_PASSWORD}
    db_name: ${DB_NAME}
```

Set environment variables:

```bash
export DB_HOST=prod-db.company.com
export DB_USER=weiser_user
export DB_PASSWORD=secret123
export DB_NAME=production
```

## Configuration Includes

Split configuration across multiple files:

```yaml
# main.yaml
version: 1
includes:
  - datasources.yaml
  - checks/row_counts.yaml
  - checks/revenue_checks.yaml
```

```yaml
# datasources.yaml
datasources:
  - name: postgres_prod
    type: postgresql
    # ... connection details
```

## Templating

Use template variables for reusable configurations:

```yaml
# Template variables
variables:
  min_row_count: 1000
  revenue_threshold: 50000

checks:
  - name: orders_count
    dataset: orders
    type: row_count
    condition: gt
    threshold: ${min_row_count}
```

## Slack Integration

Configure Slack notifications:

```yaml
slack_url: https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

## Validation

Weiser validates your configuration on startup. Common validation errors:

### Missing Required Fields

```
Error: Check 'my_check' missing required field 'condition'
```

### Invalid Check Type

```
Error: Unknown check type 'invalid_type'
```

### Invalid Condition

```
Error: Invalid condition 'invalid_condition'
```

### Missing Threshold for Between

```
Error: 'between' condition requires array threshold [min, max]
```

## Best Practices

1. **Environment Variables**: Use for sensitive data
2. **Descriptive Names**: Use clear, descriptive check names
3. **Documentation**: Add descriptions to complex checks
4. **Modular Files**: Split large configurations into multiple files
5. **Version Control**: Store configurations in version control
6. **Testing**: Test configurations in non-production environments
7. **Monitoring**: Set up alerts for check failures

## Example Complete Configuration

```yaml
version: 1

datasources:
  - name: warehouse
    type: postgresql
    host: ${WAREHOUSE_HOST}
    port: 5432
    db_name: analytics
    user: ${WAREHOUSE_USER}
    password: ${WAREHOUSE_PASSWORD}

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: weiser_metrics.db

checks:
  # Row count checks
  - name: orders_daily_count
    dataset: orders
    type: row_count
    condition: gt
    threshold: 100
    time_dimension:
      name: created_at
      granularity: day

  # Revenue validation
  - name: daily_revenue
    dataset: orders
    type: sum
    measure: order_amount
    condition: ge
    threshold: 10000
    filter: status = 'completed'
    time_dimension:
      name: created_at
      granularity: day

  # Data completeness
  - name: customer_data_completeness
    dataset: customers
    type: not_empty_pct
    dimensions: [email, phone]
    condition: le
    threshold: 0.05 # Max 5% NULL values

  # Anomaly detection
  - name: orders_anomaly
    dataset: metrics
    type: anomaly
    check_id: c5cee10898e30edd1c0dde3f24966b4c47890fcf247e5b630c2c156f7ac7ba22
    condition: between
    threshold: [-3.0, 3.0]

slack_url: ${SLACK_WEBHOOK_URL}
```
