# Row Count Check

The `row_count` check validates the number of rows in a dataset. This is one of the most basic and commonly used data quality checks.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Table name or SQL query |
| `type` | Yes | Must be `row_count` |
| `condition` | Yes | Comparison operator |
| `threshold` | Yes | Expected row count value |
| `dimensions` | No | Group by columns |
| `filter` | No | WHERE clause conditions |

## Examples

### Basic Row Count Check

```yaml
- name: orders_minimum_rows
  dataset: orders
  type: row_count
  condition: gt
  threshold: 0
```

This check ensures the orders table has more than 0 rows.

### Row Count with Filter

```yaml
- name: active_users_count
  dataset: users
  type: row_count
  condition: ge
  threshold: 100
  filter: status = 'active'
```

This check validates that there are at least 100 active users.

### Row Count by Dimensions

```yaml
- name: orders_by_region
  dataset: orders
  type: row_count
  dimensions: [region, status]
  condition: gt
  threshold: 10
```

This check ensures each region/status combination has more than 10 orders.

### Row Count with Time Dimension

```yaml
- name: daily_orders_count
  dataset: orders
  type: row_count
  condition: gt
  threshold: 50
  time_dimension:
    name: created_at
    granularity: day
```

This check validates that each day has more than 50 orders.

## Use Cases

- **Data Freshness**: Ensure new data is being loaded
- **Completeness**: Verify expected data volume
- **Business Rules**: Validate minimum activity levels
- **Monitoring**: Track growth or decline in data volume

## Generated SQL

The row count check generates SQL similar to:

```sql
SELECT COUNT(*) 
FROM orders 
WHERE status = 'active'
```

With dimensions:
```sql
SELECT region, status, COUNT(*) 
FROM orders 
GROUP BY region, status
```