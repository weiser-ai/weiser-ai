# Min Check

The `min` check validates the minimum value of a numeric column. This check helps ensure data quality by validating that the smallest values in your dataset meet expectations.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Table name or SQL query |
| `type` | Yes | Must be `min` |
| `measure` | Yes | Column name to find minimum |
| `condition` | Yes | Comparison operator |
| `threshold` | Yes | Value to compare against |
| `dimensions` | No | Group by columns |
| `filter` | No | WHERE clause conditions |

## Examples

### Basic Min Check

```yaml
- name: minimum_order_amount
  dataset: orders
  type: min
  measure: order_amount
  condition: ge
  threshold: 1.0
```

This check ensures the smallest order is at least $1.

### Min with Filter

```yaml
- name: min_product_price
  dataset: products
  type: min
  measure: price
  condition: gt
  threshold: 0
  filter: status = 'active'
```

This check validates that all active products have a price greater than 0.

### Min by Dimensions

```yaml
- name: min_salary_by_department
  dataset: employees
  type: min
  measure: salary
  dimensions: [department]
  condition: ge
  threshold: 30000
```

This check ensures minimum salary in each department is at least $30K.

### Min with Time Dimension

```yaml
- name: daily_min_temperature
  dataset: sensor_readings
  type: min
  measure: temperature
  condition: gt
  threshold: -40
  time_dimension:
    name: reading_date
    granularity: day
```

This check validates daily minimum temperature is above -40°.

## Use Cases

- **Data Quality**: Ensure no unreasonably low values
- **Business Rules**: Validate minimum acceptable values
- **Range Validation**: Check lower bounds of data ranges
- **Outlier Detection**: Identify potentially problematic minimum values
- **Compliance**: Ensure minimum standards are met

## Generated SQL

The min check generates SQL like:

```sql
SELECT MIN(order_amount)
FROM orders
WHERE status = 'completed'
```

With dimensions:
```sql
SELECT department, MIN(salary)
FROM employees
GROUP BY department
```

## Example Results

```
✓ minimum_order_amount: 5.50 (≥ 1.0)
✗ min_product_price: 0.00 (> 0)
✓ min_salary_by_department_engineering: 65000 (≥ 30000)
✓ min_salary_by_department_sales: 35000 (≥ 30000)
```

## Data Types

The `measure` column should be numeric or comparable. Common types include:
- `INTEGER`
- `DECIMAL` / `NUMERIC` 
- `FLOAT` / `DOUBLE`
- `DATE` / `TIMESTAMP`
- `TIME`

## NULL Handling

- `MIN()` ignores NULL values automatically
- If all values are NULL, `MIN()` returns NULL
- The check will fail if MIN() returns NULL (unless specifically handled)

## Common Patterns

### Price Validation
```yaml
- name: positive_prices
  dataset: products
  type: min
  measure: price
  condition: gt
  threshold: 0
```

### Date Range Validation
```yaml
- name: recent_data
  dataset: transactions
  type: min
  measure: transaction_date
  condition: ge
  threshold: '2024-01-01'
```

### Performance Metrics
```yaml
- name: minimum_response_time
  dataset: api_logs
  type: min
  measure: response_time_ms
  condition: le
  threshold: 5000  # Max acceptable minimum response time
```

## Performance Tips

1. **Indexes**: Ensure the measure column has appropriate indexes
2. **Partitioning**: Use partitioning for time-based queries
3. **Filters**: Apply filters to reduce data scanning
4. **Statistics**: Keep table statistics updated for query optimization

## Related Checks

- [**Max**](./max.md) - Maximum value validation
- [**Sum**](./sum.md) - Sum aggregation validation
- [**Numeric**](./numeric.md) - Custom numeric expressions