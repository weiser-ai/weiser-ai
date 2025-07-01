# Max Check

The `max` check validates the maximum value of a numeric column. This check helps ensure data quality by validating that the largest values in your dataset meet expectations and don't exceed reasonable bounds.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Table name or SQL query |
| `type` | Yes | Must be `max` |
| `measure` | Yes | Column name to find maximum |
| `condition` | Yes | Comparison operator |
| `threshold` | Yes | Value to compare against |
| `dimensions` | No | Group by columns |
| `filter` | No | WHERE clause conditions |

## Examples

### Basic Max Check

```yaml
- name: maximum_order_amount
  dataset: orders
  type: max
  measure: order_amount
  condition: le
  threshold: 10000
```

This check ensures no order exceeds $10,000.

### Max with Filter

```yaml
- name: max_discount_percentage
  dataset: promotions
  type: max
  measure: discount_percentage
  condition: le
  threshold: 0.5
  filter: status = 'active'
```

This check validates that active promotions don't exceed 50% discount.

### Max by Dimensions

```yaml
- name: max_salary_by_level
  dataset: employees
  type: max
  measure: salary
  dimensions: [job_level]
  condition: le
  threshold: 200000
```

This check ensures maximum salary per job level doesn't exceed $200K.

### Max with Time Dimension

```yaml
- name: daily_max_temperature
  dataset: sensor_readings
  type: max
  measure: temperature
  condition: le
  threshold: 45
  time_dimension:
    name: reading_date
    granularity: day
```

This check validates daily maximum temperature stays below 45°.

## Use Cases

- **Data Quality**: Ensure no unreasonably high values
- **Business Rules**: Validate maximum acceptable limits
- **Range Validation**: Check upper bounds of data ranges
- **Outlier Detection**: Identify potentially problematic maximum values
- **Security**: Prevent values that might indicate data issues or attacks
- **Compliance**: Ensure maximum limits are not exceeded

## Generated SQL

The max check generates SQL like:

```sql
SELECT MAX(order_amount)
FROM orders
WHERE status = 'completed'
```

With dimensions:
```sql
SELECT job_level, MAX(salary)
FROM employees
GROUP BY job_level
```

## Example Results

```
✓ maximum_order_amount: 8500.00 (≤ 10000)
✗ max_discount_percentage: 0.75 (≤ 0.5)
✓ max_salary_by_level_senior: 180000 (≤ 200000)
✗ max_salary_by_level_executive: 250000 (≤ 200000)
```

## Data Types

The `measure` column should be numeric or comparable. Common types include:
- `INTEGER`
- `DECIMAL` / `NUMERIC`
- `FLOAT` / `DOUBLE`
- `DATE` / `TIMESTAMP`
- `TIME`

## NULL Handling

- `MAX()` ignores NULL values automatically
- If all values are NULL, `MAX()` returns NULL
- The check will fail if MAX() returns NULL (unless specifically handled)

## Common Patterns

### Fraud Detection
```yaml
- name: suspicious_transaction_amount
  dataset: transactions
  type: max
  measure: amount
  condition: le
  threshold: 100000
  dimensions: [user_id]
```

### Performance Monitoring
```yaml
- name: max_response_time
  dataset: api_logs
  type: max
  measure: response_time_ms
  condition: le
  threshold: 30000  # 30 seconds max
```

### Data Validation
```yaml
- name: age_upper_bound
  dataset: users
  type: max
  measure: age
  condition: le
  threshold: 120  # Reasonable max age
```

### System Limits
```yaml
- name: max_file_size
  dataset: uploads
  type: max
  measure: file_size_bytes
  condition: le
  threshold: 104857600  # 100MB max
```

## Performance Tips

1. **Indexes**: Ensure the measure column has appropriate indexes
2. **Partitioning**: Use partitioning for time-based queries
3. **Filters**: Apply filters to reduce data scanning
4. **Statistics**: Keep table statistics updated for query optimization

## Alerting Scenarios

Max checks are particularly useful for alerting on:
- **Anomalous spikes** in transaction amounts
- **Performance degradation** (response times, queue lengths)
- **Resource exhaustion** (disk usage, memory consumption)
- **Data quality issues** (unrealistic values)

## Related Checks

- [**Min**](./min.md) - Minimum value validation
- [**Sum**](./sum.md) - Sum aggregation validation
- [**Numeric**](./numeric.md) - Custom numeric expressions