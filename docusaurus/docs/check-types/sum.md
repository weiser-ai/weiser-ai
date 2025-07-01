# Sum Check

The `sum` check validates the sum of a numeric column. This is a specialized version of the numeric check optimized for sum aggregations.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Table name or SQL query |
| `type` | Yes | Must be `sum` |
| `measure` | Yes | Column name to sum |
| `condition` | Yes | Comparison operator |
| `threshold` | Yes | Value to compare against |
| `dimensions` | No | Group by columns |
| `filter` | No | WHERE clause conditions |

## Examples

### Basic Sum Check

```yaml
- name: total_revenue
  dataset: orders
  type: sum
  measure: order_amount
  condition: ge
  threshold: 1000000
```

This check ensures total revenue is at least $1M.

### Sum with Filter

```yaml
- name: completed_orders_revenue
  dataset: orders
  type: sum
  measure: order_amount
  condition: gt
  threshold: 500000
  filter: status = 'completed'
```

This check validates that completed orders total more than $500K.

### Sum by Dimensions

```yaml
- name: revenue_by_region
  dataset: sales
  type: sum
  measure: revenue
  dimensions: [region]
  condition: ge
  threshold: 100000
```

This check ensures each region has at least $100K in revenue.

### Sum with Time Dimension

```yaml
- name: daily_sales_sum
  dataset: transactions
  type: sum
  measure: amount
  condition: gt
  threshold: 10000
  time_dimension:
    name: transaction_date
    granularity: day
```

This check validates daily sales exceed $10K.

### Multiple Dimensions

```yaml
- name: product_sales_by_region_quarter
  dataset: sales
  type: sum
  measure: sales_amount
  dimensions: [region, product_category, quarter]
  condition: ge
  threshold: 25000
```

This check ensures each region/product/quarter combination has at least $25K in sales.

## Use Cases

- **Revenue Validation**: Ensure minimum revenue targets
- **Financial Controls**: Validate total amounts in accounting
- **Performance Monitoring**: Track sum metrics over time
- **Data Quality**: Ensure calculated totals are reasonable
- **Business Rules**: Validate sum-based business requirements

## Generated SQL

The sum check generates SQL like:

```sql
SELECT SUM(order_amount)
FROM orders
WHERE status = 'completed'
```

With dimensions:
```sql
SELECT region, SUM(revenue)
FROM sales
GROUP BY region
```

## Example Results

```
✓ total_revenue: 1250000 (≥ 1000000)
✗ completed_orders_revenue: 450000 (> 500000)
✓ revenue_by_region_east: 125000 (≥ 100000)
✓ revenue_by_region_west: 175000 (≥ 100000)
```

## Data Types

The `measure` column should be numeric. Common types include:
- `INTEGER`
- `DECIMAL` / `NUMERIC`
- `FLOAT` / `DOUBLE`
- `MONEY` (database-specific)

## NULL Handling

- `SUM()` ignores NULL values automatically
- If all values are NULL, `SUM()` returns NULL
- Consider using filters or data cleaning if NULL handling is critical

## Performance Tips

1. **Indexes**: Ensure the measure column has appropriate indexes
2. **Partitioning**: Use time-based partitioning for large datasets
3. **Filters**: Apply filters to reduce data volume
4. **Materialized Views**: Pre-calculate sums for frequently checked aggregations

## Related Checks

- [**Numeric**](./numeric.md) - More flexible numeric expressions
- [**Min**](./min.md) - Minimum value validation
- [**Max**](./max.md) - Maximum value validation