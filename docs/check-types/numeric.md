# Numeric Check

The `numeric` check allows you to validate custom numeric expressions and calculations. This is the most flexible check type for complex business logic validation.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Table name or SQL query |
| `type` | Yes | Must be `numeric` |
| `measure` | Yes | SQL expression to evaluate |
| `condition` | Yes | Comparison operator |
| `threshold` | Yes | Value to compare against |
| `dimensions` | No | Group by columns |
| `filter` | No | WHERE clause conditions |

## Examples

### Basic Numeric Check

```yaml
- name: average_order_value
  dataset: orders
  type: numeric
  measure: AVG(order_amount)
  condition: gt
  threshold: 50.0
```

This check ensures the average order value is greater than $50.

### Complex Expression

```yaml
- name: conversion_rate
  dataset: >
    SELECT 
      campaign_id,
      SUM(CASE WHEN status = 'converted' THEN 1 ELSE 0 END) as conversions,
      COUNT(*) as total_leads
    FROM marketing_leads 
    GROUP BY campaign_id
  type: numeric
  measure: CAST(conversions AS FLOAT) / CAST(total_leads AS FLOAT)
  condition: ge
  threshold: 0.05  # 5% minimum conversion rate
```

This check validates that conversion rates are at least 5%.

### Numeric with Dimensions

```yaml
- name: revenue_by_region
  dataset: sales
  type: numeric
  measure: SUM(revenue)
  dimensions: [region, quarter]
  condition: gt
  threshold: 100000
```

This check ensures each region/quarter combination has revenue > $100K.

### Using Window Functions

```yaml
- name: month_over_month_growth
  dataset: monthly_sales
  type: numeric
  measure: |
    (current_month_sales - LAG(current_month_sales) OVER (ORDER BY month)) 
    / LAG(current_month_sales) OVER (ORDER BY month)
  condition: gt
  threshold: 0.0  # Positive growth
```

This check validates month-over-month growth is positive.

### Statistical Analysis

The numeric check is powerful for statistical validation and data quality assessment.

#### Standard Deviation Analysis

```yaml
- name: price_variance_control
  dataset: products
  type: numeric
  measure: STDDEV(price)
  condition: le
  threshold: 50.0
  description: "Ensure price variance is within acceptable range"
```

This check ensures product prices don't vary too widely, indicating consistent pricing strategy.

#### Coefficient of Variation

```yaml
- name: sales_consistency
  dataset: daily_sales
  type: numeric
  measure: STDDEV(daily_revenue) / AVG(daily_revenue)
  condition: le
  threshold: 0.3  # Max 30% coefficient of variation
  dimensions: [region]
```

This validates that sales are relatively consistent across regions (low coefficient of variation).

#### Outlier Detection with Z-Score

```yaml
- name: outlier_detection
  dataset: |
    SELECT 
      customer_id,
      order_amount,
      (order_amount - AVG(order_amount) OVER()) / STDDEV(order_amount) OVER() as z_score
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
  type: numeric
  measure: COUNT(CASE WHEN ABS(z_score) > 3 THEN 1 END)
  condition: le
  threshold: 5  # Max 5 statistical outliers
```

This check counts orders with z-scores beyond 3 standard deviations, limiting extreme outliers.

#### Percentile Analysis

```yaml
- name: response_time_p95
  dataset: api_logs
  type: numeric
  measure: PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms)
  condition: le
  threshold: 2000  # 95th percentile under 2 seconds
  time_dimension:
    name: timestamp
    granularity: hour
```

This validates that 95% of API responses are under 2 seconds.

#### Interquartile Range (IQR) Analysis

```yaml
- name: transaction_amount_iqr
  dataset: transactions
  type: numeric
  measure: |
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) - 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount)
  condition: le
  threshold: 1000
  description: "Ensure transaction amounts have reasonable spread"
```

This check monitors the interquartile range to detect unusual spread in transaction amounts.

#### Correlation Analysis

```yaml
- name: price_quantity_correlation
  dataset: order_items
  type: numeric
  measure: |
    CORR(unit_price, quantity)
  condition: between
  threshold: [-0.3, 0.3]  # Weak correlation expected
  description: "Price and quantity should have minimal correlation"
```

This validates that price and quantity aren't strongly correlated, which might indicate pricing issues.

#### Data Distribution Validation

```yaml
- name: order_amount_skewness
  dataset: |
    SELECT 
      order_amount,
      AVG(order_amount) OVER() as mean_amount,
      STDDEV(order_amount) OVER() as std_amount
    FROM orders
    WHERE order_amount > 0
  type: numeric
  measure: |
    AVG(POWER((order_amount - mean_amount) / std_amount, 3))
  condition: between
  threshold: [-2.0, 2.0]  # Acceptable skewness range
  description: "Order amounts should have reasonable distribution"
```

This check calculates skewness to ensure order amounts follow a reasonable distribution.

#### Time Series Statistical Analysis

```yaml
- name: daily_sales_trend_analysis
  dataset: |
    SELECT 
      DATE(created_at) as sale_date,
      SUM(order_amount) as daily_total,
      ROW_NUMBER() OVER (ORDER BY DATE(created_at)) as day_number
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE(created_at)
  type: numeric
  measure: |
    CORR(day_number, daily_total)
  condition: ge
  threshold: -0.1  # Slight negative trend acceptable
  description: "Sales trend should not be strongly declining"
```

This analyzes sales trends over time using correlation with day numbers.

## Supported SQL Functions

The `measure` parameter supports any valid SQL expression, including:

- **Aggregation Functions**: `SUM()`, `AVG()`, `COUNT()`, `MIN()`, `MAX()`
- **Mathematical Functions**: `ROUND()`, `ABS()`, `SQRT()`, etc.
- **Conditional Logic**: `CASE WHEN ... THEN ... ELSE ... END`
- **Window Functions**: `LAG()`, `LEAD()`, `ROW_NUMBER()`, etc.
- **Type Casting**: `CAST(column AS FLOAT)`
- **Date Functions**: `DATE_TRUNC()`, `EXTRACT()`, etc.

## Use Cases

- **Business Metrics**: Revenue, conversion rates, customer metrics
- **Data Quality**: Calculate data quality scores
- **Performance KPIs**: Response times, success rates
- **Financial Validation**: Balances, totals, ratios
- **Custom Logic**: Any complex business rule

## Generated SQL

The numeric check generates SQL like:

```sql
SELECT AVG(order_amount)
FROM orders
WHERE status = 'completed'
```

With dimensions:
```sql
SELECT region, quarter, SUM(revenue)
FROM sales
GROUP BY region, quarter
```

## Example Results

```
✓ average_order_value: 67.50 (> 50.0)
✗ conversion_rate: 0.03 (≥ 0.05)
✓ revenue_by_region_east_q1: 125000 (> 100000)
```

## Tips

1. **Type Casting**: Use `CAST()` for precise decimal calculations
2. **NULL Handling**: Consider `COALESCE()` or `ISNULL()` for NULL values
3. **Performance**: Use appropriate indexes for complex expressions
4. **Testing**: Test expressions in your database before adding to checks

## Related Checks

- [**Sum**](./sum.md) - Simplified sum aggregation
- [**Measure**](./measure.md) - Cube.js specific measures