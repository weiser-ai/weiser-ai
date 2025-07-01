# Not Empty Percentage Check

The `not_empty_pct` check validates that the percentage of NULL values in specified dimensions does not exceed a threshold. This check is similar to `not_empty` but uses percentage-based thresholds instead of absolute counts.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Table name or SQL query |
| `type` | Yes | Must be `not_empty_pct` |
| `dimensions` | Yes | List of columns to check for NULL values |
| `condition` | Yes | Comparison operator (typically `le` for "less than or equal") |
| `threshold` | No | Maximum allowed NULL percentage as decimal (0.0-1.0, default: 0.0) |
| `filter` | No | WHERE clause conditions |

## Examples

### Basic Percentage Check

```yaml
- name: customer_data_completeness_pct
  dataset: customers
  type: not_empty_pct
  dimensions: [phone, address]
  condition: le
  threshold: 0.05  # Allow up to 5% NULL values
```

This check ensures that no more than 5% of customers have NULL phone or address values.

### Strict Completeness

```yaml
- name: critical_fields_complete
  dataset: orders
  type: not_empty_pct
  dimensions: [customer_id, product_id]
  condition: le
  threshold: 0.0  # No NULL values allowed (0%)
```

This check requires 100% completeness for critical order fields.

### Higher Tolerance for Optional Fields

```yaml
- name: optional_fields_check
  dataset: users
  type: not_empty_pct
  dimensions: [middle_name, phone_secondary]
  condition: le
  threshold: 0.8  # Allow up to 80% NULL values
```

This check allows optional fields to have high NULL rates.

### Default Threshold

```yaml
- name: required_fields_pct
  dataset: products
  type: not_empty_pct
  dimensions: [product_name, price]
  condition: le
  # No threshold specified - defaults to 0.0 (0%)
```

When no threshold is specified, it defaults to 0.0 (no NULL values allowed).

## Threshold Format

Thresholds are specified as decimal values between 0.0 and 1.0:

- `0.0` = 0% (no NULL values allowed)
- `0.05` = 5% (up to 5% NULL values allowed)
- `0.1` = 10% (up to 10% NULL values allowed)
- `0.5` = 50% (up to 50% NULL values allowed)
- `1.0` = 100% (all values can be NULL)

## Behavior

- **Individual Dimension Checks**: The check runs separately for each dimension specified
- **Result per Dimension**: Each dimension generates its own check result
- **Naming Convention**: Results are named as `{original_name}_{dimension}_not_empty_pct`
- **Percentage Calculation**: Automatically calculates NULL percentage using SQL CAST operations

## Use Cases

- **Data Quality with Tolerance**: Allow some missing data while maintaining quality
- **Business Rules**: Validate acceptable completeness rates
- **SLA Monitoring**: Ensure data completeness meets service level agreements
- **Gradual Improvement**: Set improving targets over time (e.g., reduce from 10% to 5%)

## Generated SQL

For each dimension, the check generates percentage calculation SQL like:

```sql
SELECT CAST(SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT)
FROM customers
WHERE status = 'active'
```

This automatically calculates the percentage as a decimal (e.g., 0.05 for 5%).

## Example Results

If checking `phone` and `email` dimensions with a 5% threshold, you might get:

```
✓ customer_completeness_phone_not_empty_pct: 0.03 (3% ≤ 5%)
✗ customer_completeness_email_not_empty_pct: 0.08 (8% > 5%)
```

## Comparison with Not Empty Check

| Aspect | not_empty | not_empty_pct |
|--------|-----------|---------------|
| Threshold Type | Absolute count | Percentage (0.0-1.0) |
| Use Case | Fixed datasets | Variable dataset sizes |
| Example | ≤ 100 NULL values | ≤ 5% NULL values |
| Scalability | Poor for growing data | Scales with data size |

## Related Checks

- [**Not Empty**](./not-empty.md) - Similar check but using absolute count thresholds