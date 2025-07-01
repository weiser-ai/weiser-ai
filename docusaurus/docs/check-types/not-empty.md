# Not Empty Check

The `not_empty` check validates that the number of NULL values in specified dimensions does not exceed a threshold. This check helps ensure data completeness for critical columns.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Table name or SQL query |
| `type` | Yes | Must be `not_empty` |
| `dimensions` | Yes | List of columns to check for NULL values |
| `condition` | Yes | Comparison operator (typically `le` for "less than or equal") |
| `threshold` | No | Maximum allowed NULL count (default: 0) |
| `filter` | No | WHERE clause conditions |

## Examples

### Basic Not Empty Check

```yaml
- name: customer_data_completeness
  dataset: customers
  type: not_empty
  dimensions: [customer_id, email]
  condition: le
  threshold: 0
```

This check ensures that both `customer_id` and `email` columns have no NULL values.

### Not Empty with Tolerance

```yaml
- name: order_fields_mostly_complete
  dataset: orders
  type: not_empty
  dimensions: [customer_id, product_id, order_date]
  condition: le
  threshold: 5
```

This check allows up to 5 NULL values in each of the specified columns.

### Not Empty with Filter

```yaml
- name: active_user_completeness
  dataset: users
  type: not_empty
  dimensions: [first_name, last_name, email]
  condition: le
  threshold: 0
  filter: status = 'active'
```

This check ensures active users have complete name and email information.

### Default Threshold

```yaml
- name: required_fields_check
  dataset: products
  type: not_empty
  dimensions: [product_name, price]
  condition: le
  # No threshold specified - defaults to 0
```

When no threshold is specified, it defaults to 0 (no NULL values allowed).

## Behavior

- **Individual Dimension Checks**: The check runs separately for each dimension specified
- **Result per Dimension**: Each dimension generates its own check result
- **Naming Convention**: Results are named as `{original_name}_{dimension}_not_empty`

## Use Cases

- **Data Quality**: Ensure critical fields are populated
- **Business Rules**: Validate required information is present
- **ETL Validation**: Check data completeness after transformations
- **Compliance**: Ensure mandatory fields meet requirements

## Generated SQL

For each dimension, the check generates SQL like:

```sql
SELECT SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)
FROM customers
WHERE status = 'active'
```

## Example Results

If checking `customer_id` and `email` dimensions, you might get results like:

```
✓ customer_data_completeness_customer_id_not_empty: 0 NULL values (≤ 0)
✗ customer_data_completeness_email_not_empty: 3 NULL values (≤ 0)
```

## Related Checks

- [**Not Empty Percentage**](./not-empty-pct.md) - Similar check but using percentage thresholds