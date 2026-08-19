# Cross-Source Fill Rate Check

The `cross_source_fill_rate` check validates that a column has a matching fill-rate (percentage of non-NULL values) across the same table on two different datasources.

## Configuration

| Parameter | Required | Description |
|-----------|----------|--------------|
| `name` | Yes | Unique name for the check |
| `datasource` | Yes | The primary datasource |
| `dataset` | Yes | Table name (or list of table names) on the primary datasource |
| `type` | Yes | Must be `cross_source_fill_rate` |
| `dimensions` | Yes | List of columns to compare fill-rate on (applied to every table pair) |
| `compare_datasource` | Yes | The second datasource to compare against |
| `compare_dataset` | Yes | Table name (or list of table names) on `compare_datasource`, zipped pairwise with `dataset` |
| `condition` | Yes | Comparison operator, applied to the relative fill-rate difference |
| `threshold` | No | Maximum allowed relative difference (default: 0) |
| `filter` | No | WHERE clause conditions, applied to both sides |

`dataset` and `compare_dataset` are paired by position, same as [`cross_source_row_count`](./cross-source-row-count.md). `dimensions` names are assumed to match on both sides.

## Metric

For each table pair and each dimension, the check computes each side's fill-rate (`non-null count / total count`), then the **relative difference** between the two:

```
actual_value = abs(fill_rate_a - fill_rate_b) / max(fill_rate_a, fill_rate_b)
```

A value of `0` means both sides have identical fill-rates. If both sides have a fill-rate of `0`, the check reports `0` (considered a match).

## Examples

### Exact match required

```yaml
- name: customers_fill_rate_parity
  datasource: warehouse
  dataset: customers
  type: cross_source_fill_rate
  dimensions: [email, phone]
  compare_datasource: source_db
  compare_dataset: customers
  condition: eq
  threshold: 0
```

### Allow a small drift

```yaml
- name: customers_fill_rate_parity_tolerant
  datasource: warehouse
  dataset: customers
  type: cross_source_fill_rate
  dimensions: [email, phone]
  compare_datasource: source_db
  compare_dataset: customers
  condition: le
  threshold: 0.05  # allow up to 5% relative difference in fill-rate
```

## Behavior

- **One result per table pair per dimension**: e.g. two table pairs with two dimensions each produce four results.
- **Naming convention**: results are named `{original_name}_{primary_table}__vs__{compare_table}__{dimension}`.
- **Both connections must be configured**: `datasource` and `compare_datasource` must both exist under `datasources` in the config.

## Related Checks

- [**Not Empty Percentage**](./not-empty-pct.md) -- single-datasource NULL percentage check
- [**Cross-Source Row Count**](./cross-source-row-count.md) -- compare row counts across two datasources
