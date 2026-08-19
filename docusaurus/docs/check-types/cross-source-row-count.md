# Cross-Source Row Count Check

The `cross_source_row_count` check validates that the same table (or set of tables) has a matching row count across two different datasources -- for example, comparing a replicated table in a data warehouse against its source database.

## Configuration

| Parameter | Required | Description |
|-----------|----------|--------------|
| `name` | Yes | Unique name for the check |
| `datasource` | Yes | The primary datasource |
| `dataset` | Yes | Table name (or list of table names) on the primary datasource |
| `type` | Yes | Must be `cross_source_row_count` |
| `compare_datasource` | Yes | The second datasource to compare against |
| `compare_dataset` | Yes | Table name (or list of table names) on `compare_datasource`, zipped pairwise with `dataset` |
| `condition` | Yes | Comparison operator, applied to the relative row-count difference |
| `threshold` | No | Maximum allowed relative difference (default: 0) |
| `filter` | No | WHERE clause conditions, applied to both sides |

`dataset` and `compare_dataset` are paired by position: the first entry of `dataset` is compared against the first entry of `compare_dataset`, and so on. Both lists must be the same length.

## Metric

For each table pair, the check computes the **relative difference** between the two row counts:

```
actual_value = abs(count_a - count_b) / max(count_a, count_b)
```

A value of `0` means the row counts are identical. A value of `0.1` means one side has 10% more/fewer rows than the other. If both counts are `0`, the check reports `0` (considered a match).

## Examples

### Exact match required

```yaml
- name: orders_row_count_parity
  datasource: warehouse
  dataset: orders
  type: cross_source_row_count
  compare_datasource: source_db
  compare_dataset: orders
  condition: eq
  threshold: 0
```

### Allow a small drift (e.g. replication lag)

```yaml
- name: orders_row_count_parity_tolerant
  datasource: warehouse
  dataset: orders
  type: cross_source_row_count
  compare_datasource: source_db
  compare_dataset: orders
  condition: le
  threshold: 0.02  # allow up to 2% relative difference
```

### Multiple table pairs in one check

```yaml
- name: replicated_tables_row_count
  datasource: warehouse
  dataset: [orders, customers, products]
  type: cross_source_row_count
  compare_datasource: source_db
  compare_dataset: [orders, customers, products]
  condition: le
  threshold: 0.01
```

## Behavior

- **One result per table pair**: each `dataset`/`compare_dataset` pair generates its own check result.
- **Naming convention**: results are named `{original_name}_{primary_table}__vs__{compare_table}`.
- **Both connections must be configured**: `datasource` and `compare_datasource` must both exist under `datasources` in the config.

## Related Checks

- [**Row Count**](./row-count.md) -- single-datasource row count check
- [**Cross-Source Fill Rate**](./cross-source-fill-rate.md) -- compare NULL fill-rate for a column across two datasources
