# Check Types

Weiser supports various types of data quality checks to validate different aspects of your data. Each check type serves a specific purpose and can be configured with different conditions and thresholds.

## Available Check Types

### Basic Checks
- [**Row Count**](./row-count.md) - Validates the number of rows in a dataset
- [**Numeric**](./numeric.md) - Custom numeric expressions and calculations  
- [**Sum**](./sum.md) - Validates the sum of a numeric column
- [**Min**](./min.md) - Validates the minimum value of a column
- [**Max**](./max.md) - Validates the maximum value of a column
- [**Measure**](./measure.md) - Cube.js specific measure validation

### Data Completeness Checks
- [**Not Empty**](./not-empty.md) - Validates NULL values in dimensions (count-based)
- [**Not Empty Percentage**](./not-empty-pct.md) - Validates NULL values in dimensions (percentage-based)

### Advanced Checks
- [**Anomaly Detection**](./anomaly.md) - Detects anomalies using statistical methods

## Common Configuration

All checks share common configuration options:

- **name**: Unique identifier for the check
- **dataset**: Target table or SQL query
- **type**: The check type (see list above)
- **condition**: Comparison operator (`gt`, `ge`, `lt`, `le`, `eq`, `neq`, `between`)
- **threshold**: Value(s) to compare against
- **dimensions**: Group by columns (optional)
- **filter**: WHERE clause conditions (optional)
- **time_dimension**: Time-based aggregation (optional)

## Example Configuration

```yaml
checks:
  - name: orders_row_count
    dataset: orders
    type: row_count
    condition: gt
    threshold: 1000
    
  - name: revenue_sum_by_region
    dataset: sales
    type: sum
    measure: revenue
    dimensions: [region]
    condition: ge
    threshold: 50000
```