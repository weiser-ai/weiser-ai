# Anomaly Detection Check

The `anomaly` check uses statistical methods to detect unusual patterns in your metrics over time. It employs Median Absolute Deviation (MAD) to identify values that deviate significantly from historical patterns.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Unique name for the check |
| `dataset` | Yes | Must be `metrics` (references historical data) |
| `type` | Yes | Must be `anomaly` |
| `check_id` | Yes | ID of the metric to analyze for anomalies |
| `condition` | Yes | Usually `between` for z-score bounds |
| `threshold` | Yes | Z-score range (e.g., [-3.5, 3.5]) |

## How It Works

1. **Historical Data**: Analyzes past values from the metrics store
2. **MAD Calculation**: Computes Median Absolute Deviation for robust statistics
3. **Z-Score**: Calculates modified z-score for the latest value
4. **Threshold Check**: Compares z-score against configured bounds

## Examples

### Basic Anomaly Detection

```yaml
- name: orders_anomaly_detection
  dataset: metrics
  type: anomaly
  check_id: c5cee10898e30edd1c0dde3f24966b4c47890fcf247e5b630c2c156f7ac7ba22
  condition: between
  threshold: [-3.5, 3.5]
```

This check detects anomalies in order count using 3.5 standard deviations.

### Strict Anomaly Detection

```yaml
- name: revenue_strict_anomaly
  dataset: metrics
  type: anomaly
  check_id: abc123def456...
  condition: between
  threshold: [-2.0, 2.0]
```

This uses tighter bounds (2 standard deviations) for more sensitive detection.

### Relaxed Anomaly Detection

```yaml
- name: user_activity_relaxed
  dataset: metrics
  type: anomaly
  check_id: xyz789uvw012...
  condition: between
  threshold: [-5.0, 5.0]
```

This uses wider bounds for less sensitive anomaly detection.

## Getting Check IDs

To find the `check_id` for your metrics:

1. **Run the check first**: Execute the underlying metric check
2. **Check logs**: The check ID is generated and logged
3. **Query metrics store**: Look in the metrics table for the check_id
4. **Generate manually**: Use the same hashing algorithm (SHA256)

```sql
-- Query to find check IDs
SELECT DISTINCT check_id, check_name 
FROM metrics 
ORDER BY run_time DESC;
```

## Statistical Method

### Median Absolute Deviation (MAD)

```
MAD = median(|xi - median(x)|)
```

### Modified Z-Score

```
z-score = 0.6745 * (x - median) / MAD
```

This method is more robust than standard deviation because it's less affected by outliers.

## Minimum Data Requirements

- **Minimum 5 data points** required for statistical analysis
- **Recommended 30+ points** for reliable anomaly detection
- **Regular intervals** work best (daily, hourly, etc.)

## Use Cases

- **System Monitoring**: Detect unusual system behavior
- **Business Metrics**: Identify unexpected changes in KPIs
- **Data Quality**: Find data pipeline issues
- **Performance**: Monitor response times and error rates
- **Security**: Detect unusual access patterns

## Example Results

```
✓ orders_anomaly_detection: z-score 1.2 (within [-3.5, 3.5])
✗ revenue_strict_anomaly: z-score -2.8 (outside [-2.0, 2.0])
✓ user_activity_relaxed: z-score 3.1 (within [-5.0, 5.0])
```

## Workflow Integration

### Step 1: Set up base metric
```yaml
- name: daily_order_count
  dataset: orders
  type: row_count
  condition: gt
  threshold: 0
  time_dimension:
    name: created_at
    granularity: day
```

### Step 2: Run and collect data
```bash
weiser run config.yaml
```

### Step 3: Add anomaly detection
```yaml
- name: daily_order_count_anomaly
  dataset: metrics
  type: anomaly
  check_id: <generated_check_id>
  condition: between
  threshold: [-3.0, 3.0]
```

## Threshold Guidelines

| Sensitivity | Z-Score Range | Use Case |
|-------------|---------------|----------|
| Very Strict | [-1.5, 1.5] | Critical systems |
| Strict | [-2.0, 2.0] | Important metrics |
| Normal | [-3.0, 3.0] | Standard monitoring |
| Relaxed | [-3.5, 3.5] | Noisy data |
| Very Relaxed | [-5.0, 5.0] | Exploratory analysis |

## Limitations

- **Seasonal Data**: May not handle seasonal patterns well
- **Trend Data**: Works best with stationary time series
- **Small Datasets**: Requires sufficient historical data
- **Sudden Changes**: May flag legitimate business changes as anomalies

## Best Practices

1. **Start Relaxed**: Begin with wider thresholds and tighten as needed
2. **Monitor Trends**: Consider detrending data for better results
3. **Regular Review**: Periodically review and adjust thresholds
4. **Context Matters**: Combine with business context for interpretation
5. **Multiple Metrics**: Use alongside other validation checks

## Troubleshooting

### Insufficient Data
```
Error: Not enough historical data points (need at least 5)
```
**Solution**: Run base checks more frequently to build history

### Check ID Not Found
```
Error: No data found for check_id: abc123...
```
**Solution**: Verify the check_id exists in metrics table

### All Values Flagged
**Symptoms**: Every value detected as anomaly
**Solution**: Widen threshold range or check data quality

## Related Checks

- [**Row Count**](./row-count.md) - Base metric for anomaly detection
- [**Numeric**](./numeric.md) - Custom metrics for anomaly analysis
- [**Sum**](./sum.md) - Revenue/amount anomaly detection