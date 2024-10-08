# Weiser

Data Quality Framework

# Install

```sh
pip install weiser-ai
```

# Test it yourself!

## Run test docker-compose image

A database is needed for example tests.

```bash
docker-compose up
```

## Run test checks

It connects to a postgres db from the example docker-compose image.

Run checks in verbose mode

```sh
weiser run examples/example.yaml -v
```

# Check definitions

Simple count check defintion

```yaml
- name: test row_count
  dataset: orders
  type: row_count
  condition: gt
  threshold: 0
```

Custom sql definition

```yaml
- name: test numeric
  dataset: orders
  type: numeric
  measure: sum(budgeted_amount::numeric::float)
  condition: gt
  threshold: 0
```

Target multiple datasets with the same check definition

```yaml
- name: test row_count
  dataset: [orders, vendors]
  type: row_count
  condition: gt
  threshold: 0
```

Check individual group by values in a check

```yaml
- name: test row_count groupby
  dataset: vendors
  type: row_count
  dimensions:
    - tenant_id
  condition: gt
  threshold: 0
```

Time aggregation check with granularity

```yaml
- name: test numeric gt sum yearly
  dataset: orders
  type: sum
  measure: budgeted_amount::numeric::float
  condition: gt
  threshold: 0
  time_dimension:
    name: _updated_at
    granularity: year
```

Custom SQL expression for dataset and filter usage

```yaml
- name: test numeric completed
  dataset: >
    SELECT * FROM orders o LEFT JOIN orders_status os ON o.order_id = os.order_id
  type: numeric
  measure: sum(budgeted_amount::numeric::float)
  condition: gt
  threshold: 0
  filter: status = 'FULFILLED'
```

Anomaly detection check

```yaml
- name: test anomaly
  # anomaly test should always target metrics metadata dataset
  dataset: metrics
  type: anomaly
  # References Orders row count.
  check_id: c5cee10898e30edd1c0dde3f24966b4c47890fcf247e5b630c2c156f7ac7ba22
  condition: between
  # long tails of normal distribution for Z-score.
  threshold: [-3.5, 3.5]
```
