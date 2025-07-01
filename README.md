# Weiser

Data Quality Framework

## Introduction

Weiser is a data quality framework designed to help you ensure the integrity and accuracy of your data. It provides a set of tools and checks to validate your data and detect anomalies. It also includes a dashboard to visualize the results of the checks.

## Installation

To install Weiser, use the following command:

```sh
pip install weiser-ai
```

## Usage

### Run example checks

Connections are defined at the datasources section in the config file see: `examples/example.yaml`.

Run checks in verbose mode:

```sh
weiser run examples/example.yaml -v
```

[![Watch the CLI Demo](https://cdn.loom.com/sessions/thumbnails/ce75ad760c324733a36c637a9f8fe826-401f2819c5918c19-full-play.gif)](https://www.loom.com/share/ce75ad760c324733a36c637a9f8fe826)

Compile checks only in verbose mode:

```sh
weiser compile examples/example.yaml -v
```

### Run dashboard

```sh
cd weiser-ui
pip install -r requirements.txt
streamlit run app.py
```

[![Watch the Dashboard Demo](https://cdn.loom.com/sessions/thumbnails/3154b4ce21ea4aaa917066991eaf1fb6-aca9c23da977e100-full-play.gif)](https://www.loom.com/share/3154b4ce21ea4aaa917066991eaf1fb6)

## Configuration

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

Missing values check

```yaml
- name: customer data quality
  dataset: orders
  type: not_empty
  dimensions: ["customer_id", "product_id", "order_date"]
  condition: le
  # Allow up to 5 NULL values per dimension
  threshold: 5
  filter: "status = 'active'"
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

## Contributing

We welcome contributions!

## License

This project is licensed under the Apache 2.0 License. See the `LICENSE` file for more details.
