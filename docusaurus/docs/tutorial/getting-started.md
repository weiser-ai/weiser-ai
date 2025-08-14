# Getting Started with Weiser

Get up and running with Weiser data quality checks in minutes. This guide will walk you through installation, minimal configuration, and running your first data quality checks.

## Installation

Install Weiser using pip:

```bash
pip install weiser-ai
```

## Prerequisites

You'll need:

- **PostgreSQL database** with sample data
- **Database credentials** (host, port, username, password, database name)
- **Python 3.8+**

## Quick Start

### 1. Create Your First Configuration

Create a file called `weiser-config.yaml`:

```yaml
version: 1

# Database connection
datasources:
  - name: default
    type: postgresql
    host: localhost
    port: 5432
    db_name: your_database
    user: your_username
    password: your_password

# Metric storage (uses local DuckDB file)
connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: weiser_metrics.db

# Data quality checks
checks:
  # Basic row count check
  - name: orders_exist
    dataset: orders
    type: row_count
    condition: gt
    threshold: 0
    description: "Ensure orders table has data"

  # Check for recent data
  - name: recent_orders
    dataset: orders
    type: row_count
    condition: gt
    threshold: 10
    filter: created_at >= CURRENT_DATE - INTERVAL '7 days'
    description: "Ensure we have recent orders"

  # Revenue validation
  - name: positive_revenue
    dataset: orders
    type: sum
    measure: order_amount
    condition: gt
    threshold: 0
    filter: status = 'completed'
    description: "Ensure completed orders have positive revenue"
```

### 2. Test Your Configuration

First, validate your configuration without running checks:

```bash
# Using default .env file
weiser compile weiser-config.yaml -v

# Using custom .env file
weiser compile weiser-config.yaml -v --env-file /path/to/custom.env
```

This will:

- ✅ Validate your YAML syntax
- ✅ Check database connectivity
- ✅ Verify table access
- ✅ Generate SQL queries for review

### 3. Run Your First Checks

Execute the data quality checks:

```bash
# Using default .env file
weiser run weiser-config.yaml -v

# Using custom .env file
weiser run weiser-config.yaml -v --env-file /path/to/custom.env
```

Expected output:

```
✅ orders_exist: 1,247 rows (> 0) - PASSED
✅ recent_orders: 89 rows (> 10) - PASSED
✅ positive_revenue: $45,231.50 (> 0) - PASSED

All checks passed! 🎉
```

[![Watch the CLI Demo](https://cdn.loom.com/sessions/thumbnails/ce75ad760c324733a36c637a9f8fe826-401f2819c5918c19-full-play.gif)](https://www.loom.com/share/ce75ad760c324733a36c637a9f8fe826)

## Environment Variables (Recommended)

For security, use environment variables for sensitive data:

```yaml
# weiser-config.yaml
datasources:
  - name: default
    type: postgresql
    host: {{ DB_HOST }}
    port: {{ DB_PORT }}
    db_name: {{ DB_NAME }}
    user: {{ DB_USER }}
    password: {{ DB_PASSWORD }}
```

Create a `.env` file in your project directory:

```bash
# .env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
```

Or use a custom .env file location:

```bash
# Use custom .env file
weiser run weiser-config.yaml --env-file /path/to/custom.env

# Short form
weiser run weiser-config.yaml -e /path/to/custom.env
```

## Common Check Patterns

### Data Freshness

```yaml
- name: daily_data_check
  dataset: transactions
  type: row_count
  condition: gt
  threshold: 100
  time_dimension:
    name: created_at
    granularity: day
```

### Data Completeness

```yaml
- name: customer_data_complete
  dataset: customers
  type: not_empty_pct
  dimensions: [email, phone]
  condition: le
  threshold: 0.05 # Max 5% NULL values
```

### Business Logic Validation

```yaml
- name: average_order_value
  dataset: orders
  type: numeric
  measure: AVG(order_amount)
  condition: ge
  threshold: 25.0
  filter: status = 'completed'
```

### Multi-Table Checks

```yaml
- name: critical_tables_exist
  dataset: [orders, customers, products]
  type: row_count
  condition: gt
  threshold: 0
```

## Next Steps

### 📊 Add More Check Types

Explore all available check types in our [Check Types Documentation](../check-types/index.md):

- [Row Count](../check-types/row-count.md) - Basic data volume validation
- [Numeric](../check-types/numeric.md) - Custom calculations and business logic
- [Data Completeness](../check-types/not-empty.md) - NULL value monitoring
- [Anomaly Detection](../check-types/anomaly.md) - Statistical outlier detection

### ⚙️ Advanced Configuration

Learn about advanced features in the [Configuration Guide](../configuration.md):

- Multiple datasources
- Complex filters and dimensions
- Time-based aggregations
- Slack notifications

### 🔄 Automation

Integrate Weiser into your data pipeline:

```bash
# Add to your CI/CD pipeline
weiser run production-config.yaml

# Schedule with cron
0 8 * * * /usr/local/bin/weiser run /path/to/config.yaml
```

### 📈 Monitoring Dashboard

Once you have checks running regularly, explore the visualization dashboard:

```bash
cd weiser-ui
pip install -r requirements.txt
streamlit run app.py
```

[![Watch the Dashboard Demo](https://cdn.loom.com/sessions/thumbnails/3154b4ce21ea4aaa917066991eaf1fb6-aca9c23da977e100-full-play.gif)](https://www.loom.com/share/3154b4ce21ea4aaa917066991eaf1fb6)

The dashboard provides:

- **Historical Trends**: Track check results over time
- **Failure Analysis**: Investigate failed checks
- **Performance Metrics**: Monitor check execution times
- **Data Quality Scores**: Overall system health view

## Troubleshooting

### Connection Issues

```bash
# Test database connectivity
psql -h localhost -U your_username -d your_database -c "SELECT 1;"
```

### Permission Issues

Ensure your database user has `SELECT` permissions on target tables:

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_username;
```

### Configuration Validation

Use the compile command to validate your setup:

```bash
weiser compile your-config.yaml --verbose
```

## Getting Help

- 📚 [Check Types Documentation](../check-types/index.md)
- ⚙️ [Configuration Reference](../configuration.md)
- 💬 [GitHub Issues](https://github.com/weiser-ai/weiser/issues)

Ready to ensure your data quality? Start building more comprehensive checks with our detailed [Check Types Documentation](../check-types/index.md)!
