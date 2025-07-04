# Supported Datasources

Weiser supports multiple database systems and data platforms for running data quality checks. This page provides detailed configuration and setup instructions for each supported datasource.

## Overview

Currently supported datasources:

| Datasource     | Type         | Status             | Use Cases                        |
| -------------- | ------------ | ------------------ | -------------------------------- |
| **PostgreSQL** | `postgresql` | ✅ Fully Supported | OLTP, Analytics, Data Warehouses |
| **MySQL**      | `mysql`      | ✅ Fully Supported | OLTP, Web Applications           |
| **Snowflake**  | `snowflake`  | ✅ Fully Supported | Cloud Data Warehouse             |
| **Databricks** | `databricks` | ✅ Fully Supported | Cloud Data Warehouse             |
| **BigQuery**   | `bigquery`   | ✅ Fully Supported | Cloud Data Warehouse             |
| **Cube.js**    | `cube`       | ✅ Fully Supported | Semantic Layer, Business Metrics |
| **Redshift**   | `redshift`   | 📋 Planned         | Cloud Data Warehouse             |
| **Athena**     | `athena`     | 📋 Planned         | Cloud Data Warehouse             |
| **Trino**      | `trino`      | 📋 Planned         | Distributed Data Warehouse       |

## PostgreSQL

PostgreSQL is the primary supported datasource with full feature compatibility for all check types.

### Configuration

#### Basic Connection

```yaml
datasources:
  - name: postgres_prod
    type: postgresql
    host: localhost
    port: 5432
    db_name: production
    user: weiser_user
    password: secure_password
```

#### Connection URI

```yaml
datasources:
  - name: postgres_warehouse
    type: postgresql
    uri: postgresql://user:password@host:5432/database
```

#### Environment Variables (Recommended)

```yaml
datasources:
  - name: postgres_prod
    type: postgresql
    host: {{POSTGRES_HOST}}
    port: {{POSTGRES_PORT}}
    db_name: {{POSTGRES_DB}}
    user: {{POSTGRES_USER}}
    password: {{POSTGRES_PASSWORD}}
```

### Connection Parameters

| Parameter  | Required | Default | Description                                                |
| ---------- | -------- | ------- | ---------------------------------------------------------- |
| `name`     | Yes      | -       | Unique datasource identifier                               |
| `type`     | Yes      | -       | Must be `postgresql`                                       |
| `host`     | Yes\*    | -       | Database server hostname                                   |
| `port`     | No       | 5432    | Database server port                                       |
| `db_name`  | Yes\*    | -       | Database name                                              |
| `user`     | Yes\*    | -       | Database username                                          |
| `password` | Yes\*    | -       | Database password                                          |
| `uri`      | Yes\*    | -       | Complete connection URI (alternative to individual params) |

\*Either individual parameters OR uri is required

### Setup Requirements

#### 1. Database User Permissions

```sql
-- Create dedicated user for Weiser
CREATE USER weiser_user WITH PASSWORD 'secure_password';

-- Grant read permissions
GRANT CONNECT ON DATABASE your_database TO weiser_user;
GRANT USAGE ON SCHEMA public TO weiser_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO weiser_user;

-- Grant permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO weiser_user;
```

#### 2. Network Access

Ensure your PostgreSQL server allows connections from Weiser:

```bash
# Test connectivity
psql -h your-host -U weiser_user -d your_database -c "SELECT 1;"
```

#### 3. SSL Configuration (Recommended)

```yaml
datasources:
  - name: postgres_prod
    type: postgresql
    uri: postgresql://user:password@host:5432/database?sslmode=require
```

### Supported Features

| Feature                   | PostgreSQL Support |
| ------------------------- | ------------------ |
| **Row Count Checks**      | ✅ Full Support    |
| **Numeric Checks**        | ✅ Full Support    |
| **Sum/Min/Max Checks**    | ✅ Full Support    |
| **Not Empty Checks**      | ✅ Full Support    |
| **Anomaly Detection**     | ✅ Full Support    |
| **Custom SQL**            | ✅ Full Support    |
| **Time Dimensions**       | ✅ Full Support    |
| **Dimensions/Grouping**   | ✅ Full Support    |
| **Complex Filters**       | ✅ Full Support    |
| **Window Functions**      | ✅ Full Support    |
| **Statistical Functions** | ✅ Full Support    |

### PostgreSQL-Specific Examples

#### Using PostgreSQL Date Functions

```yaml
- name: recent_data_check
  dataset: orders
  type: row_count
  condition: gt
  threshold: 100
  filter: created_at >= CURRENT_DATE - INTERVAL '7 days'
```

### Performance Optimization

#### Indexes

Ensure proper indexes for check performance:

```sql
-- Index for time-based checks
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- Index for filtered checks
CREATE INDEX idx_orders_status ON orders(status);

-- Composite index for grouped checks
CREATE INDEX idx_sales_region_date ON sales(region, sale_date);
```

### Common Issues & Solutions

#### Connection Timeout

```yaml
# Increase timeout for large queries
datasources:
  - name: postgres_prod
    type: postgresql
    uri: postgresql://user:password@host:5432/database?connect_timeout=30
```

#### SSL Certificate Issues

```yaml
# Skip SSL verification (not recommended for production)
datasources:
  - name: postgres_dev
    type: postgresql
    uri: postgresql://user:password@host:5432/database?sslmode=disable
```

#### Large Result Sets

```yaml
# Use LIMIT in checks for large tables
- name: sample_data_check
  dataset: |
    SELECT * FROM large_table 
    ORDER BY created_at DESC 
    LIMIT 100000
  type: row_count
  condition: gt
  threshold: 50000
```

## MySQL

MySQL is a popular open-source relational database that supports all Weiser check types. The MySQL connector uses the PyMySQL driver for reliable connectivity and full feature compatibility.

### Configuration

#### Basic Connection

```yaml
datasources:
  - name: mysql_prod
    type: mysql
    host: localhost
    port: 3306
    db_name: production
    user: weiser_user
    password: secure_password
```

#### Connection URI

```yaml
datasources:
  - name: mysql_warehouse
    type: mysql
    uri: mysql+pymysql://user:password@host:3306/database
```

#### Environment Variables (Recommended)

```yaml
datasources:
  - name: mysql_prod
    type: mysql
    host: {{MYSQL_HOST}}
    port: {{MYSQL_PORT}}
    db_name: {{MYSQL_DB}}
    user: {{MYSQL_USER}}
    password: {{MYSQL_PASSWORD}}
```

### Connection Parameters

| Parameter  | Required | Default | Description                                                |
| ---------- | -------- | ------- | ---------------------------------------------------------- |
| `name`     | Yes      | -       | Unique datasource identifier                               |
| `type`     | Yes      | -       | Must be `mysql`                                            |
| `host`     | Yes\*    | -       | Database server hostname                                   |
| `port`     | No       | 3306    | Database server port                                       |
| `db_name`  | Yes\*    | -       | Database name                                              |
| `user`     | Yes\*    | -       | Database username                                          |
| `password` | No       | -       | Database password                                          |
| `uri`      | Yes\*    | -       | Complete connection URI (alternative to individual params) |

\*Either individual parameters OR uri is required

### Setup Requirements

#### 1. Database User Permissions

```sql
-- Create dedicated user for Weiser
CREATE USER 'weiser_user'@'%' IDENTIFIED BY 'secure_password';

-- Grant read permissions
GRANT SELECT ON production.* TO 'weiser_user'@'%';

-- Grant connection permissions
GRANT USAGE ON *.* TO 'weiser_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;
```

#### 2. Network Access

Ensure your MySQL server allows connections from Weiser:

```bash
# Test connectivity
mysql -h your-host -u weiser_user -p production -e "SELECT 1;"
```

#### 3. SSL Configuration (Recommended)

```yaml
datasources:
  - name: mysql_prod
    type: mysql
    uri: mysql+pymysql://user:password@host:3306/database?ssl_ca=/path/to/ca.pem&ssl_verify_identity=true
```

### Supported Features

| Feature                   | MySQL Support  |
| ------------------------- | -------------- |
| **Row Count Checks**      | ✅ Full Support |
| **Numeric Checks**        | ✅ Full Support |
| **Sum/Min/Max Checks**    | ✅ Full Support |
| **Not Empty Checks**      | ✅ Full Support |
| **Anomaly Detection**     | ✅ Full Support |
| **Custom SQL**            | ✅ Full Support |
| **Time Dimensions**       | ✅ Full Support |
| **Dimensions/Grouping**   | ✅ Full Support |
| **Complex Filters**       | ✅ Full Support |
| **Window Functions**      | ✅ Full Support |
| **Statistical Functions** | ✅ Full Support |

### MySQL-Specific Examples

#### Using MySQL Date Functions

```yaml
- name: recent_data_check
  dataset: orders
  type: row_count
  condition: gt
  threshold: 100
  filter: created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
```

#### Using MySQL String Functions

```yaml
- name: email_format_check
  dataset: customers
  type: row_count
  condition: eq
  threshold: 0
  filter: email NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
```

#### JSON Data Validation

```yaml
- name: json_data_check
  dataset: user_preferences
  type: row_count
  condition: gt
  threshold: 1000
  filter: JSON_VALID(preferences_json) = 1
```

### Performance Optimization

#### Indexes

Ensure proper indexes for check performance:

```sql
-- Index for time-based checks
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- Index for filtered checks
CREATE INDEX idx_orders_status ON orders(status);

-- Composite index for grouped checks
CREATE INDEX idx_sales_region_date ON sales(region, sale_date);
```

#### Query Optimization

```sql
-- Use appropriate storage engines
ALTER TABLE large_table ENGINE=InnoDB;

-- Enable query cache for repeated checks
SET GLOBAL query_cache_type = ON;
SET GLOBAL query_cache_size = 268435456;  -- 256MB
```

### Common Issues & Solutions

#### Connection Timeout

```yaml
# Increase timeout for large queries
datasources:
  - name: mysql_prod
    type: mysql
    uri: mysql+pymysql://user:password@host:3306/database?connect_timeout=30&read_timeout=60
```

#### SSL Certificate Issues

```yaml
# Disable SSL (not recommended for production)
datasources:
  - name: mysql_dev
    type: mysql
    uri: mysql+pymysql://user:password@host:3306/database?ssl_disabled=true
```

#### Character Set Issues

```yaml
# Specify character set
datasources:
  - name: mysql_prod
    type: mysql
    uri: mysql+pymysql://user:password@host:3306/database?charset=utf8mb4
```

#### Large Result Sets

```yaml
# Use LIMIT in checks for large tables
- name: sample_data_check
  dataset: |
    SELECT * FROM large_table 
    ORDER BY created_at DESC 
    LIMIT 100000
  type: row_count
  condition: gt
  threshold: 50000
```

### MySQL Version Compatibility

Weiser supports MySQL versions:

- **MySQL 5.7+**: Full support for all features
- **MySQL 8.0+**: Enhanced support with JSON functions and window functions
- **MariaDB 10.3+**: Compatible with MySQL features

### Cloud MySQL Services

#### Amazon RDS for MySQL

```yaml
datasources:
  - name: mysql_rds
    type: mysql
    host: myinstance.123456789012.us-east-1.rds.amazonaws.com
    port: 3306
    db_name: production
    user: {{RDS_USERNAME}}
    password: {{RDS_PASSWORD}}
```

#### Google Cloud SQL for MySQL

```yaml
datasources:
  - name: mysql_cloudsql
    type: mysql
    host: 10.1.2.3  # Private IP or public IP
    port: 3306
    db_name: production
    user: {{CLOUDSQL_USERNAME}}
    password: {{CLOUDSQL_PASSWORD}}
```

#### Azure Database for MySQL

```yaml
datasources:
  - name: mysql_azure
    type: mysql
    host: myserver.mysql.database.azure.com
    port: 3306
    db_name: production
    user: {{AZURE_MYSQL_USERNAME}}
    password: {{AZURE_MYSQL_PASSWORD}}
```

## Snowflake

Snowflake is a fully-managed cloud data warehouse that supports all Weiser check types. The Snowflake connector provides native support for Snowflake-specific features like warehouses, roles, and schemas.

### Configuration

#### Basic Connection

```yaml
datasources:
  - name: snowflake_prod
    type: snowflake
    account: your-account-id
    db_name: PRODUCTION_DB
    user: WEISER_USER
    password: secure_password
    warehouse: COMPUTE_WH
    role: ANALYST_ROLE
    schema_name: PUBLIC
```

#### Connection URI

```yaml
datasources:
  - name: snowflake_warehouse
    type: snowflake
    uri: snowflake://user:password@account.snowflakecomputing.com/database?warehouse=WH&role=ROLE&schema=SCHEMA
```

#### Environment Variables (Recommended)

```yaml
datasources:
  - name: snowflake_prod
    type: snowflake
    account: {{SNOWFLAKE_ACCOUNT}}
    db_name: {{SNOWFLAKE_DATABASE}}
    user: {{SNOWFLAKE_USER}}
    password: {{SNOWFLAKE_PASSWORD}}
    warehouse: {{SNOWFLAKE_WAREHOUSE}}
    role: {{SNOWFLAKE_ROLE}}
    schema_name: {{SNOWFLAKE_SCHEMA}}
```

### Connection Parameters

| Parameter   | Required | Default | Description                                                |
| ----------- | -------- | ------- | ---------------------------------------------------------- |
| `name`      | Yes      | -       | Unique datasource identifier                               |
| `type`      | Yes      | -       | Must be `snowflake`                                        |
| `host`      | Yes\*    | -       | Snowflake account URL (e.g., account.snowflakecomputing.com) |
| `db_name`   | Yes\*    | -       | Database name                                              |
| `user`      | Yes\*    | -       | Snowflake username                                         |
| `password`  | Yes\*    | -       | Snowflake password                                         |
| `warehouse` | No       | -       | Warehouse to use for compute                               |
| `role`      | No       | -       | Role to assume for permissions                             |
| `schema_name`    | No       | -       | Default schema to use                                      |
| `uri`       | Yes\*    | -       | Complete connection URI (alternative to individual params) |

\*Either individual parameters OR uri is required

### Setup Requirements

#### 1. Snowflake User and Role Setup

```sql
-- Create role for Weiser
CREATE ROLE weiser_role;

-- Create user for Weiser
CREATE USER weiser_user
    PASSWORD = 'secure_password'
    DEFAULT_ROLE = weiser_role
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_NAMESPACE = 'PRODUCTION_DB.PUBLIC';

-- Grant role to user
GRANT ROLE weiser_role TO USER weiser_user;

-- Grant database and schema permissions
GRANT USAGE ON DATABASE PRODUCTION_DB TO ROLE weiser_role;
GRANT USAGE ON SCHEMA PRODUCTION_DB.PUBLIC TO ROLE weiser_role;
GRANT SELECT ON ALL TABLES IN SCHEMA PRODUCTION_DB.PUBLIC TO ROLE weiser_role;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE weiser_role;

-- Grant permissions for future tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA PRODUCTION_DB.PUBLIC TO ROLE weiser_role;
```

#### 2. Network Access

Snowflake is cloud-hosted and accessible via HTTPS. Ensure your network allows outbound HTTPS connections.

#### 3. Account Identifier

Find your Snowflake account identifier:

```sql
-- Run this in Snowflake to get your account identifier
SELECT CURRENT_ACCOUNT();
```

### Supported Features

| Feature                   | Snowflake Support |
| ------------------------- | ----------------- |
| **Row Count Checks**      | ✅ Full Support   |
| **Numeric Checks**        | ✅ Full Support   |
| **Sum/Min/Max Checks**    | ✅ Full Support   |
| **Not Empty Checks**      | ✅ Full Support   |
| **Anomaly Detection**     | ✅ Full Support   |
| **Custom SQL**            | ✅ Full Support   |
| **Time Dimensions**       | ✅ Full Support   |
| **Dimensions/Grouping**   | ✅ Full Support   |
| **Complex Filters**       | ✅ Full Support   |
| **Window Functions**      | ✅ Full Support   |
| **Statistical Functions** | ✅ Full Support   |

### Snowflake-Specific Examples

#### Using Snowflake Date Functions

```yaml
- name: recent_data_check
  dataset: orders
  type: row_count
  condition: gt
  threshold: 100
  filter: created_at >= DATEADD(day, -7, CURRENT_DATE())
```

#### Time Travel Queries

```yaml
- name: historical_comparison
  dataset: |
    SELECT COUNT(*) as current_count
    FROM orders
    WHERE created_at >= CURRENT_DATE()
  type: numeric
  measure: current_count
  condition: gt
  threshold: 1000
```

#### Using Snowflake Warehouse Scaling

```yaml
# Use larger warehouse for intensive checks
datasources:
  - name: snowflake_large
    type: snowflake
    host: account.snowflakecomputing.com
    db_name: ANALYTICS_DB
    user: weiser_user
    password: secure_password
    warehouse: LARGE_WH  # Use larger warehouse for heavy workloads
    role: ANALYST_ROLE
```

### Performance Optimization

#### Warehouse Sizing

Choose appropriate warehouse size based on check complexity:

- **XS/S**: Simple row counts and basic aggregations
- **M/L**: Complex joins and window functions
- **XL/2XL**: Large-scale statistical analysis

#### Query Optimization

```sql
-- Use clustering keys for large tables
ALTER TABLE orders CLUSTER BY (created_at);

-- Use materialized views for complex aggregations
CREATE MATERIALIZED VIEW daily_order_metrics AS
SELECT 
    DATE_TRUNC('day', created_at) as order_date,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY 1;
```

### Common Issues & Solutions

#### Authentication Errors

```yaml
# Ensure correct role and warehouse
datasources:
  - name: snowflake_prod
    type: snowflake
    host: account.snowflakecomputing.com
    db_name: PROD_DB
    user: WEISER_USER
    password: secure_password
    warehouse: COMPUTE_WH  # Must have USAGE privilege
    role: ANALYST_ROLE     # Must be granted to user
```

#### Warehouse Suspension

```yaml
# Auto-resume warehouses (default behavior)
# Warehouses automatically resume when queries are executed
# Consider using auto-suspend settings in Snowflake
```

#### Large Result Sets

```yaml
# Use LIMIT for sampling large tables
- name: sample_data_check
  dataset: |
    SELECT * FROM large_table 
    SAMPLE (1000 ROWS)  -- Snowflake sampling
  type: row_count
  condition: eq
  threshold: 1000
```

## Databricks

Databricks is a unified data analytics platform that supports all Weiser check types. The Databricks connector provides native support for both SQL warehouses and compute clusters with Unity Catalog integration.

### Configuration

#### Basic Connection (SQL Warehouse)

```yaml
datasources:
  - name: databricks_prod
    type: databricks
    host: workspace-123.cloud.databricks.com
    access_token: dapi123456789abcdef
    http_path: /sql/1.0/warehouses/abc123def456
    catalog: main
    schema_name: default
```

#### Cluster Connection

```yaml
datasources:
  - name: databricks_cluster
    type: databricks
    host: workspace-456.cloud.databricks.com
    access_token: dapi987654321fedcba
    http_path: /sql/protocolv1/o/123456789/clusters/1234-567890-abc123
    catalog: hive_metastore
    schema_name: public
```

#### Environment Variables (Recommended)

```yaml
datasources:
  - name: databricks_prod
    type: databricks
    host: {{DATABRICKS_HOST}}
    access_token: {{DATABRICKS_ACCESS_TOKEN}}
    http_path: {{DATABRICKS_HTTP_PATH}}
    catalog: {{DATABRICKS_CATALOG}}
    schema_name: {{DATABRICKS_SCHEMA}}
```

### Connection Parameters

| Parameter      | Required | Default | Description                                                |
| -------------- | -------- | ------- | ---------------------------------------------------------- |
| `name`         | Yes      | -       | Unique datasource identifier                               |
| `type`         | Yes      | -       | Must be `databricks`                                       |
| `host`         | Yes\*    | -       | Databricks workspace hostname (e.g., workspace-123.cloud.databricks.com) |
| `access_token` | Yes\*    | -       | Databricks personal access token                           |
| `http_path`    | Yes\*    | -       | HTTP path to SQL warehouse or cluster endpoint            |
| `catalog`      | No       | -       | Unity Catalog name (e.g., main, hive_metastore)          |
| `schema_name`  | No       | -       | Default schema to use                                      |
| `uri`          | Yes\*    | -       | Complete connection URI (alternative to individual params) |

\*Either individual parameters OR uri is required

### Setup Requirements

#### 1. Databricks Access Token

Create a personal access token in your Databricks workspace:

1. Go to User Settings > Developer > Access tokens
2. Generate new token with appropriate permissions
3. Copy the token value (starts with `dapi`)

#### 2. SQL Warehouse or Cluster Setup

**For SQL Warehouses (Recommended):**
```sql
-- Get HTTP path from SQL Warehouses page
-- Format: /sql/1.0/warehouses/{warehouse-id}
```

**For Compute Clusters:**
```sql
-- Get HTTP path from Compute page
-- Format: /sql/protocolv1/o/{org-id}/clusters/{cluster-id}
```

#### 3. Unity Catalog Setup (Optional)

```sql
-- Create catalog for data quality
CREATE CATALOG data_quality;

-- Grant permissions
GRANT USE CATALOG ON data_quality TO `weiser@company.com`;
GRANT USE SCHEMA ON data_quality.default TO `weiser@company.com`;
GRANT SELECT ON data_quality.default.* TO `weiser@company.com`;
```

### Supported Features

| Feature                   | Databricks Support |
| ------------------------- | ------------------- |
| **Row Count Checks**      | ✅ Full Support     |
| **Numeric Checks**        | ✅ Full Support     |
| **Sum/Min/Max Checks**    | ✅ Full Support     |
| **Not Empty Checks**      | ✅ Full Support     |
| **Anomaly Detection**     | ✅ Full Support     |
| **Custom SQL**            | ✅ Full Support     |
| **Time Dimensions**       | ✅ Full Support     |
| **Dimensions/Grouping**   | ✅ Full Support     |
| **Complex Filters**       | ✅ Full Support     |
| **Window Functions**      | ✅ Full Support     |
| **Statistical Functions** | ✅ Full Support     |
| **Unity Catalog**         | ✅ Full Support     |

### Databricks-Specific Examples

#### Using Unity Catalog

```yaml
- name: unity_catalog_check
  dataset: main.sales.orders
  type: row_count
  condition: gt
  threshold: 1000
  filter: order_date >= current_date() - interval 7 days
```

#### Delta Lake Time Travel

```yaml
- name: historical_comparison
  dataset: |
    SELECT COUNT(*) as current_count
    FROM delta.`/mnt/delta/orders`
    VERSION AS OF 123
  type: numeric
  measure: current_count
  condition: gt
  threshold: 5000
```

#### Using Databricks SQL Functions

```yaml
- name: data_freshness_check
  dataset: events
  type: numeric
  measure: datediff(current_timestamp(), max(event_timestamp))
  condition: lt
  threshold: 24
  filter: event_date = current_date()
```

### Performance Optimization

#### Warehouse Sizing

Choose appropriate warehouse size based on check complexity:

- **2X-Small/X-Small**: Simple row counts and basic aggregations
- **Small/Medium**: Complex joins and window functions
- **Large/X-Large**: Large-scale statistical analysis and heavy workloads

#### Query Optimization

```sql
-- Use Delta Lake optimization features
OPTIMIZE delta.`/mnt/delta/orders` ZORDER BY (order_date);

-- Create materialized views for complex aggregations
CREATE MATERIALIZED VIEW daily_order_metrics AS
SELECT 
    date_trunc('day', order_date) as order_date,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY 1;
```

### Common Issues & Solutions

#### Authentication Errors

```yaml
# Ensure access token has proper permissions
datasources:
  - name: databricks_prod
    type: databricks
    host: workspace-123.cloud.databricks.com
    access_token: dapi123456789abcdef  # Must start with 'dapi'
    http_path: /sql/1.0/warehouses/abc123def456
```

#### Warehouse Auto-Suspend

```yaml
# Warehouses auto-suspend when idle
# They automatically resume when queries are executed
# Consider warehouse settings in Databricks UI for cost optimization
```

#### Unity Catalog Access

```yaml
# Specify full three-part names for Unity Catalog tables
- name: unity_catalog_check
  dataset: catalog_name.schema_name.table_name
  type: row_count
  condition: gt
  threshold: 100
```

#### Large Result Sets

```yaml
# Use LIMIT for sampling large tables
- name: sample_data_check
  dataset: |
    SELECT * FROM large_table 
    TABLESAMPLE (1000 ROWS)  -- Databricks sampling
  type: row_count
  condition: eq
  threshold: 1000
```

## BigQuery

BigQuery is Google Cloud's fully-managed data warehouse that supports all Weiser check types. The BigQuery connector provides native support for Google Cloud authentication, regional datasets, and BigQuery-specific SQL features.

### Configuration

#### Basic Connection

```yaml
datasources:
  - name: bigquery_prod
    type: bigquery
    project_id: my-gcp-project
    dataset_id: production_data
    credentials_path: /path/to/service-account.json
```

#### Connection with Location

```yaml
datasources:
  - name: bigquery_eu
    type: bigquery
    project_id: my-gcp-project
    dataset_id: eu_dataset
    location: europe-west1
    credentials_path: /path/to/service-account.json
```

#### Connection URI

```yaml
datasources:
  - name: bigquery_warehouse
    type: bigquery
    uri: bigquery://my-project/my-dataset?credentials_path=/path/to/creds.json&location=us-central1
```

#### Environment Variables (Recommended)

```yaml
datasources:
  - name: bigquery_prod
    type: bigquery
    project_id: {{GCP_PROJECT_ID}}
    dataset_id: {{BIGQUERY_DATASET}}
    credentials_path: {{GOOGLE_APPLICATION_CREDENTIALS}}
    location: {{BIGQUERY_LOCATION}}
```

### Connection Parameters

| Parameter         | Required | Default | Description                                                |
| ----------------- | -------- | ------- | ---------------------------------------------------------- |
| `name`            | Yes      | -       | Unique datasource identifier                               |
| `type`            | Yes      | -       | Must be `bigquery`                                         |
| `project_id`      | Yes\*    | -       | Google Cloud project ID                                    |
| `dataset_id`      | No       | -       | Default dataset ID to use                                  |
| `db_name`         | No       | -       | Alternative to dataset_id (fallback)                      |
| `credentials_path`| No       | -       | Path to service account JSON file                          |
| `location`        | No       | -       | Dataset location (e.g., us-central1, europe-west1)        |
| `uri`             | Yes\*    | -       | Complete connection URI (alternative to individual params) |

\*Either individual parameters OR uri is required

### Setup Requirements

#### 1. Google Cloud Project Setup

1. Create or select a Google Cloud project
2. Enable the BigQuery API
3. Create a service account with BigQuery permissions
4. Download the service account JSON key file

#### 2. Service Account Permissions

```bash
# Grant BigQuery Data Viewer role
gcloud projects add-iam-policy-binding my-gcp-project \
    --member="serviceAccount:weiser@my-gcp-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

# Grant BigQuery Job User role (for running queries)
gcloud projects add-iam-policy-binding my-gcp-project \
    --member="serviceAccount:weiser@my-gcp-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"
```

#### 3. Authentication Methods

**Option A: Service Account Key File**
```yaml
datasources:
  - name: bigquery_prod
    type: bigquery
    project_id: my-gcp-project
    credentials_path: /path/to/service-account.json
```

**Option B: Environment Variable**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

**Option C: Application Default Credentials (GCP environments)**
```yaml
# No credentials_path needed when running on GCP
datasources:
  - name: bigquery_prod
    type: bigquery
    project_id: my-gcp-project
```

### Supported Features

| Feature                   | BigQuery Support |
| ------------------------- | ---------------- |
| **Row Count Checks**      | ✅ Full Support  |
| **Numeric Checks**        | ✅ Full Support  |
| **Sum/Min/Max Checks**    | ✅ Full Support  |
| **Not Empty Checks**      | ✅ Full Support  |
| **Anomaly Detection**     | ✅ Full Support  |
| **Custom SQL**            | ✅ Full Support  |
| **Time Dimensions**       | ✅ Full Support  |
| **Dimensions/Grouping**   | ✅ Full Support  |
| **Complex Filters**       | ✅ Full Support  |
| **Window Functions**      | ✅ Full Support  |
| **Statistical Functions** | ✅ Full Support  |
| **Standard SQL**          | ✅ Full Support  |

### BigQuery-Specific Examples

#### Using BigQuery Date Functions

```yaml
- name: recent_data_check
  dataset: orders
  type: row_count
  condition: gt
  threshold: 100
  filter: created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
```

#### Cross-Dataset Queries

```yaml
- name: cross_dataset_check
  dataset: "`my-project.dataset1.orders` o JOIN `my-project.dataset2.customers` c ON o.customer_id = c.id"
  type: row_count
  condition: gt
  threshold: 1000
```

#### Using BigQuery Array Functions

```yaml
- name: array_data_check
  dataset: events
  type: numeric
  measure: ARRAY_LENGTH(event_tags)
  condition: between
  threshold: [1, 10]
  filter: event_date = CURRENT_DATE()
```

#### Partitioned Table Queries

```yaml
- name: partition_efficiency_check
  dataset: "`my-project.analytics.events`"
  type: row_count
  condition: gt
  threshold: 10000
  filter: _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
```

### Performance Optimization

#### Query Optimization

```sql
-- Use clustering and partitioning for large tables
CREATE TABLE `my-project.analytics.events`
(
  event_timestamp TIMESTAMP,
  user_id STRING,
  event_name STRING,
  event_data JSON
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, event_name;
```

#### Cost Management

```yaml
# Use specific datasets to limit query scope
- name: cost_efficient_check
  dataset: "`my-project.production.orders`"  # Specific table reference
  type: row_count
  condition: gt
  threshold: 1000
  filter: DATE(created_at) = CURRENT_DATE()  # Partition pruning
```

#### Regional Optimization

```yaml
# Use location parameter for regional datasets
datasources:
  - name: bigquery_eu
    type: bigquery
    project_id: my-gcp-project
    dataset_id: eu_dataset
    location: europe-west1  # Reduces latency for EU data
```

### Common Issues & Solutions

#### Authentication Errors

```yaml
# Ensure service account has proper permissions
datasources:
  - name: bigquery_prod
    type: bigquery
    project_id: my-gcp-project
    credentials_path: /path/to/service-account.json  # Must have BigQuery permissions
```

#### Dataset Access Issues

```yaml
# Use fully qualified table names for cross-project access
- name: cross_project_check
  dataset: "`other-project.public_data.table_name`"
  type: row_count
  condition: gt
  threshold: 100
```

#### Query Quotas and Limits

```yaml
# Use LIMIT for large table sampling
- name: sample_data_check
  dataset: |
    SELECT * FROM `my-project.large_dataset.big_table`
    TABLESAMPLE SYSTEM (1 PERCENT)  -- BigQuery sampling
  type: row_count
  condition: gt
  threshold: 1000
```

#### Slot Availability

```yaml
# Consider query priority and complexity
# BigQuery automatically manages slot allocation
# For consistent performance, consider reservations for production workloads
```

### Cost Considerations

#### Query Costs

- BigQuery charges based on data processed
- Use partition pruning and clustering for cost efficiency
- Consider query caching for repeated checks

#### Best Practices

```yaml
# Efficient date filtering
- name: cost_efficient_check
  dataset: orders
  type: row_count
  condition: gt
  threshold: 100
  filter: DATE(created_at) = CURRENT_DATE()  # Uses partition pruning

# Avoid SELECT * on large tables
- name: specific_column_check
  dataset: |
    SELECT order_id, status FROM orders
    WHERE DATE(created_at) = CURRENT_DATE()
  type: row_count
  condition: gt
  threshold: 50
```

## Cube

Cube is a semantic layer that allows you to define business metrics and dimensions. Weiser integrates with Cube to run data quality checks on these metrics.
Cube implements the PostgreSQL interface, so you can use the same configuration as PostgreSQL. Visit the [Cube documentation](https://cube.dev/docs) for more details on setting up Cube.

## Contributing

Help us expand datasource support! We welcome contributions for:

- New database connectors
- Performance optimizations
- Documentation improvements
- Testing and validation

See our [GitHub repository](https://github.com/weiser-ai/weiser) for contribution guidelines.

## Getting Help

- 📚 [Configuration Reference](./configuration.md)
- 🔧 [Getting Started Guide](./tutorial/getting-started.md)
- 💬 [GitHub Issues](https://github.com/weiser-ai/weiser/issues)
- 📖 [Check Types Documentation](./check-types/index.md)
