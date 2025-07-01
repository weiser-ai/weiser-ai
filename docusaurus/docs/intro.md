# Introduction

Weiser is a comprehensive data quality framework that empowers teams to build reliable, trustworthy data systems through automated validation and monitoring. Whether you're managing a small analytics pipeline or enterprise-scale data infrastructure, Weiser provides the tools you need to ensure data integrity at every step.

## What Weiser Does

### 🔍 **Data Quality Validation**
- **Row Count Checks**: Ensure tables have expected data volumes
- **Numeric Validation**: Validate sums, averages, min/max values, and custom calculations
- **Data Completeness**: Monitor NULL values and missing data with count or percentage thresholds
- **Business Logic**: Validate complex business rules and KPIs
- **Range Validation**: Check data falls within expected bounds

### 📊 **Statistical Anomaly Detection**
- **Automated Anomaly Detection**: Uses Median Absolute Deviation (MAD) for robust outlier detection
- **Historical Analysis**: Compares current metrics against historical patterns
- **Configurable Sensitivity**: Adjust detection thresholds based on your data's characteristics
- **Time Series Monitoring**: Track metrics over time to identify unusual patterns

### 🏗️ **Multi-Platform Support**
- **Database Compatibility**: PostgreSQL, MySQL, and more
- **Semantic Layer Integration**: Native Cube.js support for business metrics
- **Flexible Datasets**: Work with tables, views, or custom SQL queries
- **Cloud Ready**: DuckDB local storage or PostgreSQL/S3 for distributed teams

### ⚡ **Developer-Friendly Experience**
- **YAML Configuration**: Simple, version-controllable configuration files
- **CLI Interface**: Easy command-line operation for CI/CD integration
- **Modular Design**: Split configurations across multiple files for large projects
- **Environment Variables**: Secure handling of sensitive connection data

### 📈 **Monitoring & Alerting**
- **Historical Metrics Storage**: Track check results over time
- **Slack Integration**: Real-time notifications for check failures
- **Dashboard Visualization**: Streamlit-based UI for exploring results
- **Trend Analysis**: Monitor data quality trends and improvements

### 🎯 **Use Cases**

**Data Engineering Teams**
- Validate ETL pipeline outputs
- Monitor data freshness and completeness
- Catch data quality regressions early
- Ensure SLA compliance

**Analytics Teams**
- Validate business metrics and KPIs
- Monitor dashboard data quality
- Detect anomalies in key business metrics
- Ensure report accuracy

**Data Science Teams**
- Validate model input data quality
- Monitor feature engineering pipelines
- Detect data drift and model degradation
- Ensure training data consistency

**DevOps & Platform Teams**
- Integrate data quality into CI/CD pipelines
- Monitor production data systems
- Set up automated alerting for data issues
- Track data quality SLAs

## Key Features

### **Comprehensive Check Types**
- **Basic Validation**: Row counts, sums, min/max values
- **Advanced Logic**: Custom SQL expressions and calculations
- **Data Completeness**: NULL value monitoring with flexible thresholds
- **Semantic Layer**: Cube.js measure validation
- **Anomaly Detection**: Statistical outlier detection

### **Flexible Configuration**
- **Multiple Datasources**: Connect to different databases simultaneously
- **Dimensional Analysis**: Group checks by business dimensions
- **Time-Based Aggregation**: Daily, monthly, yearly rollups
- **Custom Filters**: Apply business logic and date ranges
- **Templating**: Reusable configuration patterns

### **Enterprise Ready**
- **Scalable Architecture**: Handle large datasets efficiently
- **Security**: Environment variable support for credentials
- **Monitoring**: Built-in metrics storage and trend analysis
- **Integration**: CLI tools for automation and CI/CD
- **Extensibility**: Plugin architecture for custom checks

## Getting Started

Ready to improve your data quality? Start with our [Getting Started Guide](./tutorial/getting-started.md) to set up your first data quality checks in minutes.

Explore specific check types in our [Check Types Documentation](./check-types/index.md) or dive into the complete [Configuration Reference](./configuration.md) for advanced setups.



