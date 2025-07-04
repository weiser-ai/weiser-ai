from decimal import Decimal
from enum import Enum, IntEnum
from typing import Optional, Union, List, Annotated, Literal

from pydantic import BaseModel, SecretStr, Field


class Version(IntEnum):
    v1 = 1


class CheckType(str, Enum):
    measure = "measure"
    numeric = "numeric"
    row_count = "row_count"
    anomaly = "anomaly"
    sum = "sum"
    min = "min"
    max = "max"
    not_empty = "not_empty"
    not_empty_pct = "not_empty_pct"


class DBType(str, Enum):
    postgresql = "postgresql"
    mysql = "mysql"
    cube = "cube"
    snowflake = "snowflake"
    databricks = "databricks"
    bigquery = "bigquery"


class MetricStoreType(str, Enum):
    # Duckdb local db is not compatible with cube
    duckdb = "duckdb"
    postgresql = "postgresql"


class ConnectionType(str, Enum):
    metricstore = "metricstore"


class Condition(str, Enum):
    gt = "gt"
    ge = "ge"
    lt = "lt"
    le = "le"
    eq = "eq"
    neq = "neq"
    between = "between"


class Granularity(str, Enum):
    millennium = "millennium"
    century = "century"
    decade = "decade"
    year = "year"
    quarter = "quarter"
    month = "month"
    week = "week"
    day = "day"
    hour = "hour"
    minute = "minute"
    second = "second"
    milliseconds = "milliseconds"
    microseconds = "microseconds"


class S3UrlStyle(str, Enum):
    vhost = "vhost"
    path = "path"


class TimeDimension(BaseModel):
    name: str
    granularity: Optional[Granularity] = Granularity.day


class Check(BaseModel):
    name: str
    datasource: Optional[Union[str, List[str]]] = "default"
    type: Optional[CheckType] = CheckType.numeric
    dataset: Union[str, List[str]]

    description: Optional[str] = None
    measure: Optional[str] = None
    condition: Optional[Condition] = None
    fail: Optional[bool] = False
    threshold: Optional[
        Union[Union[int, float, Decimal], List[Union[int, float, Decimal]]]
    ] = 0
    dimensions: List[str] = []
    filter: List[str] = []
    time_dimension: Optional[TimeDimension] = None
    filter: Optional[str] = None
    # Used for metadata checks
    check_id: Optional[str] = None

    class Config:
        use_enum_values = True


# Base datasource model with common fields
class Datasource(BaseModel):
    name: str
    uri: Optional[str] = None
    # Type must be provided even if URI is used.
    type: Optional[DBType] = DBType.postgresql

    class Config:
        use_enum_values = True


# Database-specific datasource models
class PostgreSQLDatasource(Datasource):
    type: Literal["postgresql"] = "postgresql"
    user: Optional[str] = None
    host: Optional[str] = None
    db_name: Optional[str] = None
    password: Optional[SecretStr] = None
    port: Optional[int] = 5432


class MySQLDatasource(Datasource):
    type: Literal["mysql"] = "mysql"
    user: Optional[str] = None
    host: Optional[str] = None
    db_name: Optional[str] = None
    password: Optional[SecretStr] = None
    port: Optional[int] = 3306


class CubeDatasource(Datasource):
    type: Literal["cube"] = "cube"
    user: Optional[str] = None
    host: Optional[str] = None
    db_name: Optional[str] = None
    password: Optional[SecretStr] = None
    port: Optional[int] = 5432


class SnowflakeDatasource(Datasource):
    type: Literal["snowflake"] = "snowflake"
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    account: str  # Required for Snowflake
    warehouse: Optional[str] = None
    role: Optional[str] = None
    db_name: Optional[str] = None
    schema_name: Optional[str] = None


class DatabricksDatasource(Datasource):
    type: Literal["databricks"] = "databricks"
    host: str  # Required for Databricks
    http_path: str  # Required for Databricks
    access_token: SecretStr  # Required for Databricks
    catalog: Optional[str] = None
    schema_name: Optional[str] = None


class BigQueryDatasource(Datasource):
    type: Literal["bigquery"] = "bigquery"
    project_id: str  # Required for BigQuery
    dataset_id: Optional[str] = None
    db_name: Optional[str] = None  # Alternative to dataset_id
    credentials_path: Optional[str] = None
    location: Optional[str] = "US"


# Discriminated union type for all datasource types
AnyDatasource = Annotated[
    Union[
        PostgreSQLDatasource,
        MySQLDatasource,
        CubeDatasource,
        SnowflakeDatasource,
        DatabricksDatasource,
        BigQueryDatasource,
    ],
    Field(discriminator="type"),
]


class MetricStore(BaseModel):
    name: Optional[str] = None
    uri: Optional[str] = None
    type: Optional[ConnectionType] = ConnectionType.metricstore
    db_type: Optional[MetricStoreType] = MetricStoreType.duckdb
    db_name: Optional[str] = None
    user: Optional[str] = None
    host: Optional[str] = None
    password: Optional[SecretStr] = None
    port: Optional[int] = None
    s3_access_key: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_region: Optional[str] = "us-east-1"
    s3_url_style: Optional[str] = S3UrlStyle.vhost

    class Config:
        use_enum_values = True


class BaseConfig(BaseModel):
    version: Optional[Version] = Version.v1
    checks: List[Check]
    datasources: List[AnyDatasource]
    includes: Optional[List[str]] = None
    connections: Optional[List[MetricStore]] = [MetricStore()]
    slack_url: Optional[str] = None
