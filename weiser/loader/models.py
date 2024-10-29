from datetime import datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Optional, Union, List

from pydantic import BaseModel, SecretStr


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


class DBType(str, Enum):
    postgresql = "postgresql"
    mysql = "mysql"
    cube = "cube"


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
    ] = None
    dimensions: List[str] = []
    filter: List[str] = []
    time_dimension: Optional[TimeDimension] = None
    filter: Optional[str] = None
    # Used for metadata checks
    check_id: str = None

    class Config:
        use_enum_values = True


class Datasource(BaseModel):
    name: str
    uri: Optional[str] = None
    # Type must be provided even if URI is used.
    type: Optional[DBType] = DBType.postgresql
    user: Optional[str] = None
    host: Optional[str] = None
    db_name: Optional[str] = None
    password: Optional[SecretStr] = None
    port: Optional[int] = None

    class Config:
        use_enum_values = True


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
    datasources: List[Datasource]
    includes: Optional[List[str]] = None
    connections: Optional[List[MetricStore]] = [MetricStore()]
