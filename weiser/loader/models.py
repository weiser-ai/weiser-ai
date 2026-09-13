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
    cross_source_row_count = "cross_source_row_count"
    cross_source_fill_rate = "cross_source_fill_rate"


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

    # Used by cross_source_* checks to compare `dataset` against a table (or list of
    # tables, zipped pairwise with `dataset`) living on a second datasource.
    compare_datasource: Optional[str] = None
    compare_dataset: Optional[Union[str, List[str]]] = None

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
    tenant_id: Optional[int] = 1
    s3_access_key: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_region: Optional[str] = "us-east-1"
    s3_url_style: Optional[str] = S3UrlStyle.vhost

    class Config:
        use_enum_values = True


class AgentFramework(str, Enum):
    pydantic_ai = "pydantic_ai"
    custom = "custom"
    # strands = "strands"  # planned, not implemented yet


class SemanticLayerType(str, Enum):
    cube = "cube"
    generic_sql = "generic_sql"
    # snowflake_semantic_view = "snowflake_semantic_view"  # planned, not implemented yet


class AgentVariant(BaseModel):
    """A declaratively-configured agent shape ("arm"). `entrypoint` is a dotted path to
    a factory function written once per agent family; every other field is a knob the
    harness passes into that factory so that variants of the same agent are pure config.

    `adapter_class` is only used when `framework == AgentFramework.custom`: a dotted
    path to a class implementing `weiser.evals.adapters.base.AgentAdapter`, for agent
    pipelines that don't fit PydanticAI's single-`Agent`/standardized-toolset shape
    (e.g. a multi-agent router with its own tools). `entrypoint` keeps its usual meaning
    for that adapter's own `build()` to resolve however it needs to.
    """

    name: str
    framework: AgentFramework
    entrypoint: str
    adapter_class: Optional[str] = None
    model: Optional[str] = None
    system_prompt: Optional[str] = None
    tools: Optional[List[str]] = None
    model_settings: Optional[dict] = None
    max_turns: int = 40
    extra: Optional[dict] = None


class SemanticLayerConfig(BaseModel):
    name: str
    type: SemanticLayerType
    datasource: str
    meta_api_url: Optional[str] = None
    meta_api_token: Optional[SecretStr] = None

    class Config:
        use_enum_values = True


class ReferenceValue(BaseModel):
    metric: str
    expected: float
    tolerance_pct: float = 0.0
    widget_index: Optional[int] = None


class EvalGolden(BaseModel):
    """A single test case / "seed" (agentic-sql-mini's terminology). Declared inline in
    an EvalSuite or loaded from an external YAML/JSONL golden file."""

    id: str
    input: str
    split: Literal["train", "held_out"] = "train"
    level: Literal["easy", "hard"] = "easy"
    source: Literal["hand_written", "synthetic", "production"] = "hand_written"
    reference_values: Optional[List[ReferenceValue]] = None
    reference_answer_text: Optional[str] = None
    reference_source: Optional[
        Literal["human_verified", "independent_query", "unverified"]
    ] = None
    expected_views: Optional[List[str]] = None
    extra: Optional[dict] = None


class MetricConfig(BaseModel):
    """Declarative metric selection/configuration, resolved via MetricFactory."""

    type: str
    name: Optional[str] = None
    threshold: float = 0.5
    params: Optional[dict] = None


class EvalArm(BaseModel):
    """A named pairing of an agent variant with a semantic layer — one point in a
    suite's comparison."""

    name: str
    agent_variant: str
    semantic_layer: str


class EvalSuite(BaseModel):
    name: str
    arms: List[EvalArm]
    golden_set: Optional[str] = None
    goldens: Optional[List[EvalGolden]] = None
    metrics: List[MetricConfig]
    dq_scope: Optional[List[str]] = None


class BaseConfig(BaseModel):
    version: Optional[Version] = Version.v1
    checks: List[Check] = []
    datasources: List[AnyDatasource] = []
    includes: Optional[List[str]] = None
    connections: Optional[List[MetricStore]] = [MetricStore()]
    slack_url: Optional[str] = None
    agent_variants: Optional[List[AgentVariant]] = None
    semantic_layers: Optional[List[SemanticLayerConfig]] = None
    eval_suites: Optional[List[EvalSuite]] = None
