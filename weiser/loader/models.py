from datetime import datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Optional, Union, List

from pydantic import BaseModel


class Version(IntEnum):
    v1 = 1

class CheckType(str, Enum):
    numeric = 'numeric'
    row_count = 'row_count'

class DBType(str, Enum):
    postgresql = 'postgresql'
    mysql = 'mysql'

class Condition(str, Enum):
    gt = 'gt'
    ge = 'ge'
    lt = 'le'
    le = 'le'
    between = 'between'


class Check(BaseModel):
    name: str
    datasource: Optional[Union[str, List[str]]] = 'default'
    type: Optional[CheckType] = CheckType.numeric
    dataset: Union[str, List[str]]
    
    sql: Optional[str] = None
    condition: Optional[Condition] = None
    fail: Optional[bool] = False
    threshold: Optional[Union[Union[int, float, Decimal], List[Union[int, float, Decimal]]]] = None

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
    password: Optional[str] = None
    port: Optional[int] = None

    class Config:  
        use_enum_values = True


class BaseConfig(BaseModel):
    version: Optional[Version] = Version.v1
    checks: List[Check]
    datasources: List[Datasource]
    includes: Optional[List[str]] = None
