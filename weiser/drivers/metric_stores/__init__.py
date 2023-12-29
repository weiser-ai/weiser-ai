from typing import Union
from weiser.drivers.metric_stores.duckdb import DuckDBMetricStore
from weiser.loader.models import MetricStore

QUERY_TYPE_MAP = {
    'duckdb': DuckDBMetricStore,
}

#MetricStoreDB = Union[DuckDBMetricStore, ...]
MetricStoreDB = DuckDBMetricStore

class MetricStoreFactory():
    @staticmethod
    def create_driver(metric_store: MetricStore) -> MetricStoreDB:
        # TODO: create custom drivers for different DBs, duckdb is the only MetricStore implemented so far.
        return DuckDBMetricStore(metric_store)