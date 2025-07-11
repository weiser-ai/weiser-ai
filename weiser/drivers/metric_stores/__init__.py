from typing import Union
from weiser.drivers.metric_stores.duckdb import DuckDBMetricStore
from weiser.drivers.metric_stores.postgres import PostgresMetricStore
from weiser.loader.models import MetricStore, MetricStoreType

QUERY_TYPE_MAP = {
    MetricStoreType.duckdb: DuckDBMetricStore,
    MetricStoreType.postgresql: PostgresMetricStore,
}

MetricStoreDB = Union[DuckDBMetricStore, PostgresMetricStore]
# MetricStoreDB = DuckDBMetricStore


class MetricStoreFactory:
    @staticmethod
    def create_driver(metric_store: MetricStore, verbose: bool = False) -> MetricStoreDB:
        if metric_store:
            return QUERY_TYPE_MAP.get(
                metric_store.db_type,
                DuckDBMetricStore,
            )(metric_store, verbose=verbose)
        return DuckDBMetricStore(MetricStore(verbose=verbose))
