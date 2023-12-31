from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pypika import PostgreSQLQuery
from pypika import Table
from typing import List, Tuple

from weiser.loader.models import MetricStore


class PostgresMetricStore():
    def __init__(self, metric_store: MetricStore) -> None:
        if not metric_store.uri:
            uri = URL.create(
                metric_store.db_type,
                username=metric_store.user,
                password=metric_store.password,
                host=metric_store.host,
                database=metric_store.db_name,
            )
        else:
            uri = metric_store.uri
        
        self.engine = create_engine(uri)

        with self.engine.connect() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS metrics (
                            actual_value double precision,
                            check_id VARCHAR,
                            condition VARCHAR,
                            dataset VARCHAR,
                            datasource VARCHAR,
                            fail BOOLEAN,
                            name VARCHAR,
                            run_id VARCHAR,
                            run_time TIMESTAMP,
                            sql VARCHAR,
                            success boolean,
                            threshold VARCHAR,
                            threshold_list double precision[],
                            type VARCHAR
                            )""")
        
    def insert_results(self, record):
        with self.engine.connect() as conn:
            if isinstance(record['threshold'], List) or isinstance(record['threshold'], Tuple):
                record['threshold_list'] = record['threshold']
                record['threshold'] = None
            elif 'threshold_list' not in record:
                record['threshold_list'] = None
            metrics = Table('metrics')
            q = PostgreSQLQuery.into(metrics).insert(
                record['actual_value'],
                record['check_id'],
                record['condition'],
                record['dataset'],
                record['datasource'],
                record['fail'],
                record['name'],
                record['run_id'],
                record['run_time'],
                record['sql'],
                record['success'],
                record['threshold'],
                record['threshold_list'],
                record['type'],
                )
            conn.execute(str(q))