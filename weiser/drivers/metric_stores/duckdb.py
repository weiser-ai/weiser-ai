from typing import List, Tuple
import duckdb
from pypika import Table, Query
from weiser.loader.models import MetricStore

class DuckDBMetricStore():
    def __init__(self, config: MetricStore) -> None:
        self.db_name = config.db_name
        with duckdb.connect(self.db_name) as conn:
            conn.sql("""CREATE TABLE IF NOT EXISTS metrics (
                     actual_value DOUBLE,
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
                     threshold_list DOUBLE[],
                     type VARCHAR
                     )""")
            
    def insert_results(self, record):
        with duckdb.connect(self.db_name) as conn:
            if isinstance(record['threshold'], List) or isinstance(record['threshold'], Tuple):
                record['threshold_list'] = record['threshold']
                record['threshold'] = None
            elif 'threshold_list' not in record:
                record['threshold_list'] = None
            metrics = Table('metrics')
            q = Query.into(metrics).insert(
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
            conn.sql(str(q))
