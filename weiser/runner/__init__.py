import json
import uuid

from datetime import datetime
from pprint import pprint

from weiser.checks import CheckFactory
from weiser.loader.models import BaseConfig, ConnectionType
from weiser.drivers import DriverFactory
from weiser.drivers.metric_stores import MetricStoreFactory, MetricStoreDB


def run_checks(run_id:str, config: BaseConfig, connections: dict, metric_store: MetricStoreDB, verbose=False):
    results = []
    for check in config.checks:
        if isinstance(check.datasource, str):
            check.datasource = [check.datasource]
        for datasource in check.datasource:
            if datasource not in connections:
                raise Exception(f'Check <{check.name}>: Datasource {datasource} is not configured. ')
            driver = connections[datasource]
            check_instance = CheckFactory.create_check(run_id, check, driver, datasource, metric_store)
            results.append({
                            'check_instance': check_instance, 
                            'results': check_instance.run(verbose),
                            'run_id': run_id
                           })
    return results

def pre_run_config(config: dict, verbose=False) -> dict:
    base_config = BaseConfig(**config)
    metric_store = None
    for config_conn in base_config.connections:
        if config_conn.type == ConnectionType.metricstore:
            metric_store = config_conn
            break
    context =  {
        'config': base_config,
        'connections': {},
        'metric_store': MetricStoreFactory.create_driver(metric_store),
        'run_id': uuid.uuid4(),
        'run_ts': datetime.now()
    }
    if verbose:
        pprint(json.loads(base_config.model_dump_json()))
    for connection in base_config.datasources:
        context['connections'][connection.name] = DriverFactory.create_driver(connection)
        engine = context['connections'][connection.name].engine
        with engine.connect() as conn:
            conn.execute('SELECT 1')
            if verbose:
                pprint(f'Connected to {connection.name}')
    return context