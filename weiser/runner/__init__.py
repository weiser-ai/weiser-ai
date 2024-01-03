import json
import random
import uuid

from datetime import datetime, timedelta
from pprint import pprint

from weiser.checks import CheckFactory, apply_condition
from weiser.loader.models import BaseConfig, ConnectionType, Condition
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


def generate_sample_data(check_name: str, config: BaseConfig, connections: dict, metric_store: MetricStoreDB, verbose=False):
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
    results = []
    for check in config.checks:
        if check_name == check.name:
            if isinstance(check.datasource, str):
                check.datasource = [check.datasource]
            for i in range((end_date - start_date).days + 1):
                dt = start_date + timedelta(days=i)
                run_id = str(uuid.uuid4())

                for datasource in check.datasource:
                    if datasource not in connections:
                        raise Exception(f'Check <{check.name}>: Datasource {datasource} is not configured. ')
                    driver = connections[datasource]

                    check_instance = CheckFactory.create_check(run_id, check, driver, datasource, metric_store)
                    datasets = check_instance.check.dataset
                    results = []
                    if isinstance(datasets, str):
                        datasets = [datasets]
                    for dataset in datasets:
                        if check_instance.check.condition == Condition.between:
                            delta = int((check_instance.check.threshold[1] - check_instance.check.threshold[0]) / 2)
                            value =  random.randint(check_instance.check.threshold[0] - delta, check_instance.check.threshold[1] + delta)
                        else:
                            delta = int(check_instance.check.threshold / 2)
                            value =  random.randint(check_instance.check.threshold - delta, check_instance.check.threshold + delta)
                        success = apply_condition(value, check_instance.check.threshold, check_instance.check.condition) 
                        check_instance.append_result(success, value, results, dataset, dt, verbose)

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
        'run_id': str(uuid.uuid4()),
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