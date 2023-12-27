import json
from collections.abc import Iterable

from pprint import pprint

from weiser.checks import CheckFactory
from weiser.loader.models import BaseConfig
from weiser.drivers import DriverFactory


def run_checks(config: BaseConfig, connections: dict, verbose=False):
    results = []
    for check in config.checks:
        if isinstance(check.datasource, str):
            check.datasource = [check.datasource]
        for datasource in check.datasource:
            if datasource not in connections:
                raise Exception(f'Check <{check.name}>: Datasource {datasource} is not configured. ')
            driver = connections[datasource]
            check_instance = CheckFactory.create_check(check, driver)
            results.append({
                            'check_instance': check_instance, 
                            'results': check_instance.run(verbose)
                           })
    return results

def pre_run_config(config: dict, verbose=False) -> dict:
    base_config = BaseConfig(**config)
    context =  {
        'config': base_config,
        'connections': {}
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