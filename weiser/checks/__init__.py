import hashlib

from datetime import datetime
from typing import Any, List
from pprint import pprint
from pypika import Table, Query, functions as fn
from pypika.terms import LiteralValue

from weiser.loader.models import Check, CheckType, Condition
from weiser.drivers import BaseDriver
from weiser.drivers.metric_stores import MetricStoreDB

def apply_condition(value, threshold, condition):
    if condition == Condition.ge:
        return value >= threshold
    if condition == Condition.gt:
        return value > threshold
    if condition == Condition.le:
        return value <= threshold
    if condition == Condition.lt:
        return value < threshold
    if condition == Condition.between:
        return value >= threshold[0] and value <= threshold[1]
    raise Exception(f'Condition not implemented yet {condition}')


class BaseCheck():
    def __init__(self, run_id: str, check: Check, driver: BaseDriver, 
                 datasource: str, metric_store: MetricStoreDB) -> None:
        self.run_id = run_id
        self.check = check
        self.driver = driver
        self.datasource = datasource
        self.metric_store = metric_store
        pass

    def generate_check_id(self, dataset):
        encode = lambda s: str(s).encode('utf-8')
        m = hashlib.sha256()
        # Each check is run once for each datasource defined
        m.update(encode(self.datasource))
        # Each check is run for each dataset defined
        m.update(encode(dataset))
        # Each check name is unique across runs
        m.update(encode(self.check.name))
        return m.hexdigest()

    def execute_query(self, q, verbose):
        return self.driver.execute_query(q, self.check, verbose)

    def append_result(self, success:bool, value:Any, results: List[dict], dataset: str, verbose: bool=False):
        result = self.check.model_dump()
        result.update({
            'check_id': self.generate_check_id(dataset),
            'datasource': self.datasource,
            'dataset': dataset,
            'actual_value': value,
            'success': success,
            'fail': not success,
            'run_id': self.run_id,
            'run_time': datetime.now().isoformat()
        })
        if verbose:
            pprint(result)
        self.metric_store.insert_results(result)
        results.append(result)
        return results

    def run(self, verbose: bool):
        datasets = self.check.dataset
        results = []
        if isinstance(datasets, str):
            datasets = [datasets]
        for dataset in datasets:
            table = Table(dataset)
            q = self.get_query(table, verbose)
            value = self.execute_query(q, verbose)
            success = apply_condition(value, self.check.threshold, self.check.condition) 
            self.append_result(success, value, results, dataset, verbose)

        return results

    def get_query(self, table: Table, verbose: bool):
        if verbose:
            pprint('Called BaseCheck')
        raise Exception('Get Query Method Not Implemented Yet')

class CheckNumeric(BaseCheck):
    def get_query(self, table: Table, verbose: bool):
        query: Query = self.driver.query
        q = query.from_(table).select(LiteralValue(self.check.sql)).limit(1)
        if verbose:
            pprint(q)
        return q

class CheckRowCount(BaseCheck):
    def get_query(self, table: Table, verbose: bool):
        query: Query = self.driver.query
        q = query.from_(table).select(fn.Count('*')).limit(1)
        if verbose:
            pprint(q)
        return q
            


CHECK_TYPE_MAP = {
    CheckType.numeric: CheckNumeric,
    CheckType.row_count: CheckRowCount,
}

class CheckFactory():
    @staticmethod
    def create_check(run_id: str, check: Check, driver: BaseDriver, datasource: str, metric_store: MetricStoreDB):
        check_class = CHECK_TYPE_MAP.get(check.type, None)
        if not check_class:
            raise Exception(f'Check Type {check.type} not implemented yet')
        return check_class(run_id, check, driver, datasource, metric_store)