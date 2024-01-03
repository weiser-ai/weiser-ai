import hashlib

from datetime import datetime
from typing import Any, List, Union
from pprint import pprint
from sqlglot.expressions import Select

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

    def generate_check_id(self, check_name, dataset):
        encode = lambda s: str(s).encode('utf-8')
        m = hashlib.sha256()
        # Each check is run once for each datasource defined
        m.update(encode(self.datasource))
        # Each check is run for each dataset defined
        m.update(encode(dataset))
        # Each check name is unique across runs
        m.update(encode(check_name))
        return m.hexdigest()

    def execute_query(self, q: Select, verbose: bool=False):
        return self.driver.execute_query(q, self.check, verbose)

    def append_result(self, success:bool, value:Any, results: List[dict], dataset: str, run_time: datetime, verbose: bool=False):
        result = self.check.model_dump()
        if self.check.group_by:
            result['name'] = '_'.join((result['name'], 
                                      '_'.join(map(lambda pair: '_'.join(pair), zip(self.check.group_by, map(str, value[:-1]))))))
        result.update({
            'check_id': self.generate_check_id(dataset, result['name']),
            'datasource': self.datasource,
            'dataset': dataset,
            'actual_value': value[-1] if self.check.group_by else value,
            'success': success,
            'fail': not success,
            'run_id': self.run_id,
            'run_time': run_time.isoformat()
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
            q = self.get_query(dataset, verbose)
            rows = self.execute_query(q, verbose)
            if self.check.group_by:
                for row in rows:
                    success = apply_condition(row[-1], self.check.threshold, self.check.condition) 
                    self.append_result(success, row, results, dataset, datetime.now(), verbose)
            else:
                success = apply_condition(rows[0][0], self.check.threshold, self.check.condition)
                self.append_result(success, rows[0][0], results, dataset, datetime.now(), verbose)

        return results

    def get_query(self, table: str, verbose: bool):
        if verbose:
            pprint('Called BaseCheck')
        raise Exception('Get Query Method Not Implemented Yet')
    
    def build_query(self, select_stmnt: List[Any], table:str, limit: int=1, verbose: bool=False):
        if self.check.group_by:
            q = Select().from_(table).select(*(self.check.group_by + select_stmnt)).group_by(*self.check.group_by)
        else:
            q = Select().from_(table).select(*select_stmnt).limit(1)
        if verbose:
            pprint(q.sql(pretty=True))
        return q

class CheckNumeric(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query([self.check.sql,], table, verbose=verbose)

class CheckRowCount(BaseCheck):
    def get_query(self, table: str, verbose: bool):
        return self.build_query(['COUNT(*)',], table, verbose=verbose)
            


CHECK_TYPE_MAP = {
    CheckType.numeric: CheckNumeric,
    CheckType.row_count: CheckRowCount,
}

CHECK_TYPES = Union[BaseCheck, CheckNumeric, CheckRowCount]

class CheckFactory():
    @staticmethod
    def create_check(run_id: str, check: Check, driver: BaseDriver, datasource: str, metric_store: MetricStoreDB) -> CHECK_TYPES:
        check_class = CHECK_TYPE_MAP.get(check.type, None)
        if not check_class:
            raise Exception(f'Check Type {check.type} not implemented yet')
        return check_class(run_id, check, driver, datasource, metric_store)