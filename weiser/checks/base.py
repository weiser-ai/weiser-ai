import hashlib

from datetime import datetime
from typing import Any, List
from pprint import pprint
from sqlglot.expressions import Select

from weiser.loader.models import Check, Condition
from weiser.drivers import BaseDriver
from weiser.drivers.metric_stores import MetricStoreDB

class BaseCheck():
    def __init__(self, run_id: str, check: Check, driver: BaseDriver, 
                 datasource: str, metric_store: MetricStoreDB) -> None:
        self.run_id = run_id
        self.check = check
        self.driver = driver
        self.datasource = datasource
        self.metric_store = metric_store
        pass

    def apply_condition(self, value: Any) -> bool:
        threshold = self.check.threshold
        condition = self.check.condition

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

    def generate_check_id(self, check_name: str, dataset: str) -> str:
        encode = lambda s: str(s).encode('utf-8')
        m = hashlib.sha256()
        # Each check is run once for each datasource defined
        m.update(encode(self.datasource))
        # Each check is run for each dataset defined
        m.update(encode(dataset))
        # Each check name is unique across runs
        m.update(encode(check_name))
        return m.hexdigest()

    def execute_query(self, q: Select, verbose: bool=False) -> Any:
        return self.driver.execute_query(q, self.check, verbose)

    def append_result(self, success:bool, value:Any, results: List[dict], dataset: str, run_time: datetime, verbose: bool=False) -> List[Any]:
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

    def run(self, verbose: bool) -> List[Any]:
        datasets = self.check.dataset
        results = []
        if isinstance(datasets, str):
            datasets = [datasets]
        for dataset in datasets:
            q = self.get_query(dataset, verbose)
            rows = self.execute_query(q, verbose)
            if self.check.group_by:
                for row in rows:
                    success = self.apply_condition(row[-1]) 
                    self.append_result(success, row, results, dataset, datetime.now(), verbose)
            else:
                success = self.apply_condition(rows[0][0])
                self.append_result(success, rows[0][0], results, dataset, datetime.now(), verbose)

        return results

    def get_query(self, table: str, verbose: bool) -> Select:
        if verbose:
            pprint('Called BaseCheck')
        raise Exception('Get Query Method Not Implemented Yet')
    
    def build_query(self, select_stmnt: List[Any], table:str, limit: int=1, verbose: bool=False) -> Select:
        if self.check.group_by:
            q = Select().from_(table).select(*(self.check.group_by + select_stmnt)).group_by(*self.check.group_by)
        else:
            q = Select().from_(table).select(*select_stmnt).limit(1)
        if verbose:
            pprint(q.sql(pretty=True))
        return q     