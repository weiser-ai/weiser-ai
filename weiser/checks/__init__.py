from pprint import pprint
from pypika import Table, Query, functions as fn

from weiser.loader.models import Check, CheckType, Condition
from weiser.drivers import BaseDriver

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
    def __init__(self, check: Check, driver: BaseDriver) -> None:
        self.check = check
        self.driver = driver
        pass

    def execute_query(self, q):
        engine = self.driver.engine
        with engine.connect() as conn:
            rows = list(conn.execute(str(q)))
            if not len(rows) > 0 and not len(rows[0]) > 0 and not rows[0][0] is None:
                raise Exception(f'Unexpected result executing check: {self.check.model_dump()}')
            value = rows[0][0]
        return value


    def run(self, verbose: bool):
        raise Exception('Check Run Method Not Implemented Yet')

class CheckNumeric(BaseCheck):
    def run(self, verbose: bool):
        pass
    

class CheckRowCount(BaseCheck):
    def run(self, verbose: bool):
        query: Query = self.driver.query
        datasets = self.check.dataset
        results = []
        if isinstance(datasets, str):
            datasets = [datasets]
        for dataset in datasets:
            table = Table(dataset)
            q = query.from_(table).select(fn.Count('*'))
            if verbose:
                pprint(q)

            value = self.execute_query(q)

            success = apply_condition(value, self.check.threshold, self.check.condition) 
            result = self.check.model_dump()
            result.update({
                'actual_value': value,
                'success': success,
                'fail': not success
            })
            if verbose:
                pprint(result)
            results.append(result)
        return results
            


CHECK_TYPE_MAP = {
    CheckType.numeric: CheckNumeric,
    CheckType.row_count: CheckRowCount,
}

class CheckFactory():
    @staticmethod
    def create_check(check: Check, driver: BaseDriver):
        check_class = CHECK_TYPE_MAP.get(check.type, None)
        if not check_class:
            raise Exception(f'Check Type {check.type} not implemented yet')
        return check_class(check, driver)