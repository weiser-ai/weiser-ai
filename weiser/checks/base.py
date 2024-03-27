import hashlib

from datetime import datetime
from pprint import pprint
from re import sub
from sqlglot import parse_one
from sqlglot.expressions import Select, Table
from typing import Any, List, Union

from weiser.loader.models import Check, Condition
from weiser.drivers import BaseDriver
from weiser.drivers.metric_stores import MetricStoreDB


class BaseCheck:
    def __init__(
        self,
        run_id: str,
        check: Check,
        driver: BaseDriver,
        datasource: str,
        metric_store: MetricStoreDB,
    ) -> None:
        self.run_id = run_id
        self.check = check
        self.driver = driver
        self.datasource = datasource
        self.metric_store = metric_store
        pass

    def snake_case(self, s: str) -> str:
        # Replace hyphens with spaces, then apply regular expression substitutions for title case conversion
        # and add an underscore between words, finally convert the result to lowercase
        return "_".join(
            sub(
                "([A-Z][a-z]+)", r" \1", sub("([A-Z]+)", r" \1", s.replace("-", " "))
            ).split()
        ).lower()

    def time_grain_alias(self) -> str:
        if self.check and self.check.time_grain:
            return self.snake_case(
                f"{self.check.time_grain.sql} {self.check.time_grain.granularity}"
            )

    def apply_condition(self, value: Any) -> bool:
        threshold = self.check.threshold
        condition = self.check.condition
        if value is None:
            return False
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
        raise Exception(f"Condition not implemented yet {condition}")

    def generate_check_id(self, dataset: str, check_name: str) -> str:
        encode = lambda s: str(s).encode("utf-8")
        m = hashlib.sha256()
        # Each check is run once for each datasource defined
        m.update(encode(self.datasource))
        # Each check name is unique across runs
        m.update(encode(check_name))
        # Each check is run for each dataset defined
        m.update(encode(dataset))
        return m.hexdigest()

    def execute_query(self, q: Select, verbose: bool = False) -> Any:
        return self.driver.execute_query(q, self.check, verbose)

    def append_result(
        self,
        success: bool,
        value: Any,
        results: List[dict],
        dataset: str,
        run_time: datetime,
        verbose: bool = False,
    ) -> List[Any]:

        result = self.check.model_dump()
        if self.check.group_by or self.check.time_grain:
            group_by_columns = (self.check.group_by if self.check.group_by else []) + (
                [self.time_grain_alias()] if self.check.time_grain else []
            )
            result["name"] = "_".join(
                (
                    result["name"],
                    "_".join(
                        map(
                            lambda pair: "_".join(pair),
                            zip(group_by_columns, map(str, value[:-1])),
                        )
                    ),
                )
            )

        if isinstance(dataset, str):
            check_id_dataset = dataset
        else:
            check_id_dataset = "_".join(map(str, list(dataset.find_all(Table))))

        result.update(
            {
                "check_id": self.generate_check_id(check_id_dataset, result["name"]),
                "datasource": self.datasource,
                "dataset": dataset,
                "actual_value": (
                    value[-1] if self.check.group_by or self.check.time_grain else value
                ),
                "success": success,
                "fail": not success,
                "run_id": self.run_id,
                "run_time": run_time.isoformat(),
            }
        )
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
            exp = self.parse_dataset(dataset)
            q = self.get_query(exp, verbose)
            rows = self.execute_query(q, verbose)
            if self.check.group_by or self.check.time_grain:
                for row in rows:
                    success = self.apply_condition(row[-1])
                    self.append_result(
                        success, row, results, dataset, datetime.now(), verbose
                    )
            else:
                success = self.apply_condition(rows[0][0])
                self.append_result(
                    success, rows[0][0], results, dataset, datetime.now(), verbose
                )

        return results

    def parse_dataset(self, dataset) -> Union[Table, str]:
        exp = parse_one(dataset)
        if list(exp.find_all(Table)):
            return parse_one(dataset).subquery(alias="dataset_")
        return dataset

    def get_query(self, table: str, verbose: bool) -> Select:
        if verbose:
            pprint("Called BaseCheck")
        raise Exception("Get Query Method Not Implemented Yet")

    def build_query(
        self,
        select_stmnt: List[Any],
        table: str,
        limit: int = 1,
        group_by: List[Any] = None,
        verbose: bool = False,
    ) -> Select:

        group_by = [] if group_by is None else group_by

        if self.check.time_grain:
            time_grain_alias = self.time_grain_alias()
            time_grain_sql = f"DATE_TRUNC('{self.check.time_grain.granularity}', {self.check.time_grain.sql})"
            select_stmnt = [f"{time_grain_sql} AS {time_grain_alias}"] + select_stmnt
            group_by = group_by + [time_grain_sql]

        if self.check.group_by:
            select_stmnt = self.check.group_by + select_stmnt
            group_by = group_by + self.check.group_by

        q = Select().from_(table).select(*select_stmnt)

        if self.check.filter:
            q = q.where(self.check.filter)

        if group_by:
            q = q.group_by(*group_by)
        else:  # limit only if no group-by.
            q = q.limit(limit)

        if verbose:
            pprint(q.sql(pretty=True))
        return q
