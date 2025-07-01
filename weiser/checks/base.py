import hashlib

from datetime import datetime
from rich import print
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

    def time_dimension_alias(self) -> str:
        if self.check and self.check.time_dimension:
            return self.snake_case(
                f"{self.check.time_dimension.name} {self.check.time_dimension.granularity}"
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
        if condition == Condition.eq:
            return value == threshold
        if condition == Condition.neq:
            return value != threshold
        if condition == Condition.between:
            if isinstance(threshold, list) and len(threshold) == 2:
                return value >= threshold[0] and value <= threshold[1]
            else:
                raise ValueError(
                    "Condition 'between' requires a threshold list with two elements"
                )
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
        dataset: Union[Table, str],
        run_time: datetime,
        verbose: bool = False,
    ) -> List[Any]:

        result = self.check.model_dump()
        if self.check.dimensions or self.check.time_dimension:
            dimensions_columns = (
                self.check.dimensions if self.check.dimensions else []
            ) + ([self.time_dimension_alias()] if self.check.time_dimension else [])
            result["name"] = "_".join(
                (
                    result["name"],
                    "_".join(
                        map(
                            lambda pair: "_".join(pair),
                            zip(dimensions_columns, map(str, value[:-1])),
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
                "dataset": check_id_dataset,
                "actual_value": (
                    value[-1]
                    if self.check.dimensions or self.check.time_dimension
                    else value
                ),
                "success": success,
                "fail": not success,
                "run_id": self.run_id,
                "run_time": run_time.isoformat(),
            }
        )
        if verbose:
            pass
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
            self.process_dataset(exp, results, verbose)
        return results

    def process_dataset(
        self, dataset_exp: Union[Table, str], results: List[Any], verbose: bool
    ) -> None:
        """Process a single dataset. Can be overridden by subclasses for custom logic."""
        q = self.get_query(dataset_exp, verbose)
        rows = self.execute_query(q, verbose)
        self.process_query_results(rows, dataset_exp, results, verbose)

    def process_query_results(
        self,
        rows: List[Any],
        dataset_exp: Union[Table, str],
        results: List[Any],
        verbose: bool,
    ) -> None:
        """Process query results. Can be overridden by subclasses for custom result processing."""
        if self.check.dimensions or self.check.time_dimension:
            for row in rows:
                success = self.apply_condition(row[-1])
                self.append_result(
                    success, row, results, dataset_exp, datetime.now(), verbose
                )
        else:
            success = self.apply_condition(rows[0][0])
            self.append_result(
                success, rows[0][0], results, dataset_exp, datetime.now(), verbose
            )

    def parse_dataset(self, dataset) -> Union[Table, str]:
        exp = parse_one(dataset)
        if list(exp.find_all(Table)):
            return parse_one(dataset).subquery(alias="dataset_")
        return dataset

    def get_query(self, table: str, verbose: bool) -> Select:
        if verbose:
            print("Called BaseCheck")
        raise Exception("Get Query Method Not Implemented Yet")

    def build_query(
        self,
        select_stmnt: List[Any],
        table: str,
        limit: int = 1,
        dimensions: List[Any] = None,
        verbose: bool = False,
        use_check_dimensions: bool = True,
    ) -> Select:

        dimensions = [] if dimensions is None else dimensions

        if self.check.time_dimension:
            time_dimension_alias = self.time_dimension_alias()
            time_dimension_sql = f"DATE_TRUNC('{self.check.time_dimension.granularity}', {self.check.time_dimension.name})"
            select_stmnt = [
                f"{time_dimension_sql} AS {time_dimension_alias}"
            ] + select_stmnt
            dimensions = dimensions + [time_dimension_sql]

        if use_check_dimensions and self.check.dimensions:
            select_stmnt = self.check.dimensions + select_stmnt
            dimensions = dimensions + self.check.dimensions

        q = Select().from_(table).select(*select_stmnt)

        if self.check.filter:
            q = q.where(self.check.filter)

        if dimensions:
            q = q.group_by(*dimensions)
        else:  # limit only if no group-by.
            q = q.limit(limit)

        if verbose:
            # print(q.sql(pretty=True))
            pass
        return q
