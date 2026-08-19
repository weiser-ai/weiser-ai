from datetime import datetime
from typing import Any, List, Tuple

from sqlglot.expressions import Select
from weiser.checks.base import BaseCheck


class CrossSourceCheck(BaseCheck):
    """Base class for checks that compare `dataset` (on `driver`) against
    `compare_dataset` (on `compare_driver`). `dataset`/`compare_dataset` are zipped
    pairwise, so a single check can validate several table pairs at once."""

    def get_pairs(self) -> List[Tuple[str, str]]:
        if not self.compare_driver or not self.check.compare_dataset:
            raise ValueError(
                f"Check <{self.check.name}> requires both compare_datasource and "
                "compare_dataset to be set"
            )
        datasets = self.check.dataset
        compare_datasets = self.check.compare_dataset
        if isinstance(datasets, str):
            datasets = [datasets]
        if isinstance(compare_datasets, str):
            compare_datasets = [compare_datasets]
        if len(datasets) != len(compare_datasets):
            raise ValueError(
                f"Check <{self.check.name}>: dataset and compare_dataset must have "
                f"the same number of entries ({len(datasets)} vs {len(compare_datasets)})"
            )
        return list(zip(datasets, compare_datasets))

    def execute_compare_query(self, q: Select, verbose: bool = False) -> Any:
        return self.compare_driver.execute_query(q, self.check, verbose)

    def relative_diff(self, a: float, b: float) -> float:
        denom = max(abs(a), abs(b))
        if denom == 0:
            return 0.0
        return abs(a - b) / denom


class CheckCrossSourceRowCount(CrossSourceCheck):
    def run(self, verbose: bool) -> List[Any]:
        results = []
        for primary_table, compare_table in self.get_pairs():
            primary_exp = self.parse_dataset(primary_table)
            compare_exp = self.parse_dataset(compare_table)

            primary_q = self.build_query(
                ["COUNT(*)"], primary_exp, use_check_dimensions=False, verbose=verbose
            )
            compare_q = self.build_query(
                ["COUNT(*)"], compare_exp, use_check_dimensions=False, verbose=verbose
            )

            primary_count = self.execute_query(primary_q, verbose)[0][0]
            compare_count = self.execute_compare_query(compare_q, verbose)[0][0]

            actual_value = self.relative_diff(primary_count, compare_count)
            success = self.apply_condition(actual_value)

            if verbose:
                print(
                    f"{self.check.name}: {primary_table}={primary_count} "
                    f"{compare_table}={compare_count} rel_diff={actual_value}"
                )

            self._append_pair_result(
                primary_table, compare_table, success, actual_value, results, verbose
            )
        return results

    def _append_pair_result(
        self, primary_table, compare_table, success, actual_value, results, verbose
    ) -> None:
        pair_label = f"{primary_table}__vs__{compare_table}"
        original_name = self.check.name
        self.check.name = f"{original_name}_{self.snake_case(pair_label)}"
        self.append_result(
            success, actual_value, results, pair_label, datetime.now(), verbose
        )
        self.check.name = original_name


class CheckCrossSourceFillRate(CrossSourceCheck):
    def get_fill_rate_sql(self, dimension: str) -> str:
        return (
            "CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE "
            f"CAST(SUM(CASE WHEN {dimension} IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) "
            "/ CAST(COUNT(*) AS FLOAT) END"
        )

    def run(self, verbose: bool) -> List[Any]:
        if not self.check.dimensions:
            raise ValueError(
                f"Check <{self.check.name}> requires at least one dimension (column) "
                "to compare fill-rate on"
            )
        results = []
        for primary_table, compare_table in self.get_pairs():
            primary_exp = self.parse_dataset(primary_table)
            compare_exp = self.parse_dataset(compare_table)

            for dimension in self.check.dimensions:
                primary_q = self.build_query(
                    [self.get_fill_rate_sql(dimension)],
                    primary_exp,
                    use_check_dimensions=False,
                    verbose=verbose,
                )
                compare_q = self.build_query(
                    [self.get_fill_rate_sql(dimension)],
                    compare_exp,
                    use_check_dimensions=False,
                    verbose=verbose,
                )

                primary_rate = self.execute_query(primary_q, verbose)[0][0] or 0.0
                compare_rate = (
                    self.execute_compare_query(compare_q, verbose)[0][0] or 0.0
                )

                actual_value = self.relative_diff(primary_rate, compare_rate)
                success = self.apply_condition(actual_value)

                if verbose:
                    print(
                        f"{self.check.name}[{dimension}]: {primary_table}={primary_rate} "
                        f"{compare_table}={compare_rate} rel_diff={actual_value}"
                    )

                self._append_pair_result(
                    primary_table,
                    compare_table,
                    dimension,
                    success,
                    actual_value,
                    results,
                    verbose,
                )
        return results

    def _append_pair_result(
        self,
        primary_table,
        compare_table,
        dimension,
        success,
        actual_value,
        results,
        verbose,
    ) -> None:
        pair_label = f"{primary_table}__vs__{compare_table}__{dimension}"
        original_name = self.check.name
        original_dimensions = self.check.dimensions
        self.check.name = f"{original_name}_{self.snake_case(pair_label)}"
        # Clear dimensions so append_result treats this as a scalar result, not a
        # dimensioned tuple.
        self.check.dimensions = []
        self.append_result(
            success, actual_value, results, pair_label, datetime.now(), verbose
        )
        self.check.name = original_name
        self.check.dimensions = original_dimensions
