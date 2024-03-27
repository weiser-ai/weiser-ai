from datetime import datetime
from pprint import pprint
from typing import Any, List

from sqlglot.expressions import Select
from weiser.checks.base import BaseCheck


# TODO: map to different algorithms
# Basically all algorithms require extracting a window of values
# which then different algorithms can be performed
class CheckAnomaly(BaseCheck):

    def execute_query(self, q: Select, verbose: bool = False) -> Any:
        return self.metric_store.execute_query(q, self.check, verbose)

    def run(self, verbose: bool) -> List[Any]:
        datasets = self.check.dataset
        results = []
        if isinstance(datasets, str):
            datasets = [datasets]
        for dataset in datasets:
            exp = self.parse_dataset(dataset)
            q = self.get_query(exp, verbose)
            rows = self.execute_query(q, verbose)
            # Algorithm Name: Median Absolute Deviation (MAD)
            # M_i = 0.6745 * (x_i - Median(X) ) / MAD
            # Robust Z-score formula.
            # 0.6745 is the 75th percentile of the standard normal distribution
            # to which the MAD converges to.
            # If MAD -> 0 then Z score is 0 for testing purposes.
            # (Constant value across time, last_value = Median with std = 0)
            m_i = (
                (0.6745 * (rows[0][2] - rows[0][1]) / (rows[0][0]))
                if int(rows[0][0]) != 0
                else 0
            )
            success = self.apply_condition(m_i)
            self.append_result(
                success, rows[0][2], results, dataset, datetime.now(), verbose
            )

        return results

    # TODO: implement in polars or pandas
    # extract window of values and calculate MAD and median in python (with rust backend for speed)
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            ["mad(actual_value)", "median(actual_value)", "last(actual_value)"],
            table,
            verbose=verbose,
        )

    def build_query(
        self, select_stmnt: List[Any], table: str, limit: int = 1, verbose: bool = False
    ) -> Select:
        q = (
            Select()
            .from_(
                Select()
                .from_(table)
                .select("actual_value")
                .where(f"check_id = '{self.check.check_id}'")
                .order_by("run_time ASC")
                .subquery(alias="q0_")
            )
            .select(*select_stmnt)
            .limit(1)
        )
        if verbose:
            pprint(q.sql(pretty=True))
        return q
