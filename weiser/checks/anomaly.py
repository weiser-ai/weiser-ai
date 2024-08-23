import duckdb
import pandas as pd

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
            result_window = self.execute_query(q, verbose)
            if len(result_window) < 5:
                self.append_result(
                    False,
                    result_window[-1][0] if len(result_window) > 0 else None,
                    results,
                    dataset,
                    datetime.now(),
                    verbose,
                )
                continue
            results_df = pd.DataFrame(
                result_window, columns=["actual_value", "run_time"]
            )
            with duckdb.connect(":memory:") as conn:
                conn.execute("CREATE TABLE results_table AS SELECT * FROM results_df")

                rows = conn.execute(
                    """ SELECT mad(actual_value), median(actual_value), last(actual_value) 
                        FROM (SELECT * FROM results_df ORDER BY run_time ASC) q LIMIT 1"""
                ).fetchall()
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

    # It only extracts window of values, the MAD and median calculation happens in duckdb
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            ["actual_value", "run_time"],
            table,
            verbose=verbose,
        )

    def build_query(
        self,
        select_stmnt: List[Any],
        table: str,
        limit: int = 100,
        verbose: bool = False,
    ) -> Select:
        q = (
            Select()
            .from_(table)
            .select(*select_stmnt)
            .where(
                f"check_id LIKE '{self.check.check_id}%%' AND {self.check.filter if self.check.filter else '1=1'}"
            )
            .order_by("run_time ASC")
            .limit(limit)
        )
        if verbose:
            pass
        return q
