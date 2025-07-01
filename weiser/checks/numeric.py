from sqlglot.expressions import Select
from weiser.checks.base import BaseCheck
from weiser.loader.models import DBType


class CheckNumeric(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            [
                self.check.measure,
            ],
            table,
            verbose=verbose,
        )


# Cube specific check for Measures
class CheckMeasure(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            [
                f"MEASURE({self.check.measure})",
            ],
            table,
            verbose=verbose,
        )


class CheckRowCount(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            [
                "COUNT(*)",
            ],
            table,
            verbose=verbose,
        )


class CheckSum(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            [
                f"SUM({self.check.measure})",
            ],
            table,
            verbose=verbose,
        )


class CheckMax(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            [
                f"MAX({self.check.measure})",
            ],
            table,
            verbose=verbose,
        )


class CheckMin(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        return self.build_query(
            [
                f"MIN({self.check.measure})",
            ],
            table,
            verbose=verbose,
        )


class CheckNotEmpty(BaseCheck):
    def get_query(self, table: str, verbose: bool) -> Select:
        # This won't be used since we override process_dataset
        raise NotImplementedError("CheckNotEmpty uses custom process_dataset method")

    def get_check_suffix(self):
        """Return the suffix to append to check names."""
        return "not_empty"

    def get_null_count_sql(self, dimension: str) -> str:
        """Return the SQL expression to count NULL values for a dimension."""
        return f"SUM(CASE WHEN {dimension} IS NULL THEN 1 ELSE 0 END)"

    def get_total_count_sql(self, dimension: str) -> str:
        """Return the SQL expression to count total values for a dimension (used by percentage checks)."""
        return "COUNT(*)"

    def process_dataset(self, dataset_exp, results, verbose: bool) -> None:
        """Custom dataset processing for NotEmpty check."""
        from datetime import datetime

        if not self.check.dimensions:
            raise ValueError(
                "NotEmpty check requires at least one dimension to check for NULL values"
            )

        # Create separate query for each dimension
        for dimension in self.check.dimensions:
            # Build query to count NULL values in this specific dimension
            # Use build_query but disable check dimensions to avoid GROUP BY conflicts
            q = self.build_query(
                [self.get_null_count_sql(dimension)],
                dataset_exp,
                limit=1,
                use_check_dimensions=False,  # Don't add dimensions to GROUP BY
                verbose=verbose,
            )

            if verbose:
                print(
                    f"{self.__class__.__name__} check query for dimension '{dimension}': {q.sql()}"
                )

            rows = self.execute_query(q, verbose)
            actual_value = (
                rows[0][0] if rows and rows[0] and rows[0][0] is not None else 0
            )

            # Apply condition
            success = self.apply_condition(actual_value)

            # Create a modified result name to include the dimension being checked
            original_name = self.check.name
            original_dimensions = self.check.dimensions

            # Temporarily modify check to have single dimension for proper result naming
            self.check.name = f"{original_name}_{dimension}_{self.get_check_suffix()}"
            self.check.dimensions = (
                []
            )  # Clear dimensions to avoid tuple processing in append_result

            self.append_result(
                success, actual_value, results, dataset_exp, datetime.now(), verbose
            )

            # Restore original values for next iteration
            self.check.name = original_name
            self.check.dimensions = original_dimensions


class CheckNotEmptyPct(CheckNotEmpty):

    def get_check_suffix(self):
        """Return the suffix to append to check names."""
        return "not_empty_pct"

    def get_null_count_sql(self, dimension: str) -> str:
        """Return SQL to calculate NULL percentage for a dimension."""
        return f"CAST(SUM(CASE WHEN {dimension} IS NULL THEN 1 ELSE 0 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT)"
