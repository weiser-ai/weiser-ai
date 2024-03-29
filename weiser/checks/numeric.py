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
