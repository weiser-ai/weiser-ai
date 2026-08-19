from typing import Union

from weiser.loader.models import Check, CheckType
from weiser.drivers import BaseDriver
from weiser.drivers.metric_stores import MetricStoreDB
from weiser.checks.base import BaseCheck
from weiser.checks.numeric import (
    CheckNumeric,
    CheckRowCount,
    CheckSum,
    CheckMin,
    CheckMax,
    CheckMeasure,
    CheckNotEmpty,
    CheckNotEmptyPct,
)
from weiser.checks.anomaly import CheckAnomaly
from weiser.checks.cross_source import CheckCrossSourceRowCount, CheckCrossSourceFillRate


CHECK_TYPE_MAP = {
    CheckType.measure: CheckMeasure,
    CheckType.numeric: CheckNumeric,
    CheckType.row_count: CheckRowCount,
    CheckType.anomaly: CheckAnomaly,
    CheckType.sum: CheckSum,
    CheckType.max: CheckMax,
    CheckType.min: CheckMin,
    CheckType.not_empty: CheckNotEmpty,
    CheckType.not_empty_pct: CheckNotEmptyPct,
    CheckType.cross_source_row_count: CheckCrossSourceRowCount,
    CheckType.cross_source_fill_rate: CheckCrossSourceFillRate,
}

CHECK_TYPES = Union[
    BaseCheck,
    CheckNumeric,
    CheckRowCount,
    CheckAnomaly,
    CheckCrossSourceRowCount,
    CheckCrossSourceFillRate,
]


class CheckFactory:
    @staticmethod
    def create_check(
        run_id: str,
        check: Check,
        driver: BaseDriver,
        datasource: str,
        metric_store: MetricStoreDB,
        compare_driver: BaseDriver = None,
    ) -> CHECK_TYPES:
        check_class = CHECK_TYPE_MAP.get(check.type, None)
        if not check_class:
            raise Exception(f"Check Type {check.type} not implemented yet")
        return check_class(
            run_id, check, driver, datasource, metric_store, compare_driver=compare_driver
        )
