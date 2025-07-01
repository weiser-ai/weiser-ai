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
}

CHECK_TYPES = Union[BaseCheck, CheckNumeric, CheckRowCount, CheckAnomaly]


class CheckFactory:
    @staticmethod
    def create_check(
        run_id: str,
        check: Check,
        driver: BaseDriver,
        datasource: str,
        metric_store: MetricStoreDB,
    ) -> CHECK_TYPES:
        check_class = CHECK_TYPE_MAP.get(check.type, None)
        if not check_class:
            raise Exception(f"Check Type {check.type} not implemented yet")
        return check_class(run_id, check, driver, datasource, metric_store)
