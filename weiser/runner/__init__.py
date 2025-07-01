import json
import random
import uuid

from datetime import datetime, timedelta
from rich.progress import Progress
from sqlalchemy import text

from weiser.checks import CheckFactory
from weiser.loader.models import BaseConfig, ConnectionType, Condition
from weiser.drivers import DriverFactory
from weiser.drivers.metric_stores import MetricStoreFactory, MetricStoreDB


def run_checks(
    run_id: str,
    config: BaseConfig,
    connections: dict,
    metric_store: MetricStoreDB,
    verbose=False,
):
    results = []
    checks = []
    with Progress(transient=False) as progress:
        for check in config.checks:
            if isinstance(check.datasource, str):
                check.datasource = [check.datasource]
            for datasource in check.datasource:
                if datasource not in connections:
                    raise Exception(
                        f"Check <{check.name}>: Datasource {datasource} is not configured. "
                    )
                driver = connections[datasource]
                check_instance = CheckFactory.create_check(
                    run_id, check, driver, datasource, metric_store
                )
                checks.append(check_instance)
        if verbose:
            task = progress.add_task(f"[cyan]Running checks", total=len(checks) * 10)
        for check_instance in checks:
            result = {
                "check_instance": check_instance.check.name,
                "results": check_instance.run(verbose),
                "run_id": run_id,
            }
            if verbose:
                progress.update(task, advance=10)
            results.append(result)
    return results


def generate_sample_data(
    check_name: str,
    config: BaseConfig,
    connections: dict,
    metric_store: MetricStoreDB,
    verbose=False,
):
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
    results = []
    for check in config.checks:
        if check_name == check.name:
            if isinstance(check.datasource, str):
                check.datasource = [check.datasource]
            for i in range((end_date - start_date).days + 1):
                dt = start_date + timedelta(days=i)
                run_id = str(uuid.uuid4())

                for datasource in check.datasource:
                    if datasource not in connections:
                        raise Exception(
                            f"Check <{check.name}>: Datasource {datasource} is not configured. "
                        )
                    driver = connections[datasource]

                    check_instance = CheckFactory.create_check(
                        run_id, check, driver, datasource, metric_store
                    )
                    datasets = check_instance.check.dataset
                    if isinstance(datasets, str):
                        datasets = [datasets]
                    for dataset in datasets:
                        if check_instance.check.condition == Condition.between:
                            delta = int(
                                (
                                    check_instance.check.threshold[1]
                                    - check_instance.check.threshold[0]
                                )
                                / 2
                            )
                            value = random.randint(
                                check_instance.check.threshold[0] - delta,
                                check_instance.check.threshold[1] + delta,
                            )
                        else:
                            delta = int(check_instance.check.threshold / 2)
                            value = random.randint(
                                check_instance.check.threshold - delta,
                                check_instance.check.threshold + delta,
                            )
                        success = check_instance.apply_condition(value)
                        # Create temporary list for this check instance
                        temp_results = []
                        check_instance.append_result(
                            success, value, temp_results, dataset, dt, verbose
                        )

                        results.append(
                            {
                                "check_instance": check_instance,
                                "results": check_instance.run(verbose),
                                "run_id": run_id,
                            }
                        )
    return results


def pre_run_config(
    config: dict, compile_only: bool = False, verbose: bool = False
) -> dict:
    base_config = BaseConfig(**config)
    metric_store = None
    if base_config.connections:
        for config_conn in base_config.connections:
            if config_conn.type == ConnectionType.metricstore:
                metric_store = config_conn
                break
    context = {
        "config": base_config,
        "connections": {},
        "metric_store": MetricStoreFactory.create_driver(metric_store),
        "run_id": str(uuid.uuid4()),
        "run_ts": datetime.now(),
    }
    if verbose:
        pass
        # pprint(json.loads(base_config.model_dump_json()))
    if compile_only:
        return context
    for connection in base_config.datasources:
        context["connections"][connection.name] = DriverFactory.create_driver(
            connection
        )
        engine = context["connections"][connection.name].engine
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            if verbose:
                # pprint(f"Connected to {connection.name}")
                pass
    return context
