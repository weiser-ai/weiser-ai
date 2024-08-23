from rich.console import Console
from rich.table import Table, Column
from weiser.drivers.metric_stores import MetricStoreDB

console = Console()


def export_results(run_id: str, metric_store: MetricStoreDB):
    metric_store.export_results(run_id)
    return True


def print_results(results, show_ids: bool):
    columns = [
        "Check Name",
        "Datasource",
        "Dataset",
        "Measure",
        "Condition",
        "Actual Value",
        "Threshold",
        "Result",
    ]
    if show_ids:
        # columns = [Column(header="Check Id", overflow="fold")] + columns
        columns = ["Check Id"] + columns
    table = Table(*columns)
    for results_item in results:
        for result in results_item["results"]:
            row = [
                result.get("name"),
                result.get("datasource"),
                result.get("dataset"),
                result.get("measure") or result.get("type"),
                result.get("condition"),
                str(result.get("actual_value")),
                str(
                    result.get("threshold_list")
                    if result.get("threshold") is None
                    else result.get("threshold")
                ),
                ":x:" if result.get("fail") else ":white_check_mark:",
            ]
            if show_ids:
                row = [result.get("check_id")] + row
            table.add_row(*row)
    console.print(table)
