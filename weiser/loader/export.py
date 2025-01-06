import json

from datetime import datetime
from rich.console import Console
from rich.table import Table
from weiser.drivers.metric_stores import MetricStoreDB
from typing import Optional
from slack_sdk.webhook import WebhookClient
from slack_sdk.errors import SlackApiError


console = Console()


def export_results(
    run_id: str,
    metric_store: MetricStoreDB,
    slack_url: Optional[str] = None,
    run_ts: Optional[datetime] = None,
    verbose: bool = False,
) -> bool:
    """Export results to storage and optionally to Slack.
    
    Args:
        run_id: The ID of the run to export
        metric_store: The metric store database
        slack_url: Optional Slack webhook URL to post results to
        run_ts: Optional datetime for the run
        verbose: Optional bool to enable verbose output

    Returns:
        True if successful, False otherwise
    """
    # Export to storage
    results = metric_store.export_results(run_id)
    
     # Export to Slack if configured
    if slack_url:
        try:
            client = WebhookClient(url=slack_url)
            
            # Format the results message
            summary = results['summary']
            header = "\n".join([
                f"*Results Summary for Run {run_ts.strftime('%Y-%m-%d %H:%M:%S')} - {run_id[:8]}*",
                f"• Total Checks: {summary['total_checks']}",
                f"• Passed: {summary['passed_checks']} ✅",
                f"• Failed: {summary['failed_checks']} ❌\n"
            ])
            # Create blocks list with first section.
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": header
                    }
                }
            ]            
            # Add failure details if any
            if results['failures']:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*Failed Checks Details:*"}})
                for i, failure in enumerate(results['failures'], 1):
                    block = (
                        f"{i}. *{failure['name']}* ({failure['check_id'][:10]})\n"
                        f"   • Dataset: {failure['dataset']}  at Data Source: {failure['datasource']}\n"
                        f"   • Actual Value: {failure['actual_value']}\n"
                        f"   • Type: {failure['type']}\n"
                    )
                    if failure['type'] != 'anomaly':
                        block += (
                            f"   • Condition: {failure['condition']}\n"
                            f"   • Threshold: {failure['threshold']}\n"
                        )
                    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": block}})
            
            # Send message to Slack
            response = client.send(
                text=header,
                blocks=blocks,
            )

            if response.status_code != 200 or response.body != "ok":
                console.print(f"Error posting to Slack: {response.body}")
            elif verbose:
                console.print("Results exported to Slack :white_check_mark:")
            
        except SlackApiError as e:
            console.print(f"Error posting to Slack: {e.response['error']}")
    
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
