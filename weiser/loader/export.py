from rich.console import Console
from rich.table import Table
from weiser.drivers.metric_stores import MetricStoreDB
from typing import Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

console = Console()


def export_results(
    run_id: str,
    metric_store: MetricStoreDB,
    slack_channel: Optional[str] = None,
    slack_token: Optional[str] = None
):
    """Export results to storage and optionally to Slack.
    
    Args:
        run_id: The ID of the run to export
        metric_store: The metric store database
        slack_channel: Optional Slack channel to post results to
        slack_token: Optional Slack bot token
    """
    # Export to storage
    results = metric_store.export_results(run_id)
    
     # Export to Slack if configured
    if slack_channel and slack_token:
        try:
            client = WebClient(token=slack_token)
            
            # Format the results message
            summary = results['summary']
            message = [
                f"*Results Summary for Run {run_id}*",
                f"• Total Checks: {summary['total_checks']}",
                f"• Passed: {summary['passed_checks']} ✅",
                f"• Failed: {summary['failed_checks']} ❌\n"
            ]
            
            # Add failure details if any
            if results['failures']:
                message.append("*Failed Checks Details:*")
                for i, failure in enumerate(results['failures'], 1):
                    message.append(
                        f"{i}. *{failure['name']}* ({failure['check_id']})\n"
                        f"   • Dataset: {failure['dataset']}\n"
                        f"   • Datasource: {failure['datasource']}\n"
                        f"   • Condition: {failure['condition']}\n"
                        f"   • Actual Value: {failure['actual_value']}\n"
                        f"   • Threshold: {failure['threshold']}\n"
                        f"   • Type: {failure['type']}\n"
                    )
            
            # Send message to Slack
            response = client.chat_postMessage(
                channel=slack_channel,
                text="\n".join(message),
                mrkdwn=True
            )
            
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
