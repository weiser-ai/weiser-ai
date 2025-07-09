import os
import typer
import datetime

from dotenv import load_dotenv
from rich import print
from typing_extensions import Annotated


from weiser.loader.export import export_results, print_results
from weiser.loader.config import load_config
from weiser.runner import pre_run_config, run_checks, generate_sample_data


# Initialize Typer
app = typer.Typer()
version = "0.2.0"


@app.callback()
def callback():
    """
    Weiser is a data quality framework designed to help you ensure the integrity and accuracy of your data.
    It provides a set of tools and checks to validate your data and detect anomalies.
    It also includes a dashboard to visualize the results of the checks.
    """
    print(
        f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [bold red]Running Weiser version:[/bold red] [green]{version}[/green] :boom:\n"
    )


@app.command()
def run(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")
    ] = False,
    show_ids: Annotated[
        bool, typer.Option("--show-ids", "-i", help="Print check ids to results table")
    ] = False,
    skip_export: Annotated[
        bool, typer.Option("--skip-export", "-s", help="Skip exporting results")
    ] = False,
):
    """
    Main Command
    """
    # Load .env
    if os.path.exists(".env"):
        if verbose:
            print("Loading .env file")
        load_dotenv(
            dotenv_path=".env",
            verbose=verbose,
        )
    env_variables = dict(os.environ)
    config = load_config(input_config, context=env_variables)
    context = pre_run_config(config, verbose=verbose)
    results = run_checks(
        context["run_id"],
        context["config"],
        context["connections"],
        context["metric_store"],
        verbose,
    )
    if not skip_export:
        export_results(
            context["run_id"],
            context["metric_store"],
            slack_url=context["config"].slack_url,
            run_ts=context["run_ts"],
            verbose=verbose,
        )
    print_results(results, show_ids)
    print(
        f"[{context['run_ts'].strftime('%Y-%m-%d %H:%M:%S')}] [green]Finished Run[/green] :rocket:"
    )


@app.command()
def compile(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")
    ] = False,
):
    """
    Main Command
    """
    # Load .env
    if os.path.exists(".env"):
        load_dotenv(
            dotenv_path=".env",
            verbose=verbose,
        )
    config = load_config(input_config)
    pre_run_config(config, compile_only=True, verbose=verbose)
    print(
        f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [green]Finished Config compilation[/green] :rocket:"
    )


@app.command()
def sample(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    check: Annotated[str, typer.Option("--check", "-c", help="Id to populate")],
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")
    ] = False,
    skip_export: Annotated[
        bool, typer.Option("--skip-export", "-s", help="Skip exporting results")
    ] = False,
):
    """
    Generate sample data based on a check id name.
    """
    # Load .env
    if os.path.exists(".env"):
        load_dotenv(
            dotenv_path=".env",
            verbose=verbose,
        )
    config = load_config(input_config)
    context = pre_run_config(config, verbose)
    results = generate_sample_data(
        check,
        context["config"],
        context["connections"],
        context["metric_store"],
        verbose,
    )
    if not skip_export:
        export_results(context["run_id"], config)
    print(
        f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [green]Finished Generating Sample[/green] :rocket:"
    )


if __name__ == "__main__":
    app()
