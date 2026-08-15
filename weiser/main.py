import asyncio
import os
import typer
import datetime

from dotenv import load_dotenv
from rich import print
from typing import Optional
from typing_extensions import Annotated


from weiser.loader.export import export_results, print_results
from weiser.loader.config import load_config
from weiser.loader.models import BaseConfig
from weiser.runner import pre_run_config, run_checks, generate_sample_data
from weiser.evals.compare import lint_arms, print_scorecard
from weiser.evals.dataset import EvalDataset
from weiser.evals.runner import run_eval_suite


# Initialize Typer
app = typer.Typer()
version = "0.2.2"


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
    env_file: Annotated[
        str, typer.Option("--env-file", "-e", help="Path to custom .env file (default: .env)")
    ] = ".env",
):
    """
    Main Command
    """
    # Load .env
    if os.path.exists(env_file):
        if verbose:
            print(f"Loading .env file from: {env_file}")
        load_dotenv(
            dotenv_path=env_file,
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
    env_file: Annotated[
        str, typer.Option("--env-file", "-e", help="Path to custom .env file (default: .env)")
    ] = ".env",
):
    """
    Main Command
    """
    # Load .env
    if os.path.exists(env_file):
        if verbose:
            print(f"Loading .env file from: {env_file}")
        load_dotenv(
            dotenv_path=env_file,
            verbose=verbose,
        )
    env_variables = dict(os.environ)
    config = load_config(input_config, context=env_variables)
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
    env_file: Annotated[
        str, typer.Option("--env-file", "-e", help="Path to custom .env file (default: .env)")
    ] = ".env",
):
    """
    Generate sample data based on a check id name.
    """
    # Load .env
    if os.path.exists(env_file):
        if verbose:
            print(f"Loading .env file from: {env_file}")
        load_dotenv(
            dotenv_path=env_file,
            verbose=verbose,
        )
    env_variables = dict(os.environ)
    config = load_config(input_config, context=env_variables)
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


@app.command()
def eval(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    suite: Annotated[str, typer.Option("--suite", help="Name of the eval_suites entry to run")],
    split: Annotated[
        Optional[str],
        typer.Option("--split", help="Filter goldens by split (train/held_out)"),
    ] = None,
    dq_mode: Annotated[
        str,
        typer.Option(
            "--dq-mode",
            help="'latest': read the most recent stored DQ check result (default). "
            "'live': run configured DQ checks now.",
        ),
    ] = "latest",
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")
    ] = False,
    env_file: Annotated[
        str, typer.Option("--env-file", "-e", help="Path to custom .env file (default: .env)")
    ] = ".env",
):
    """
    Run an agent eval suite (weiser/evals).
    """
    if os.path.exists(env_file):
        if verbose:
            print(f"Loading .env file from: {env_file}")
        load_dotenv(dotenv_path=env_file, verbose=verbose)
    env_variables = dict(os.environ)
    config = load_config(input_config, context=env_variables)
    context = pre_run_config(config, verbose=verbose)
    base_config: BaseConfig = context["config"]

    eval_suites = {s.name: s for s in (base_config.eval_suites or [])}
    if suite not in eval_suites:
        print(f"[bold red]Error:[/bold red] eval suite '{suite}' not found in config.")
        raise typer.Exit(1)
    eval_suite = eval_suites[suite]

    agent_variants = {v.name: v for v in (base_config.agent_variants or [])}
    for warning in lint_arms(eval_suite, agent_variants):
        print(f"[yellow]Warning:[/yellow] {warning}")

    outcome = asyncio.run(
        run_eval_suite(
            context["run_id"],
            eval_suite,
            base_config,
            context["connections"],
            context["metric_store"],
            split=split,
            dq_mode=dq_mode,
            verbose=verbose,
        )
    )
    print_scorecard(outcome.results_path, list(outcome.results_by_arm.keys()))
    print(
        f"[{context['run_ts'].strftime('%Y-%m-%d %H:%M:%S')}] [green]Finished eval suite '{suite}'[/green] :rocket:  "
        f"results: {outcome.results_path}"
    )


@app.command(name="eval-compile")
def eval_compile(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    suite: Annotated[
        Optional[str],
        typer.Option("--suite", help="Name of a single eval_suites entry to validate; validates all if omitted"),
    ] = None,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")
    ] = False,
    env_file: Annotated[
        str, typer.Option("--env-file", "-e", help="Path to custom .env file (default: .env)")
    ] = ".env",
):
    """
    Validate eval config sections (agent_variants/semantic_layers/eval_suites) and
    resolve their references without executing anything.
    """
    if os.path.exists(env_file):
        if verbose:
            print(f"Loading .env file from: {env_file}")
        load_dotenv(dotenv_path=env_file, verbose=verbose)
    env_variables = dict(os.environ)
    config = load_config(input_config, context=env_variables)
    context = pre_run_config(config, compile_only=True, verbose=verbose)
    base_config: BaseConfig = context["config"]

    suites_to_check = base_config.eval_suites or []
    if suite:
        suites_to_check = [s for s in suites_to_check if s.name == suite]
        if not suites_to_check:
            print(f"[bold red]Error:[/bold red] eval suite '{suite}' not found in config.")
            raise typer.Exit(1)

    agent_variants = {v.name: v for v in (base_config.agent_variants or [])}
    semantic_layers = {s.name: s for s in (base_config.semantic_layers or [])}
    datasources = {d.name: d for d in base_config.datasources}

    for eval_suite in suites_to_check:
        for arm in eval_suite.arms:
            if arm.agent_variant not in agent_variants:
                print(
                    f"[bold red]Error:[/bold red] suite '{eval_suite.name}' arm "
                    f"'{arm.name}': unknown agent_variant '{arm.agent_variant}'"
                )
                raise typer.Exit(1)
            if arm.semantic_layer not in semantic_layers:
                print(
                    f"[bold red]Error:[/bold red] suite '{eval_suite.name}' arm "
                    f"'{arm.name}': unknown semantic_layer '{arm.semantic_layer}'"
                )
                raise typer.Exit(1)
            sl_config = semantic_layers[arm.semantic_layer]
            if sl_config.datasource not in datasources:
                print(
                    f"[bold red]Error:[/bold red] semantic_layer '{sl_config.name}': "
                    f"unknown datasource '{sl_config.datasource}'"
                )
                raise typer.Exit(1)

        for warning in lint_arms(eval_suite, agent_variants):
            print(f"[yellow]Warning:[/yellow] {warning}")

        dataset = EvalDataset.load(eval_suite.golden_set, eval_suite.goldens)
        if not dataset.goldens:
            print(
                f"[bold red]Error:[/bold red] suite '{eval_suite.name}' has no goldens "
                "(golden_set and goldens are both empty)."
            )
            raise typer.Exit(1)
        if verbose:
            print(
                f"Suite '{eval_suite.name}': {len(dataset.goldens)} goldens, "
                f"{len(eval_suite.arms)} arm(s)"
            )

    print(
        f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [green]Finished eval config compilation[/green] :rocket:"
    )


if __name__ == "__main__":
    app()
