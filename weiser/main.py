import asyncio
import json
import os
import typer
import datetime
import yaml

from dotenv import load_dotenv
from rich import print
from typing import Optional
from typing_extensions import Annotated


from weiser.loader.export import export_results, print_results
from weiser.loader.config import load_config
from weiser.loader.models import BaseConfig
from weiser.runner import pre_run_config, run_checks, generate_sample_data
from weiser.evals.compare import lint_arms, print_scorecard, print_variance_report
from weiser.evals.dataset import EvalDataset
from weiser.evals.runner import run_eval_suite
from weiser.evals.synthesizer import generate_synthetic_goldens
from weiser.evals.dataquality import known_issue_hints
from weiser.evals.gate import gate
from weiser.evals.calibrate import calibrate_judge
from weiser.evals.semantic_layer import SemanticLayerFactory


# Initialize Typer
app = typer.Typer()
version = "0.3.1"


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
    repeats: Annotated[
        int,
        typer.Option(
            "--repeats",
            help="Run each golden this many times per arm and report per-question "
            "variance (FIX-7 style) -- use to gauge how stable a score is before "
            "trusting a single run's delta.",
        ),
    ] = 1,
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

    if dq_mode not in ("latest", "live"):
        print(
            f"[bold red]Error:[/bold red] invalid --dq-mode '{dq_mode}' (expected 'latest' or 'live')."
        )
        raise typer.Exit(1)
    if repeats < 1:
        print(f"[bold red]Error:[/bold red] --repeats must be >= 1 (got {repeats}).")
        raise typer.Exit(1)

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
            repeats=repeats,
            verbose=verbose,
        )
    )
    print_scorecard(outcome.results_path, list(outcome.results_by_arm.keys()))
    if repeats > 1:
        print_variance_report(outcome.results_path, list(outcome.results_by_arm.keys()))
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


@app.command(name="eval-synth")
def eval_synth(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    semantic_layer: Annotated[
        str, typer.Option("--semantic-layer", help="Name of the semantic_layers entry to introspect")
    ],
    out: Annotated[str, typer.Option("--out", help="Path to write the generated golden YAML file")],
    n_per_view: Annotated[
        int, typer.Option("--n-per-view", help="Number of synthetic questions to generate per view")
    ] = 3,
    views: Annotated[
        Optional[str],
        typer.Option("--views", help="Comma-separated view names to restrict generation to; all views if omitted"),
    ] = None,
    model: Annotated[
        str,
        typer.Option(
            "--model",
            help="Seed-writer model -- deliberately separate from both the agent under "
            "test and any judge model, to avoid contaminating grading with generation",
        ),
    ] = "anthropic:claude-sonnet-5",
    use_dq_hints: Annotated[
        bool,
        typer.Option(
            "--use-dq-hints/--no-dq-hints",
            help="Bias generated questions toward views with a currently-failing weiser DQ check",
        ),
    ] = True,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")
    ] = False,
    env_file: Annotated[
        str, typer.Option("--env-file", "-e", help="Path to custom .env file (default: .env)")
    ] = ".env",
):
    """
    Generate synthetic seed questions from a semantic layer's schema, optionally biased
    toward views with a currently-failing weiser DQ check. Writes a golden YAML file
    (source=synthetic, split=train) directly usable as an eval_suite's golden_set --
    review before promoting anything to held_out or adding reference_values.
    """
    if os.path.exists(env_file):
        if verbose:
            print(f"Loading .env file from: {env_file}")
        load_dotenv(dotenv_path=env_file, verbose=verbose)
    env_variables = dict(os.environ)
    config = load_config(input_config, context=env_variables)
    context = pre_run_config(config, verbose=verbose)
    base_config: BaseConfig = context["config"]

    semantic_layers = {s.name: s for s in (base_config.semantic_layers or [])}
    if semantic_layer not in semantic_layers:
        print(f"[bold red]Error:[/bold red] semantic layer '{semantic_layer}' not found in config.")
        raise typer.Exit(1)
    sl_config = semantic_layers[semantic_layer]
    datasources = {d.name: d for d in base_config.datasources}
    if sl_config.datasource not in datasources:
        print(
            f"[bold red]Error:[/bold red] semantic layer '{semantic_layer}': unknown "
            f"datasource '{sl_config.datasource}'."
        )
        raise typer.Exit(1)

    sl_adapter = SemanticLayerFactory.create(sl_config, datasources[sl_config.datasource])
    schema_catalog = sl_adapter.get_schema()

    dq_hints = None
    if use_dq_hints:
        dq_hints = known_issue_hints(base_config.checks, context["connections"], context["metric_store"])
        if verbose and dq_hints:
            print(f"Known-issue hints for {len(dq_hints)} view(s): {list(dq_hints.keys())}")

    view_list = [v.strip() for v in views.split(",")] if views else None
    goldens = asyncio.run(
        generate_synthetic_goldens(
            schema_catalog, n_per_view=n_per_view, dq_hints=dq_hints, model=model, views=view_list
        )
    )

    out_dir = os.path.dirname(out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out, "w") as f:
        yaml.safe_dump(
            {"goldens": [g.model_dump(exclude_none=True) for g in goldens]}, f, sort_keys=False
        )

    print(f"[green]Generated {len(goldens)} synthetic goldens[/green] -> {out}")
    print(
        "[yellow]Review before promoting any of these to held_out or adding reference_values.[/yellow]"
    )


@app.command(name="eval-gate")
def eval_gate(
    candidate: Annotated[
        str, typer.Option("--candidate", help="Path to the candidate run's JSONL trace substrate")
    ],
    baseline: Annotated[
        str, typer.Option("--baseline", help="Path to the baseline run's JSONL trace substrate")
    ],
    arm: Annotated[str, typer.Option("--arm", help="Arm name to compare (must exist in both files)")],
    max_regression: Annotated[
        float,
        typer.Option("--max-regression", help="Max allowed accuracy drop, in percentage points"),
    ] = 3.0,
    exclude_confounded: Annotated[
        bool,
        typer.Option(
            "--exclude-confounded/--include-confounded",
            help="Exclude data_quality-attributed candidate rows from the regression delta "
            "(default: exclude -- a live DQ incident shouldn't fail an unrelated agent PR)",
        ),
    ] = True,
):
    """
    CI regression gate: compares a candidate run's JSONL results against a baseline's
    for one arm, and exits non-zero on regression. Mirrors
    eval_harness_improvement_spec.md's FIX-6.
    """
    result = gate(
        candidate, baseline, arm=arm, max_regression_pp=max_regression, exclude_confounded=exclude_confounded
    )

    summary = {
        "passed": result.passed,
        "delta_pp": round(result.delta_pp, 2),
        "excluded_confounded_count": result.excluded_confounded_count,
        "reasons": result.reasons,
        "candidate": result.candidate_summary,
        "baseline": result.baseline_summary,
    }
    print(json.dumps(summary, indent=2, default=str))

    if result.excluded_confounded_count:
        print(
            f"[yellow]Excluded {result.excluded_confounded_count} data_quality-confounded "
            "row(s) from the candidate regression delta.[/yellow]"
        )
    if result.passed:
        print(f"[green]Gate passed[/green] ({result.delta_pp:+.1f}pp vs. baseline)")
    else:
        print("[bold red]Gate failed[/bold red]")
        for reason in result.reasons:
            print(f"  - {reason}")
        raise typer.Exit(1)


@app.command(name="eval-calibrate")
def eval_calibrate(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    suite: Annotated[str, typer.Option("--suite", help="Name of the eval_suites entry the metric is defined in")],
    metric_name: Annotated[
        str, typer.Option("--metric-name", help="Name of the llm_judge metric within that suite to calibrate")
    ],
    turns: Annotated[str, typer.Option("--turns", help="Path to the calibration turns JSONL file")],
    labels: Annotated[str, typer.Option("--labels", help="Path to the human labels JSONL file")],
    threshold: Annotated[
        float, typer.Option("--threshold", help="Minimum acceptable Spearman correlation against human labels")
    ] = 0.5,
    out: Annotated[
        str, typer.Option("--out", help="Directory to write the calibration report JSON to")
    ] = "eval_results/calibration",
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")
    ] = False,
    env_file: Annotated[
        str, typer.Option("--env-file", "-e", help="Path to custom .env file (default: .env)")
    ] = ".env",
):
    """
    Score a calibration set with an llm_judge metric and report agreement with human
    labels (MAE + Spearman correlation). Mirrors eval_harness_improvement_spec.md's
    FIX-4. Exits non-zero if correlation falls below --threshold -- an uncalibrated
    judge should not silently pass.
    """
    if os.path.exists(env_file):
        if verbose:
            print(f"Loading .env file from: {env_file}")
        load_dotenv(dotenv_path=env_file, verbose=verbose)
    env_variables = dict(os.environ)
    config = load_config(input_config, context=env_variables)
    context = pre_run_config(config, compile_only=True, verbose=verbose)
    base_config: BaseConfig = context["config"]

    eval_suites = {s.name: s for s in (base_config.eval_suites or [])}
    if suite not in eval_suites:
        print(f"[bold red]Error:[/bold red] eval suite '{suite}' not found in config.")
        raise typer.Exit(1)
    metric_config = next(
        (
            mc
            for mc in eval_suites[suite].metrics
            if mc.type == "llm_judge" and (mc.name or mc.type) == metric_name
        ),
        None,
    )
    if metric_config is None:
        print(
            f"[bold red]Error:[/bold red] no llm_judge metric named '{metric_name}' "
            f"found in suite '{suite}'."
        )
        raise typer.Exit(1)

    report = asyncio.run(calibrate_judge(turns, labels, metric_config, correlation_threshold=threshold))

    os.makedirs(out, exist_ok=True)
    report_path = os.path.join(out, f"{report.judge_prompt_version}.json")
    with open(report_path, "w") as f:
        json.dump(report.__dict__, f, indent=2)

    print(
        f"criterion={report.criterion} judge_prompt_version={report.judge_prompt_version} "
        f"n={report.n} mae={report.mae:.3f} spearman={report.spearman_correlation:.3f}"
    )
    print(f"Report written to {report_path}")

    if report.below_threshold:
        print(
            f"[bold red]Judge is not calibrated[/bold red] for criterion '{report.criterion}' "
            f"(correlation {report.spearman_correlation:.3f} < threshold {threshold})."
        )
        raise typer.Exit(1)
    print(f"[green]Judge is calibrated[/green] for criterion '{report.criterion}'.")


if __name__ == "__main__":
    app()
