import typer

from typing_extensions import Annotated

from weiser.loader.export import export_results
from weiser.loader.config import load_config
from weiser.runner import pre_run_config, run_checks, generate_sample_data

app = typer.Typer()

@app.callback()
def callback():
    """
    Awesome Portal Gun
    """


@app.command()
def run(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")] = False):
    """
    Main Command
    """
    config = load_config(input_config)
    context = pre_run_config(config, verbose=verbose)
    results = run_checks(context['run_id'], context['config'], 
                         context['connections'], context['metric_store'], verbose)
    export_results(context['run_id'], context['metric_store'])
    typer.echo("Finished Run")

@app.command()
def compile(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")] = False):
    """
    Main Command
    """
    config = load_config(input_config)
    pre_run_config(config, compile_only=True, verbose=verbose)
    typer.echo("Finished Config compilation")


@app.command()
def sample(
    input_config: Annotated[str, typer.Argument(help="The path for the file to read")],
    check: Annotated[str, typer.Option("--check", "-c", help="Id to populate")],
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Print to stdout the parsed files")] = False):
    """
    Generate sample data based on a check id name.
    """
    config = load_config(input_config)
    context = pre_run_config(config, verbose)
    results = generate_sample_data(check, context['config'], 
                         context['connections'], context['metric_store'], verbose)
    export_results(context['run_id'], config)
    typer.echo("Finished Run")

if __name__ == "__main__":
    app()
