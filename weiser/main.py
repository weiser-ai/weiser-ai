import typer

from typing_extensions import Annotated

from weiser.loader import export_results
from weiser.loader.config import load_config
from weiser.runner import pre_run_config, run_checks

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
    context = pre_run_config(config, verbose)
    results = run_checks(context['config'], context['connections'], verbose)
    export_results(results, config)
    typer.echo("Finished Run")


@app.command()
def load():
    """
    Load the portal gun
    """
    typer.echo("Loading portal gun")


if __name__ == "__main__":
    app()
