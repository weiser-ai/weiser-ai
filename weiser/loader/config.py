import glob
import typer
import yaml

from jinja2 import Environment, BaseLoader
from os.path import abspath, dirname, join

visited_path = {}


def update_namespace(namespace, new_file, verbose):

    if namespace is None:
        return new_file
    for key, value in new_file.items():
        if key in namespace and key in ("checks", "datasources", "connections"):
            namespace[key] = namespace[key] + new_file[key]
        elif key in namespace and key in ("includes"):  # remove duplicates
            namespace[key] = list(set(namespace[key] + new_file[key]))
        elif key in ("checks", "datasources", "includes", "connections"):
            namespace[key] = new_file[key]
        elif key in ("extras"):
            pass  # ignored keys
        elif verbose:
            typer.echo(f"Key not supported yet: {key}")
    return namespace


def load_config(
    config_path: str,
    namespace: dict = None,
    context: dict = None,
    visited_path: dict = None,
    verbose: bool = True,
) -> dict:
    if visited_path is None:
        visited_path = {}

    file_paths = glob.glob(config_path)
    if verbose:
        typer.echo(f"Walking Paths: {file_paths}")
    for file_path in file_paths:
        if file_path in visited_path:
            continue
        visited_path[file_path] = True
        with open(file_path, "r") as stream:
            if context:
                data_loaded = yaml.safe_load(
                    Environment(loader=BaseLoader())
                    .from_string(stream.read())
                    .render(context)
                )
            else:
                data_loaded = yaml.safe_load(stream)
            if verbose:
                typer.echo(f"Context: {context}")
                typer.echo(f"Rendered YAML: {data_loaded}")

        if "includes" in data_loaded:
            for included_path in data_loaded["includes"]:
                if (
                    namespace
                    and "includes" in namespace
                    and included_path in namespace["includes"]
                ):
                    continue
                if included_path.startswith("/"):
                    included_path = included_path[1:]
                root_dir = dirname(abspath(file_path))
                namespace = load_config(
                    join(root_dir, included_path),
                    namespace=namespace,
                    visited_path=visited_path,
                    verbose=verbose,
                )
        namespace = update_namespace(namespace, data_loaded, verbose)
    return namespace
