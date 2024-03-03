import glob
import typer
import yaml

from os.path import abspath, dirname, join
from pathlib import Path

visited_path = {}


def update_namespace(namespace, new_file):

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
        else:
            typer.echo(f"Key not supported yet: {key}")
    return namespace


def load_config(config_path, namespace=None):
    file_paths = glob.glob(config_path)
    for file_path in file_paths:
        if file_path in visited_path:
            continue
        visited_path[file_path] = True
        with open(file_path, "r") as stream:
            data_loaded = yaml.safe_load(stream)

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
                    join(root_dir, included_path), namespace=namespace
                )
        namespace = update_namespace(namespace, data_loaded)
    return namespace
