import os


def parse_data_nodes(path: str | None = None) -> list[str]:
    if path is None:
        path = os.environ["DATA_NODES_CONFIG"]

    with open(path) as fp:
        return fp.read().splitlines()
