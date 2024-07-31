import os


def parse_data_volumes_config(path: str | None = None) -> list[str]:
    if path is None:
        path = os.environ["DATA_VOLUMES_CONFIG"]

    with open(path) as fp:
        return fp.read().splitlines()
