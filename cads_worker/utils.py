import contextlib
import os
import pathlib
import tempfile
from collections.abc import Iterator


def parse_data_volumes_config(path: str | None = None) -> list[str]:
    if path is None:
        path = os.environ["DATA_VOLUMES_CONFIG"]

    with open(path) as fp:
        return [os.path.expandvars(line.rstrip("\n")) for line in fp]


@contextlib.contextmanager
def enter_tmp_working_dir() -> Iterator[str]:
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            yield os.getcwd()
        finally:
            os.chdir(old_cwd)


@contextlib.contextmanager
def make_cache_tmp_path(base_dir: str) -> Iterator[pathlib.Path]:
    with tempfile.TemporaryDirectory(dir=base_dir) as tmpdir:
        cache_tmp_path = pathlib.Path(tmpdir)
        cache_tmp_path.with_suffix(".lock").touch()
        try:
            yield cache_tmp_path
        finally:
            cache_tmp_path.with_suffix(".lock").unlink(missing_ok=True)
