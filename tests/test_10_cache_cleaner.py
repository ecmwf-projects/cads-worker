import os
import pathlib
import subprocess

import cacholote
import pytest


def test_cache_cleaner(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # create dummy file
    dummy_path = tmp_path / "dummy.txt"
    dummy_path.write_text("dummy")

    # copy file to cache
    cached_open = cacholote.cacheable(open)
    cache_db_urlpath = f"sqlite:///{tmp_path / 'cacholote.db'}"
    cache_files_urlpath = str(tmp_path / "cache_files")
    with cacholote.config.set(
        cache_db_urlpath=cache_db_urlpath, cache_files_urlpath=cache_files_urlpath
    ):
        cached_path = pathlib.Path(cached_open(dummy_path).name)
    assert cached_path.exists()

    # create data nodes config
    data_volumes_config = tmp_path / "data_nodes"
    data_volumes_config.write_text(cache_files_urlpath)
    monkeypatch.setenv("DATA_VOLUMES_CONFIG", str(data_volumes_config))

    # clean cache
    cache_env = os.environ.copy()
    cache_env.update(
        {
            "MAX_SIZE": "0",
            "CACHOLOTE_CACHE_DB_URLPATH": cache_db_urlpath,
            "CACHOLOTE_CACHE_FILES_URLPATH": cache_files_urlpath,
        }
    )
    subprocess.run("cache-cleaner", check=True, env=cache_env)
    assert not cached_path.exists()
