import os
import pathlib
import tempfile

import pytest

from cads_worker import utils


def test_utils_parse_data_volumes_config(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FOO", "foo")
    monkeypatch.setenv("BAR", "bar")
    data_volumes_config = tmp_path / "data-volumes.config"
    data_volumes_config.write_text("$FOO\n${BAR}")
    assert utils.parse_data_volumes_config(str(data_volumes_config)) == ["foo", "bar"]

    monkeypatch.setenv("DATA_VOLUMES_CONFIG", str(data_volumes_config))
    assert utils.parse_data_volumes_config(None) == ["foo", "bar"]


def test_utils_enter_tmp_working_dir() -> None:
    with utils.enter_tmp_working_dir() as tmp_working_dir:
        assert os.getcwd() == tmp_working_dir
        assert os.path.dirname(tmp_working_dir) == os.path.realpath(
            tempfile.gettempdir()
        )
    assert not os.path.exists(tmp_working_dir)


def test_utils_make_cache_tmp_path(tmp_path: pathlib.Path) -> None:
    with utils.make_cache_tmp_path(str(tmp_path)) as cache_tmp_path:
        assert cache_tmp_path.parent == tmp_path
        assert cache_tmp_path.with_suffix(".lock").exists()
    assert not cache_tmp_path.exists()
    assert not cache_tmp_path.with_suffix(".lock").exists()
