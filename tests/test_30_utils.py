import pathlib

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
