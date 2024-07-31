import pathlib

import pytest

from cads_worker import utils


def test_utils_parse_data_volumes_config(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_volumes_config = tmp_path / "data_nodes"
    data_volumes_config.write_text("foo\nbar")
    assert utils.parse_data_volumes_config(str(data_volumes_config)) == ["foo", "bar"]

    monkeypatch.setenv("DATA_VOLUMES_CONFIG", str(data_volumes_config))
    assert utils.parse_data_volumes_config(None) == ["foo", "bar"]
