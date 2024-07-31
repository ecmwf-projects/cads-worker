import pathlib

import pytest

from cads_worker import utils


def test_utils_parse_data_nodes(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_nodes_path = tmp_path / "data_nodes"
    data_nodes_path.write_text("foo\nbar")
    assert utils.parse_data_nodes(str(data_nodes_path)) == ["foo", "bar"]

    monkeypatch.setenv("DATA_NODES_CONFIG", str(data_nodes_path))
    assert utils.parse_data_nodes(None) == ["foo", "bar"]
