import pathlib

import pytest

from cads_worker.models import DataVolumes


def test_data_volumes_from_yaml(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_SIZE", "10MiB")
    data_volumes_config = tmp_path / "data-volumes.yaml"
    data_volumes_config.write_text("\nfoo:\nbar:\n  weight: 0\n  max_size: 10Mb\n")

    volumes = DataVolumes.from_yaml(str(data_volumes_config))
    assert volumes.model_dump() == {
        "volumes": {
            "foo": {"weight": 1, "max_size": 10_485_760},
            "bar": {"weight": 0, "max_size": 10_000_000},
        }
    }

    assert volumes.get_random_volume() == "foo"
