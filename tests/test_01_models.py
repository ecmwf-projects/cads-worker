import pathlib

import pytest

from cads_worker.models import DataVolumes


def test_data_volumes_from_yaml(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_SIZE", "10")
    data_volumes_config = tmp_path / "data-volumes.yaml"
    data_volumes_config.write_text(
        "s3://foo:\ns3://bar:\n  weight: 0\n  max_size: 20\n"
    )

    volumes = DataVolumes.from_yaml(str(data_volumes_config))
    assert volumes.model_dump() == {
        "volumes": {
            "s3://foo": {"weight": 1, "max_size": 10},
            "s3://bar": {"weight": 0, "max_size": 20},
        }
    }

    assert volumes.get_random_volume() == "s3://foo"

    data_volumes_config.write_text(
        "s3://foo:\n  weight: 1\ncci1:///bar:\n  weight: 100_000\n"
    )

    volumes = DataVolumes.from_yaml(str(data_volumes_config))
    assert volumes.model_dump() == {
        "volumes": {
            "s3://foo": {"weight": 1, "max_size": 10},
            "cci1:///bar": {"weight": 100_000, "max_size": 10},
        }
    }

    assert volumes.get_random_volume() == "s3://foo"
