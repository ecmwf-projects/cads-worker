import os
import random
import urllib
from typing import Annotated, Self

import dask.utils
import fsspec
import structlog
import yaml
from pydantic import BaseModel, BeforeValidator, Field, NonNegativeFloat, NonNegativeInt

LOGGER = structlog.get_logger(__name__)


def get_env_max_size() -> int:
    return dask.utils.parse_bytes(os.getenv("MAX_SIZE", "1GB"))


class DataVolumeConfig(BaseModel):
    weight: NonNegativeFloat = 1
    max_size: Annotated[NonNegativeInt, BeforeValidator(dask.utils.parse_bytes)] = (
        Field(default_factory=get_env_max_size)
    )


class DataVolumes(BaseModel):
    volumes: dict[str, DataVolumeConfig]

    def filter_available_volumes(self) -> dict[str, DataVolumeConfig]:
        available_volumes = {}
        for volume in self.volumes:
            parsed = urllib.parse.urlparse(volume)
            fs = fsspec.filesystem(parsed.scheme)
            if (
                isinstance(fs, fsspec.implementations.local.LocalFileSystem)
                and parsed.path.startswith("/")
                and not os.path.ismount(f"/{parsed.path.split('/')[1]}")
            ):
                LOGGER.warning(f"Volume {volume} is not available. Skipping it.")
                continue
            available_volumes[volume] = self.volumes[volume]
        return available_volumes

    def get_random_volume(self) -> str:
        available_volumes = self.filter_available_volumes()
        (volume,) = random.choices(
            list(available_volumes),
            weights=[config.weight for config in available_volumes.values()],
            k=1,
        )
        return volume

    @classmethod
    def from_yaml(cls, path: str | None = None) -> Self:
        if path is None:
            path = os.environ["DATA_VOLUMES_CONFIG"]

        with open(path) as f:
            raw_dict = yaml.safe_load(f)
        return cls(
            volumes={
                k: DataVolumeConfig(**v) if v else DataVolumeConfig()
                for k, v in raw_dict.items()
            }
        )
