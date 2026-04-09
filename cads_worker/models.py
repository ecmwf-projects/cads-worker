import os
import random
import urllib
from typing import Self

import structlog
import yaml
from pydantic import BaseModel, Field, NonNegativeFloat, NonNegativeInt

LOGGER = structlog.get_logger(__name__)


def get_env_max_size() -> int:
    return int(os.getenv("MAX_SIZE", "1_000_000_000"))


class DataVolumeConfig(BaseModel):
    weight: NonNegativeFloat = 1
    max_size: NonNegativeInt = Field(default_factory=get_env_max_size)


class DataVolumes(BaseModel):
    volumes: dict[str, DataVolumeConfig]

    def filter_available_volumes(self) -> dict[str, DataVolumeConfig]:
        available_volumes = {}
        for volume in self.volumes:
            parsed = urllib.parse.urlparse(volume)
            if parsed.scheme == "s3" or os.path.ismount(
                f"/{parsed.path.split('/')[1]}"
            ):
                available_volumes[volume] = self.volumes[volume]
            elif self.volumes[volume].weight > 0:
                LOGGER.warning(f"Volume {volume} is not available. Skipping it.")
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
