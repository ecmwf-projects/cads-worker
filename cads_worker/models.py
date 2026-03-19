import os
import random
from typing import Self

import yaml
from pydantic import BaseModel, Field, NonNegativeFloat, NonNegativeInt


def get_env_max_size() -> int:
    return int(os.getenv("MAX_SIZE", "1_000_000_000"))


class DataVolumeConfig(BaseModel):
    weight: NonNegativeFloat = 1
    max_size: NonNegativeInt = Field(default_factory=get_env_max_size)


class DataVolumes(BaseModel):
    volumes: dict[str, DataVolumeConfig]

    def get_random_volume(self) -> str:
        (volume,) = random.choices(
            list(self.volumes),
            weights=[config.weight for config in self.volumes.values()],
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
