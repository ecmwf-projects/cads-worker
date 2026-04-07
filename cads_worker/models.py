import os
import random
from typing import Annotated, Self

import dask.utils
import yaml
from pydantic import BaseModel, BeforeValidator, Field, NonNegativeFloat, NonNegativeInt


def get_env_max_size() -> int:
    return dask.utils.parse_bytes(os.getenv("MAX_SIZE", "1GB"))


class DataVolumeConfig(BaseModel):
    weight: NonNegativeFloat = 1
    max_size: Annotated[NonNegativeInt, BeforeValidator(dask.utils.parse_bytes)] = (
        Field(default_factory=get_env_max_size)
    )


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
