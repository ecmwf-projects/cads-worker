import os
import random

from pydantic import BaseModel, Field, NonNegativeInt


def get_env_max_size() -> int:
    return int(os.getenv("MAX_SIZE", "1_000_000_000"))


class DataVolumeConfig(BaseModel):
    weight: NonNegativeInt = 1
    max_size: NonNegativeInt = Field(default_factory=get_env_max_size)


class DataVolumes(BaseModel):
    volumes: dict[str, DataVolumeConfig]

    def get_random_volume(self) -> str:
        choices = []
        for volume, config in self.volumes.items():
            choices.extend([volume] * config.weight)
        return random.choice(choices)
