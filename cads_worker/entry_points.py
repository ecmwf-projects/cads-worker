import os

import cacholote
import structlog
import typer

from . import config

config.configure_logger()
LOGGER = structlog.get_logger(__name__)


def _cache_cleaner() -> None:
    LOGGER.info("Running cache cleaner")
    try:
        cacholote.clean_cache_files(
            maxsize=int(os.environ.get("MAX_SIZE", 1_000_000_000)),
            method=os.environ.get("METHOD", "LRU"),  # type: ignore[arg-type] # let cacholote handle it
            logger=LOGGER,
        )
    except Exception:
        LOGGER.exception("cache_cleaner crashed")


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)
