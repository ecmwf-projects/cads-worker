import os

import cacholote
import structlog
import typer

from . import config

config.configure_logger()
LOGGER = structlog.get_logger(__name__)


def _cache_cleaner() -> None:
    max_size = int(os.environ.get("MAX_SIZE", 1_000_000_000))
    cache_bucket = os.environ.get("CACHE_BUCKET", None)
    LOGGER.info("Running cache cleaner", max_size=max_size, cache_bucket=cache_bucket)
    try:
        cacholote.clean_cache_files(
            maxsize=max_size,
            method=os.environ.get("METHOD", "LRU"),  # type: ignore[arg-type] # let cacholote handle it
            logger=LOGGER,
            delete_unknown_files=bool(os.environ.get("DELETE_UNKNOWN_FILES", 1)),
            lock_validity_period=float(os.environ.get("LOCK_VALIDITY_PERIOD", 60 * 60 * 24))
        )
    except Exception:
        LOGGER.exception("cache_cleaner crashed")


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)
