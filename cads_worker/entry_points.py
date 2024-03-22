import os

import cacholote
import structlog
import typer

from . import config

config.configure_logger()
LOGGER = structlog.get_logger(__name__)
cacholote.config.set(logger=LOGGER)


def _cache_cleaner() -> None:
    cache_bucket = os.environ.get("CACHE_BUCKET", None)
    max_size = int(os.environ.get("MAX_SIZE", 1_000_000_000))
    method = os.environ.get("METHOD", "LRU")
    delete_unknown_files = bool(os.environ.get("DELETE_UNKNOWN_FILES", 1))
    lock_validity_period = float(os.environ.get("LOCK_VALIDITY_PERIOD", 60 * 60 * 24))
    LOGGER.info("Running cache cleaner", max_size=max_size, cache_bucket=cache_bucket)
    cacholote.clean_cache_files(
        maxsize=max_size,
        method=method,
        delete_unknown_files=delete_unknown_files,
        lock_validity_period=lock_validity_period,
    )


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)
