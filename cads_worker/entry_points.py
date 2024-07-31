import datetime
import os
from typing import Annotated

import cacholote
import structlog
import typer
from typer import Option

from . import config, utils

config.configure_logger()
LOGGER = structlog.get_logger(__name__)
cacholote.config.set(logger=LOGGER)


def _cache_cleaner() -> None:
    max_size = int(os.getenv("MAX_SIZE", 1_000_000_000))
    method = os.getenv("METHOD", "LRU")
    delete_unknown_files = bool(os.getenv("DELETE_UNKNOWN_FILES", True))
    lock_validity_period = float(os.getenv("LOCK_VALIDITY_PERIOD", 60 * 60 * 24))
    for cache_files_urlpath in utils.parse_data_volumes_config():
        cacholote.config.set(cache_files_urlpath=cache_files_urlpath)
        LOGGER.info(
            "Running cache cleaner",
            max_size=max_size,
            method=method,
            delete_unknown_files=delete_unknown_files,
            lock_validity_period=lock_validity_period,
            cache_files_urlpath=cache_files_urlpath,
        )
        try:
            cacholote.clean_cache_files(
                maxsize=max_size,
                method=method,  # type: ignore[arg-type] # let cacholote handle it
                delete_unknown_files=delete_unknown_files,
                lock_validity_period=lock_validity_period,
            )
        except Exception:
            LOGGER.exception("cache_cleaner crashed")
            raise


def _add_tzinfo(timestamp: datetime.datetime) -> datetime.datetime:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
    return timestamp


def _expire_cache_entries(
    before: Annotated[
        datetime.datetime,
        Option(help="Expire entries created before this date"),
    ],
    after: Annotated[
        datetime.datetime,
        Option(help="Expire entries created after this date"),
    ],
    collection_id: Annotated[list[str], Option(help="Collection ID to expire")] = [],
    all_collections: Annotated[
        bool, Option("--all-collections", help="Expire all collections")
    ] = False,
) -> int:
    """Expire cache entries."""
    if (all_collections and collection_id) or not (all_collections or collection_id):
        raise ValueError(
            "Either '--collection-id' or '--all-collections' must be chosen, but not both."
        )

    count = cacholote.expire_cache_entries(
        tags=None if all_collections else collection_id,
        before=_add_tzinfo(before),
        after=_add_tzinfo(after),
    )
    typer.echo(f"Number of entries expired: {count}")
    return count


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)


def expire_cache_entries() -> None:
    typer.run(_expire_cache_entries)
