import datetime
import os
from typing import Annotated

import cacholote
import structlog
import typer
from typer import Option

from . import config

config.configure_logger()
LOGGER = structlog.get_logger(__name__)
cacholote.config.set(logger=LOGGER)


def _cache_cleaner() -> None:
    max_size = int(os.environ.get("MAX_SIZE", 1_000_000_000))
    cache_bucket = os.environ.get("CACHE_BUCKET", None)
    LOGGER.info("Running cache cleaner", max_size=max_size, cache_bucket=cache_bucket)
    try:
        cacholote.clean_cache_files(
            maxsize=max_size,
            method=os.environ.get("METHOD", "LRU"),  # type: ignore[arg-type] # let cacholote handle it
            delete_unknown_files=bool(os.environ.get("DELETE_UNKNOWN_FILES", 1)),
            lock_validity_period=float(
                os.environ.get("LOCK_VALIDITY_PERIOD", 60 * 60 * 24)
            ),
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
    delete: Annotated[
        bool, Option("--delete", help="Delete entries to expire")
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
        delete=delete,
    )
    typer.echo(f"Number of entries expired: {count}")
    return count


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)


def expire_cache_entries() -> None:
    typer.run(_expire_cache_entries)
