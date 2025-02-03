import datetime
import os
from typing import Annotated, TypedDict

import cacholote
import structlog
import typer
from typer import Option

from . import config, utils

config.configure_logger()
LOGGER = structlog.get_logger(__name__)
cacholote.config.set(logger=LOGGER)


def strtobool(value: str) -> bool:
    if value.lower() in ("y", "yes", "t", "true", "on", "1"):
        return True
    if value.lower() in ("n", "no", "f", "false", "off", "0"):
        return False
    raise ValueError(f"invalid truth value {value!r}")


class CleanerKwargs(TypedDict):
    maxsize: int
    method: str
    delete_unknown_files: bool
    lock_validity_period: float
    use_database: bool
    depth: int
    partition_size: int | None
    partition_sleep: int


def _cache_cleaner() -> None:
    use_database = strtobool(os.environ.get("USE_DATABASE", "1"))
    partition_size = os.getenv("PARTITION_SIZE")
    cleaner_kwargs = CleanerKwargs(
        maxsize=int(os.environ.get("MAX_SIZE", 1_000_000_000)),
        method=os.environ.get("METHOD", "LRU"),
        delete_unknown_files=not use_database,
        lock_validity_period=float(os.environ.get("LOCK_VALIDITY_PERIOD", 86400)),
        use_database=use_database,
        depth=int(os.getenv("CACHE_DEPTH", 2)),
        partition_size=partition_size
        if partition_size is None
        else int(partition_size),
        partition_sleep=int(os.getenv("PARTITION_SLEEP", 0)),
    )
    for cache_files_urlpath in utils.parse_data_volumes_config():
        cacholote.config.set(cache_files_urlpath=cache_files_urlpath)
        LOGGER.info(
            "Running cache cleaner",
            cache_files_urlpath=cache_files_urlpath,
            **cleaner_kwargs,
        )
        try:
            cacholote.clean_cache_files(**cleaner_kwargs)
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
    collection_id: Annotated[
        list[str],
        Option(help="Collection ID to expire"),
    ] = [],
    all_collections: Annotated[
        bool,
        Option(help="Expire all collections"),
    ] = False,
    delete: Annotated[
        bool,
        Option(help="Delete entries to expire"),
    ] = False,
    partition_size: Annotated[
        int | None,
        Option(help="Group entries to expire into partitions of this size"),
    ] = None,
    partition_sleep: Annotated[
        float,
        Option(
            help="Sleep duration after processing each partition (seconds)",
        ),
    ] = 0,
    dry_run: Annotated[
        bool,
        Option(
            help="Perform a trial run that doesn't make any changes",
        ),
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
        partition_size=partition_size,
        partition_sleep=partition_sleep,
        dry_run=dry_run,
    )
    typer.echo(f"Number of entries {'to expire' if dry_run else 'expired'}: {count}")
    return count


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)


def expire_cache_entries() -> None:
    typer.run(_expire_cache_entries)
