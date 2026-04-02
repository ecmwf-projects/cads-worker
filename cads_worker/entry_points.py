import datetime
import os
from typing import Annotated, Literal, TypedDict

import cacholote
import cads_broker.object_storage
import structlog
import typer
from typer import Option

from . import config, models

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
    method: Literal["LRU", "LFU"]
    delete_unknown_files: bool
    lock_validity_period: float
    use_database: bool
    depth: int
    batch_size: int | None
    batch_delay: float


def _get_cache_method() -> Literal["LRU", "LFU"]:
    match os.getenv("METHOD", "LRU"):
        case "LRU":
            return "LRU"
        case "LFU":
            return "LFU"
        case method:
            raise NotImplementedError(f"{method=}")


def _cache_cleaner() -> None:
    use_database = strtobool(os.environ.get("USE_DATABASE", "1"))
    volumes = models.DataVolumes.from_yaml().volumes
    for cache_files_urlpath, volume_config in volumes.items():
        cleaner_kwargs = CleanerKwargs(
            maxsize=volume_config.max_size,
            method=_get_cache_method(),
            delete_unknown_files=not use_database,
            lock_validity_period=float(os.environ.get("LOCK_VALIDITY_PERIOD", 86400)),
            use_database=use_database,
            depth=int(os.getenv("CACHE_DEPTH", 2)),
            batch_size=int(os.getenv("BATCH_SIZE", 0)) or None,
            batch_delay=float(os.getenv("BATCH_DELAY", 0)),
        )
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
    collection_id: Annotated[list[str], Option(help="Collection ID to expire")] = [],
    all_collections: Annotated[
        bool, Option("--all-collections", help="Expire all collections")
    ] = False,
    delete: Annotated[
        bool, Option("--delete", help="Delete entries to expire")
    ] = False,
    batch_size: Annotated[
        int | None,
        Option(help="Number of entries to process in each batch"),
    ] = None,
    batch_delay: Annotated[
        float,
        Option(
            help="Delay in seconds between processing batches",
        ),
    ] = 0,
    dry_run: Annotated[
        bool,
        Option("--dry-run", help="Perform a trial run that doesn't make any changes"),
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
        batch_size=batch_size,
        batch_delay=batch_delay,
        dry_run=dry_run,
    )
    typer.echo(f"Number of entries {'to expire' if dry_run else 'expired'}: {count}")
    return count


def _init_buckets() -> None:
    object_storage_url = os.environ["OBJECT_STORAGE_URL"]
    storage_kws: dict[str, str] = {
        "aws_access_key_id": os.environ["STORAGE_ADMIN"],
        "aws_secret_access_key": os.environ["STORAGE_PASSWORD"],
    }
    LOGGER.info("Initializing buckets", object_storage_url=object_storage_url)
    data_volumes = models.DataVolumes.from_yaml().volumes
    for data_volume in data_volumes:
        if data_volume.startswith("s3://"):
            LOGGER.info("Initializing bucket", data_volume=data_volume)
            cads_broker.object_storage.create_download_bucket(
                data_volume, object_storage_url, **storage_kws
            )
    LOGGER.info("Buckets initialized")


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)


def expire_cache_entries() -> None:
    typer.run(_expire_cache_entries)


def init_buckets() -> None:
    typer.run(_init_buckets)
