import datetime
import logging
import os

import cacholote
import typer


def _cache_cleaner() -> None:
    with cacholote.config.set(
        cache_files_urlpath=f"s3://{os.environ['CACHE_BUCKET']}",
        cache_files_storage_options=dict(
            key=os.environ["STORAGE_ADMIN"],
            secret=os.environ["STORAGE_PASSWORD"],
            client_kwargs={"endpoint_url": os.environ["OBJECT_STORAGE_URL"]},
            asynchronous=False,
            use_listings_cache=False,
            skip_instance_cache=False,
        ),
        raise_all_encoding_errors=True,
        cache_db_urlpath=f"postgresql://{os.environ['COMPUTE_DB_USER']}"
        f":{os.environ['COMPUTE_DB_PASSWORD']}@{os.environ['COMPUTE_DB_HOST']}"
        f"/{os.environ['COMPUTE_DB_USER']}",
    ):
        logging.warning("Running cache cleaner: %s", datetime.datetime.now())
        try:
            cacholote.clean_cache_files(
                maxsize=int(os.environ.get("MAX_SIZE", 1_000_000_000)),
                method=os.environ.get("METHOD", "LRU"),  # type: ignore[arg-type] # let cacholote handle it
            )
        except Exception:
            logging.exception("cache_cleaner crashed")


def cache_cleaner() -> None:
    typer.run(_cache_cleaner)
