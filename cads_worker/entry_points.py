import logging
import os
import time
from datetime import datetime

import cacholote
import typer


def cache_cleaner() -> None:
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
        logging.warning(
            f"Running cache cleaner {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        try:
            cacholote.clean_cache_files(
                maxsize=os.environ.get("MAX_SIZE", 200_000_000),
                method=os.environ.get("METHOD", "LRU"),
            )
        except Exception as ex:
            logging.exception(f"cache_cleaner crashed: {ex!r}")


def main() -> None:
    typer.run(cache_cleaner())
