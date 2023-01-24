import datetime
import logging
import os

import cacholote
import typer


def _cache_cleaner() -> None:
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
