import os
import time

import cacholote
import typer


# app = typer.Typer()


def cache_cleaner():
    with cacholote.config.set(
        cache_files_urlpath=f"s3://{os.environ['CACHE_BUCKET']}",
        cache_files_urlpath_readonly=f"{os.environ['STORAGE_API_URL']}/{os.environ['CACHE_BUCKET']}",
        cache_files_storage_options=dict(
            key=os.environ["STORAGE_ADMIN"],
            secret=os.environ["STORAGE_PASSWORD"],
            client_kwargs={"endpoint_url": os.environ["OBJECT_STORAGE_URL"]},
            asynchronous=False,
        ),
        io_delete_original=True,
        raise_all_encoding_errors=True,
        cache_db_urlpath=f"postgresql://{os.environ['COMPUTE_DB_USER']}"
        f":{os.environ['COMPUTE_DB_PASSWORD']}@{os.environ['COMPUTE_DB_HOST']}"
        f"/{os.environ['COMPUTE_DB_NAME']}",
    ):
        while True:
            print(os.getcwd())
            cacholote.clean.clean_cache_files(
                maxsize=os.environ.get("MAX_SIZE", 200_000_000),
                method=os.environ.get("METHOD", "LRU"),
            )
            time.sleep(1)


def main():
    typer.run(cache_cleaner())
