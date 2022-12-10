import contextvars
import logging
import os
import tempfile
from typing import Any

logging.basicConfig(level=logging.INFO)


def submit_workflow(
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> dict[str, Any]:
    import cacholote

    exec(setup_code, globals())
    logging.info(f"Submitting: {kwargs}")
    # cache key is computed from function name and kwargs, we add 'setup_code' to kwargs so functions
    # with the same name and with different setup_code have different caches
    kwargs.setdefault("config", {})["__setup_code__"] = setup_code
    func = eval(entry_point)
    with cacholote.config.set(
        cache_files_urlpath=f"s3://{os.environ['CACHE_BUCKET']}",
        cache_files_urlpath_readonly=(
            f"{os.environ['STORAGE_API_URL'].rstrip('/')}/{os.environ['CACHE_BUCKET']}"
        ),
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
        f"/{os.environ['COMPUTE_DB_USER']}",
    ):
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                func(contextvars.copy_context(), metadata=metadata, **kwargs)
            finally:
                os.chdir(cwd)

    return cacholote.cache.LAST_PRIMARY_KEYS.get()
