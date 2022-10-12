import contextlib
import json
import logging
import os
import shutil
import tempfile
import time
from typing import Any, Iterator

logging.basicConfig(level=logging.INFO)


try:
    import cacholote
except ImportError:
    # cacholote is not installed on the broker
    pass
else:
    cacholote.config.set(
        cache_files_urlpath=f"s3://{os.environ['CACHE_BUCKET']}",
        cache_files_urlpath_readonly=f"{os.environ['STORAGE_API_URL']}/{os.environ['CACHE_BUCKET']}",
        cache_files_storage_options=dict(
            key=os.environ["STORAGE_ADMIN"],
            secret=os.environ["STORAGE_PASSWORD"],
            client_kwargs={"endpoint_url": os.environ["OBJECT_STORAGE_URL"]},
        ),
        io_delete_original=True,
    )


@contextlib.contextmanager
def temporary_working_directory(path: str) -> Iterator[None]:
    origin = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)
        if os.path.exists(path):
            shutil.rmtree(path)


def submit_workflow(
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> str:

    exec(setup_code, globals())
    logging.info(f"Submitting: {kwargs}")
    # cache key is computed from function name and kwargs, we add 'setup_code' to kwargs so functions
    # with the same name and with different setup_code have different caches
    kwargs.setdefault("config", {})["__setup_code__"] = setup_code
    func = eval(entry_point)
    cache_key = cacholote.hexdigestify_python_call(func, metadata=metadata, **kwargs)
    results_dir = os.path.join(tempfile.gettempdir(), cache_key)
    # wait for the running process that is writing in the results_dir
    while os.path.exists(results_dir):
        time.sleep(2)
    with temporary_working_directory(results_dir):
        func(metadata=metadata, **kwargs)
    cache_dict = json.loads(cacholote.config.SETTINGS["cache_store"][cache_key])
    public_dict = {
        k: {} if k.endswith(":storage_options") else v for k, v in cache_dict.items()
    }
    return json.dumps(public_dict)
