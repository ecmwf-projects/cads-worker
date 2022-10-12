import json
import logging
import os
import shutil
import tempfile
import time
from typing import Any

logging.basicConfig(level=logging.INFO)


def submit_workflow(
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> str:
    import cacholote

    exec(setup_code, globals())
    logging.info(f"Submitting: {kwargs}")
    # cache key is computed from function name and kwargs, we add 'setup_code' to kwargs so functions
    # with the same name and with different setup_code have different caches
    kwargs.setdefault("config", {})["__setup_code__"] = setup_code
    func = eval(entry_point)
    with cacholote.config.set(
        cache_files_urlpath=f"s3://{os.environ['CACHE_BUCKET']}",
        cache_files_urlpath_readonly=f"{os.environ['STORAGE_API_URL']}/{os.environ['CACHE_BUCKET']}",
        cache_files_storage_options=dict(
            key=os.environ["STORAGE_ADMIN"],
            secret=os.environ["STORAGE_PASSWORD"],
            client_kwargs={"endpoint_url": os.environ["OBJECT_STORAGE_URL"]},
            asynchronous=False,
        ),
        io_delete_original=True, raise_all_encoding_errors=True
    ):
        cache_key = cacholote.hexdigestify_python_call(
            func, metadata=metadata, **kwargs
        )
        cwd = os.getcwd()
        results_dir = os.path.join(tempfile.gettempdir(), cache_key)
        cwd = os.getcwd()
        # wait for the running process that is writing in the results_dir
        while os.path.exists(results_dir):
            time.sleep(2)
        os.mkdir(results_dir)
        os.chdir(results_dir)
        try:
            func(metadata=metadata, **kwargs)
        finally:
            os.chdir(cwd)
            shutil.rmtree(results_dir)
        cache_dict = json.loads(cacholote.config.SETTINGS["cache_store"][cache_key])
    public_dict = {
        k: {} if k.endswith(":storage_options") else v for k, v in cache_dict.items()
    }
    return json.dumps(public_dict)
