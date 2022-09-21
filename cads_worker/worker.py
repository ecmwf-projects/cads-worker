import json
import logging
import os
from typing import Any

import cacholote


def submit_workflow(
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> str:

    exec(setup_code, globals())
    logging.info(f"Submitting: {metadata['process_id']}")
    func = eval(entry_point)
    with cacholote.config.set(
        cache_files_urlpath=f"s3://{os.environ['CACHE_BUCKET']}",
        cache_files_urlpath_readonly=f"{os.environ['STORAGE_API_URL']}/{os.environ['CACHE_BUCKET']}",
        cache_files_storage_options=dict(
            key=os.environ["STORAGE_ADMIN"],
            secret=os.environ["STORAGE_PASSWORD"],
            client_kwargs={"endpoint_url": os.environ["OBJECT_STORAGE_URL"]},
        ),
        io_delete_original=True,
    ):
        func(metadata=metadata, **kwargs)
        cache_key = cacholote.hexdigestify_python_call(
            func, metadata=metadata, **kwargs
        )
        cache_dict = json.loads(cacholote.config.SETTINGS["cache_store"][cache_key])
    public_dict = {
        k: {} if k.endswith(":storage_options") else v for k, v in cache_dict.items()
    }
    return json.dumps(public_dict)
