import json
import logging
import os
from typing import Any


def submit_workflow(
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> dict[str, Any] | list[dict[str, Any]]:
    import cacholote

    cacholote.config.set(
        cache_files_urlpath=os.path.join(f"s3://{os.environ['CACHE_BUCKET']}"),
        cache_files_storage_options=dict(
            key=os.environ["STORAGE_ADMIN"],
            secret=os.environ["STORAGE_PASSWORD"],
            client_kwargs={"endpoint_url": os.environ["OBJECT_STORAGE_URL"]},
        ),
    )
    exec(setup_code, globals())
    logging.info(f"Submitting: {metadata['process_id']}")
    results = eval(f"{entry_point}(metadata=metadata, **kwargs)")
    results = json.loads(cacholote.encode.dumps(results))
    results["href"] = f"{os.environ['STORAGE_API_URL']}/{results['file:local_path']}"
    results["xarray:storage_options"] = {}
    return json.dumps(results)
