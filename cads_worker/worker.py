import contextvars
import os
import tempfile
from typing import Any

import structlog

from . import config

config.configure_logger()

LOGGER = structlog.get_logger(__name__)


def submit_workflow(
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> dict[str, Any]:
    import cacholote

    exec(setup_code, globals())
    LOGGER.info(f"Submitting: {kwargs}")
    # cache key is computed from function name and kwargs, we add 'setup_code' to kwargs so functions
    # with the same name and with different setup_code have different caches
    kwargs.setdefault("config", {})["__setup_code__"] = setup_code
    func = eval(entry_point)
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            func(metadata=metadata, **kwargs, __context__=contextvars.copy_context())
        finally:
            os.chdir(cwd)

    return cacholote.cache.LAST_PRIMARY_KEYS.get()
