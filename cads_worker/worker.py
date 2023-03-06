import os
import tempfile
from typing import Any

import cacholote  # noqa: F401
import distributed.worker
import structlog
from cads_adaptors import adaptor_utils


from . import config

config.configure_logger()

LOGGER = structlog.get_logger(__name__)


def submit_workflow(
    entry_point: str,
    setup_code: str | None = None,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> int:
    job_id = distributed.worker.thread_state.key  # type: ignore
    LOGGER.info(f"Processing job: {job_id}.", job_id=job_id)
    config = kwargs.get("config", {})
    request = kwargs.get("request", {})
    adaptor_class = adaptor_utils.get_adaptor_class(entry_point, setup_code)
    adaptor = adaptor_class(**config)
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            result = adaptor.retrieve(request=request)
        except Exception:
            LOGGER.exception(job_id=job_id)
            raise
        finally:
            os.chdir(cwd)

    return result.id  # type: ignore
