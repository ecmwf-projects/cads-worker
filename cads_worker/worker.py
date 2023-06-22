import os
import tempfile
from typing import Any

import cacholote
import distributed.worker
import structlog

from . import config

config.configure_logger()

LOGGER = structlog.get_logger(__name__)
cacholote.config.set(logger=LOGGER)


def submit_workflow(
    entry_point: str,
    setup_code: str | None = None,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> int:
    from cads_adaptors.tools import adaptor_tools

    job_id = distributed.worker.thread_state.key  # type: ignore
    structlog.contextvars.bind_contextvars(event_type="DATASET_COMPUTE", job_id=job_id)
    LOGGER.info("Processing job", job_id=job_id)
    form = kwargs.get("form", {})
    config = kwargs.get("config", {})
    request = kwargs.get("request", {})
    adaptor_class = adaptor_tools.get_adaptor_class(entry_point, setup_code)
    adaptor = adaptor_class(form=form, **config)
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            result = cacholote.cacheable(adaptor.retrieve)(request=request)
        except Exception:
            LOGGER.exception(job_id=job_id)
            raise
        finally:
            os.chdir(cwd)

    return result.id  # type: ignore
