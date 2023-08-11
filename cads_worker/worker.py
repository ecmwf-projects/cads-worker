import os
import tempfile
from typing import Any

import distributed.worker
import structlog

import cacholote

from . import config

config.configure_logger()

LOGGER = structlog.get_logger(__name__)
cacholote.config.set(logger=LOGGER)


def submit_workflow(
    entry_point: str,
    setup_code: str | None = None,
    request: dict[str, Any] = {},
    config: dict[str, Any] = {},
    form: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> int:
    import cads_adaptors

    job_id = distributed.worker.thread_state.key  # type: ignore
    structlog.contextvars.bind_contextvars(event_type="DATASET_COMPUTE", job_id=job_id)
    LOGGER.info("Processing job", job_id=job_id)
    adaptor_class = cads_adaptors.get_adaptor_class(entry_point, setup_code)
    adaptor = adaptor_class(form=form, **config)
    context = cads_adaptors.adaptor.Context()
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            result = cacholote.cacheable(adaptor.retrieve)(request=request, context=context)
        except Exception:
            distributed.worker.get_worker().log_event(topic=job_id, msg=context.stdout)
            LOGGER.info(context.stdout, job_id=job_id)
            LOGGER.exception(job_id=job_id)
            raise
        finally:
            os.chdir(cwd)

    distributed.worker.get_worker().log_event(topic=job_id, msg=context.stdout)
    LOGGER.info(context.stdout, job_id=job_id)
    return result.id  # type: ignore
