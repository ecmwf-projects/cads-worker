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
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            result = cacholote.cacheable(adaptor.retrieve)(request=request)
        except Exception:
            distributed.worker.get_worker().log_event(
                topic=f"{job_id}/log",
                msg=adaptor.context.stdout + adaptor.context.stderr,
            )
            distributed.worker.get_worker().log_event(
                topic=f"{job_id}/user_visible_log", msg=adaptor.context.user_visible_log
            )
            LOGGER.info(adaptor.context.stdout, event_type="STDOUT", job_id=job_id)
            LOGGER.info(adaptor.context.stderr, event_type="STDERR", job_id=job_id)
            LOGGER.exception(job_id=job_id, event_type="EXCEPTION")
            raise
        finally:
            os.chdir(cwd)

    distributed.worker.get_worker().log_event(
        topic=f"{job_id}/log", msg=adaptor.context.stdout + adaptor.context.stderr
    )
    distributed.worker.get_worker().log_event(
        topic=f"{job_id}/user_visible_log", msg=adaptor.context.user_visible_log
    )
    fs, _ = cacholote.utils.get_cache_files_fs_dirname()
    fs.chmod(result.result["args"][0]["file:local_path"], acl="public-read")
    LOGGER.info(adaptor.context.stdout, event_type="STDOUT", job_id=job_id)
    return result.id  # type: ignore
