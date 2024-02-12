import functools
import os
import socket
import tempfile
from typing import Any

import cacholote
import cads_broker.database
import distributed.worker
import structlog

from . import config

config.configure_logger()

LOGGER = structlog.get_logger(__name__)
cacholote.config.set(logger=LOGGER)


def ensure_session(func):
    @functools.wraps(func)
    def wrapper(self, *args, session=None, **kwargs):
        close_session = False
        if session is None:
            session = self.session_maker()
            close_session = True
        func(self, *args, session=session, **kwargs)
        if close_session:
            session.close()

    return wrapper


class Context:
    def __init__(self, job_id: str, logger: Any):
        self.job_id = job_id
        self.logger = logger

    @ensure_session
    def add_user_visible_log(self, message: str, session: Any = None) -> None:
        cads_broker.database.add_event(
            event_type="user_visible_log",
            request_uid=self.job_id,
            message=message,
            session=session,
        )

    @ensure_session
    def add_user_visible_error(self, message: str, session: Any = None) -> None:
        cads_broker.database.add_event(
            event_type="user_visible_error",
            request_uid=self.job_id,
            message=message,
            session=session,
        )

    @ensure_session
    def add_stdout(self, message: str, session: Any = None) -> None:
        self.logger.info(message)
        cads_broker.database.add_event(
            event_type="stdout",
            request_uid=self.job_id,
            message=message,
            session=session,
        )

    @ensure_session
    def add_stderr(self, message: str, session: Any = None) -> None:
        self.logger.exception(message)
        cads_broker.database.add_event(
            event_type="stderr",
            request_uid=self.job_id,
            message=message,
            session=session,
        )

    @functools.cached_property
    def session_maker(self) -> Any:
        return cads_broker.database.ensure_session_obj(None)


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
    logger = LOGGER.bind(job_id=job_id)
    context = Context(job_id=job_id, logger=logger)
    with context.session_maker() as session:
        cads_broker.database.add_event(
            event_type="worker_name",
            request_uid=job_id,
            message=socket.gethostname(),
            session=session,
        )
    structlog.contextvars.bind_contextvars(event_type="DATASET_COMPUTE", job_id=job_id)
    logger.info("Processing job", job_id=job_id)
    # FIXME: Temporary hack to use the same session as the context
    cacholote.database.ENGINE = context.session_maker.kw["bind"]
    cacholote.database.SESSIONMAKER = context.session_maker
    adaptor_class = cads_adaptors.get_adaptor_class(entry_point, setup_code)
    adaptor = adaptor_class(form=form, context=context, **config)
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            result = cacholote.cacheable(adaptor.retrieve)(request=request)
        except Exception:
            logger.exception(job_id=job_id, event_type="EXCEPTION")
            raise
        finally:
            os.chdir(cwd)

    fs, _ = cacholote.utils.get_cache_files_fs_dirname()
    fs.chmod(result.result["args"][0]["file:local_path"], acl="public-read")
    return result.id  # type: ignore
