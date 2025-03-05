import datetime
import distutils.util
import functools
import logging
import os
import random
import socket
from typing import Any

import cacholote
import cads_adaptors
import cads_broker.database
import distributed.worker
import structlog
from distributed import get_worker

from . import config, utils

config.configure_logger()

LOGGER = structlog.get_logger(__name__)

WORKER_LOG_LEVEL = os.getenv("WORKER_LOG_LEVEL", "false").upper()
LEVELS_MAPPING = logging.getLevelNamesMapping()
# 60 is above all the levels. it means no log
WORKER_LOG_LEVEL_INT = LEVELS_MAPPING.get(WORKER_LOG_LEVEL, 60)


@functools.lru_cache
def create_session_maker() -> cads_broker.database.sa.orm.sessionmaker:
    return cads_broker.database.ensure_session_obj(None)


def ensure_session(func):
    @functools.wraps(func)
    def wrapper(self, *args, session=None, **kwargs):
        close_session = False
        if session is None:
            session = create_session_maker()()
            close_session = True
        func(self, *args, session=session, **kwargs)
        if close_session:
            session.close()

    return wrapper


class Context(cacholote.config.Context):
    def __init__(
        self,
        job_id: str | None = None,
        logger: Any | None = None,
        write_type: str = "stdout",
    ):
        self.job_id = job_id
        self.logger = logger if logger is not None else LOGGER
        self.write_type = write_type
        self.messages_buffer = ""

    def write(self, message: str) -> None:
        """Use the logger as a file-like object. Needed by tqdm progress bar."""
        self.messages_buffer += message + "\n"

    def flush(self) -> None:
        """Write to the logger the content of the buffer."""
        if self.messages_buffer:
            if self.write_type == "stdout":
                self.add_stdout(self.messages_buffer)
            elif self.write_type == "stderr":
                self.add_stderr(self.messages_buffer)
            self.messages_buffer = ""

    @ensure_session
    def add_user_visible_log(
        self, message: str, session: Any = None, job_id: str | None = None
    ) -> None:
        cads_broker.database.add_event(
            event_type="user_visible_log",
            request_uid=self.job_id if job_id is None else job_id,
            message=message,
            session=session,
        )

    @ensure_session
    def add_user_visible_error(
        self, message: str, session: Any = None, job_id: str | None = None
    ) -> None:
        cads_broker.database.add_event(
            event_type="user_visible_error",
            request_uid=self.job_id if job_id is None else job_id,
            message=message,
            session=session,
        )

    @ensure_session
    def add_stdout(
        self,
        message: str,
        log_type: str = "INFO",
        session: Any = None,
        job_id: str | None = None,
        **kwargs,
    ) -> None:
        if job_id is None:
            job_id = self.job_id
        log_level = LEVELS_MAPPING.get(log_type, 10)
        self.logger.log(log_level, message, job_id=job_id, **kwargs)
        if log_level >= WORKER_LOG_LEVEL_INT:
            cads_broker.database.add_event(
                event_type=log_type,
                request_uid=job_id,
                message=message,
                session=session,
            )

    @ensure_session
    def add_stderr(
        self,
        message: str,
        log_type: str = "EXCEPTION",
        session: Any = None,
        job_id: str | None = None,
        **kwargs,
    ) -> None:
        if job_id is None:
            job_id = self.job_id
        log_level = LEVELS_MAPPING.get(log_type, 10)
        self.logger.log(log_level, message, job_id=job_id, **kwargs)
        if log_level >= WORKER_LOG_LEVEL_INT:
            cads_broker.database.add_event(
                event_type=log_type,
                request_uid=job_id,
                message=message,
                session=session,
            )

    @property
    def session_maker(self) -> cads_broker.database.sa.orm.sessionmaker:
        return create_session_maker()

    def upload_log(self, *args, **kwargs):
        self.add_stdout(*args, log_type="upload", **kwargs)

    def info(self, *args, **kwargs):
        self.add_stdout(*args, log_type="INFO", **kwargs)

    def debug(self, *args, **kwargs):
        self.add_stdout(*args, log_type="DEBUG", **kwargs)

    def warn(self, *args, **kwargs):
        self.add_stdout(*args, log_type="WARN", **kwargs)

    def warning(self, *args, **kwargs):
        self.add_stdout(*args, log_type="WARNING", **kwargs)

    def critical(self, *args, **kwargs):
        self.add_stderr(*args, log_type="CRITICAL", **kwargs)

    def error(self, *args, **kwargs):
        self.add_stderr(*args, log_type="ERROR", **kwargs)

    def exception(self, *args, **kwargs):
        self.add_stderr(*args, log_type="EXCEPTION", **kwargs)


def submit_workflow(
    entry_point: str,
    setup_code: str | None = None,
    request: dict[str, Any] = {},
    config: dict[str, Any] = {},
    form: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
):
    job_id = distributed.worker.thread_state.key  # type: ignore
    # send event with worker address and pid of the job
    worker = get_worker()
    worker.log_event(job_id, {"worker": worker.address, "pid": os.getpid()})
    logger = LOGGER.bind(job_id=job_id)
    context = Context(job_id=job_id, logger=logger)
    with context.session_maker() as session:
        cads_broker.database.add_event(
            event_type="worker_name",
            request_uid=job_id,
            message=socket.gethostname(),
            session=session,
        )
        system_request = cads_broker.database.get_request(
            request_uid=job_id, session=session
        )
        request = system_request.request_body.get("request", {})
        form = system_request.adaptor_properties.form
        config.update(system_request.adaptor_properties.config)

    structlog.contextvars.bind_contextvars(event_type="DATASET_COMPUTE", job_id=job_id)

    cache_files_urlpath = random.choice(utils.parse_data_volumes_config())
    depth = int(os.getenv("CACHE_DEPTH", 1))
    if depth == 2:
        cache_files_urlpath = os.path.join(
            cache_files_urlpath, datetime.date.today().isoformat()
        )
    elif depth != 1:
        context.warn(f"CACHE_DETPH={depth} is not supported.")

    logger.info("Processing job", job_id=job_id)
    collection_id = config.get("collection_id")
    cacholote.config.set(
        logger=LOGGER,
        cache_files_urlpath=cache_files_urlpath,
        sessionmaker=context.session_maker,
        context=context,
        tag=collection_id,
    )
    fs, dirname = cacholote.utils.get_cache_files_fs_dirname()

    adaptor_class = cads_adaptors.get_adaptor_class(entry_point, setup_code)
    try:
        with utils.enter_tmp_working_dir() as working_dir:
            base_dir = dirname if "file" in fs.protocol else working_dir
            with utils.make_cache_tmp_path(base_dir) as cache_tmp_path:
                adaptor = adaptor_class(
                    form=form,
                    context=context,
                    cache_tmp_path=cache_tmp_path,
                    **config,
                )
                request = {k: request[k] for k in sorted(request.keys())}
                cached_retrieve = cacholote.cacheable(
                    adaptor.retrieve,
                    collection_id=collection_id,
                )
                result = cached_retrieve(request=request)
    except Exception as err:
        logger.exception(job_id=job_id, event_type="EXCEPTION")
        context.add_user_visible_error(f"The job failed with: {err.__class__.__name__}")
        context.error(f"{err.__class__.__name__}: {str(err)}")
        raise

    if "s3" in fs.protocol:
        fs.chmod(result.result["args"][0]["file:local_path"], acl="public-read")
    with context.session_maker() as session:
        request = cads_broker.database.set_request_cache_id(
            request_uid=job_id,
            cache_id=result.id,
            session=session,
        )
