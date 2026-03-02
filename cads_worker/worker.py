import datetime
import functools
import logging
import os
import random
import socket
import time
from typing import Any
import yaml

import cacholote
import cads_adaptors
import cads_broker.database
import dask
import dask.config
import distributed.worker
import structlog
from distributed import get_worker

from . import config, utils

config.configure_logger(os.getenv("WORKER_LOG_LEVEL", "NOT_SET").upper())

LOGGER = structlog.get_logger(__name__)

LEVELS_MAPPING = logging.getLevelNamesMapping()

DB_CONNECTION_RETRIES = int(os.getenv("WORKER_DB_CONNECTION_RETRIES", 3))


@functools.lru_cache
def create_session_maker() -> cads_broker.database.sa.orm.sessionmaker:
    return cads_broker.database.ensure_session_obj(None)


def ensure_session(func):
    @functools.wraps(func)
    def wrapper(self, *args, session=None, **kwargs):
        retries = 1
        while retries <= DB_CONNECTION_RETRIES:
            try:
                close_session = False
                # create a new session if not provided
                if session is None:
                    session = create_session_maker()()
                    close_session = True
                # run the function
                result = func(self, *args, session=session, **kwargs)
                # close the session if we created it
                if close_session:
                    session.close()
                return result
            except cads_broker.database.sa.exc.OperationalError as e:
                exception = e
                retries += 1
                self.logger.warning(
                    f"Database operation failed. Retrying {retries}/{DB_CONNECTION_RETRIES}...",
                    error=str(e),
                )
                # close the session anyway because it could be broken
                session.close()
                session = None
                time.sleep(os.getenv("WORKER_DB_CONNECTION_RETRY_SLEEP", 2))

        self.logger.error("Max retries reached. Aborting operation.")
        raise exception

    return wrapper


class Context(cacholote.config.Context):
    def __init__(
        self,
        job_id: str | None = None,
        logger: Any | None = None,
        write_type: str = "stdout",
        worker_log_level_to_stdout: int = 10,
        worker_log_level_to_db: int = 60,
    ):
        self.job_id = job_id
        self.logger = logger if logger is not None else LOGGER
        self.write_type = write_type
        self.messages_buffer = ""
        self.worker_log_level_to_stdout = worker_log_level_to_stdout
        self.worker_log_level_to_db = worker_log_level_to_db

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

        if log_level >= self.worker_log_level_to_stdout:
            self.logger.log(log_level, message, job_id=job_id, **kwargs)

        if log_level >= self.worker_log_level_to_db:
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

        if log_level >= self.worker_log_level_to_stdout:
            self.logger.log(log_level, message, job_id=job_id, **kwargs)

        if log_level >= self.worker_log_level_to_db:
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

    def set_worker_log_level_to_stdout(self, worker_log_level_to_stdout):
        self.worker_log_level_to_stdout = worker_log_level_to_stdout

    def set_worker_log_level_to_db(self, worker_log_level_to_db):
        self.worker_log_level_to_db = worker_log_level_to_db


DEFAULT_LOG_SWITCHES_CONFIG_FILE = "/etc/log-config/log-switches.yaml"
def get_log_switches_config():
    log_switches_config_file = os.getenv("LOG_SWITCHES_CONFIG_FILE", DEFAULT_LOG_SWITCHES_CONFIG_FILE)

    if os.path.exists(log_switches_config_file):
        with open(log_switches_config_file, 'r') as f:
            log_switches_config = yaml.load(f, Loader=yaml.SafeLoader)
    else:
        raise Exception(
            "MARS servers cannot be found, this is an error at the system level."
        )
    return log_switches_config


DEFAULT_CONFIG = {"stdout": "info", "db": "nothing"}
def parse_config(config):
    if isinstance(config,str):
        worker_log_level_to_stdout = config
        worker_log_level_to_db = config
    else:
        worker_log_level_to_stdout = config.get("stdout", DEFAULT_CONFIG["stdout"])
        worker_log_level_to_db = config.get("db", DEFAULT_CONFIG["db"])

    worker_log_level_to_stdout = LEVELS_MAPPING.get(worker_log_level_to_stdout.upper(), 10)
    worker_log_level_to_db = LEVELS_MAPPING.get(worker_log_level_to_db.upper(), 60)

    return worker_log_level_to_stdout, worker_log_level_to_db


def determine_log_levels(adaptor, dataset) -> tuple[int,int]:
    log_switches_config = get_log_switches_config()

    config_for_dataset =  log_switches_config.get("datasets", {}).get(dataset)
    if config_for_dataset:
        worker_log_level_to_stdout, worker_log_level_to_db = parse_config(config_for_dataset)
    else:
        config_for_adaptor =  log_switches_config.get("adaptors", {}).get(adaptor)
        if config_for_adaptor:
            worker_log_level_to_stdout, worker_log_level_to_db = parse_config(config_for_adaptor)
        else:
            config_for_dataset =  log_switches_config.get("datasets", {}).get("default")
            if config_for_dataset:
                worker_log_level_to_stdout, worker_log_level_to_db = parse_config(config_for_dataset)
            else:
                config_for_adaptor =  log_switches_config.get("adaptors", {}).get("default", {})
                worker_log_level_to_stdout, worker_log_level_to_db = parse_config(config_for_adaptor)

    return worker_log_level_to_stdout, worker_log_level_to_db


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

    collection_id = config.get("collection_id")
    worker_log_level_to_stdout,worker_log_level_to_db = determine_log_levels(entry_point, collection_id)
    logger.info(f"----------> Calling adaptor {entry_point} for dataset {collection_id}...")
    context.set_worker_log_level_to_stdout(worker_log_level_to_stdout)
    context.set_worker_log_level_to_db(worker_log_level_to_db)

    cache_files_urlpath = random.choice(utils.parse_data_volumes_config())
    depth = int(os.getenv("CACHE_DEPTH", 1))
    if depth == 2:
        cache_files_urlpath = os.path.join(
            cache_files_urlpath, datetime.date.today().isoformat()
        )
    elif depth != 1:
        context.warn(f"CACHE_DETPH={depth} is not supported.")

    logger.info("Processing job", job_id=job_id)
    dask.config.set(scheduler=os.getenv("WORKER_SCHEDULER_TYPE", "single-threaded"))
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
