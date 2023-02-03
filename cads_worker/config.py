import json
import logging
from typing import Any, Callable, Mapping, MutableMapping

import structlog


def add_user_request_flag(
    logger: logging.Logger, method_name: str, event_dict: MutableMapping[str, Any]
) -> Mapping[str, Any]:
    """Add user_request flag to log message."""
    if "user_id" in event_dict:
        event_dict["user_request"] = True
    return event_dict


def sorting_serializer_factory(
    sorted_keys: list[str],
) -> Callable[[MutableMapping[str, Any]], str]:
    def sorting_serializer(event_dict: MutableMapping[str, Any], **kw: Any) -> str:
        sorted_dict = {}
        for key in sorted_keys:
            if key in event_dict:
                sorted_dict[key] = event_dict[key]
                event_dict.pop(key)
        for key in event_dict:
            sorted_dict[key] = event_dict[key]
        return json.dumps(sorted_dict, **kw)

    return sorting_serializer


def configure_logger() -> None:
    """Configure the logging module.

    This function configures the logging module to log in rfc5424 format.
    """
    logging.basicConfig(
        level=logging.INFO,
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            add_user_request_flag,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(
                serializer=sorting_serializer_factory(["event", "user_id"])
            ),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
