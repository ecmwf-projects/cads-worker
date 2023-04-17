import logging
import sys

import structlog
import cads_common.logging


def configure_logger() -> None:
    """Configure the logging module.

    This function configures the logging module to log in rfc5424 format.
    """
    cads_common.logging.structlog_configure()
    cads_common.logging.logging_configure()
