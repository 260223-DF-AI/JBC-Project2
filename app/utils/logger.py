import logging
from functools import wraps
import os
import sys

log_str_to_obj: dict = {
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# ensure logs folder exists
os.makedirs("logs/", exist_ok=True)

# constant log configs
LOG_PATH: str = "logs/log.log"
LOG_FORMAT: str = "%(asctime)s | %(levelname)s | %(message)s"
LOGGING_LEVEL: int = logging.INFO


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger with proper configuration.
    """

    logging.basicConfig(
        filename=LOG_PATH,
        format=LOG_FORMAT,
        filemode="w",
        level=LOGGING_LEVEL)

    logger = logging.getLogger(name)
    formatter = logging.Formatter(LOG_FORMAT)

    # only configure handlers if they haven't already been
    if not logger.handlers:
        # send logs to log file
        logger.addHandler(logging.FileHandler(LOG_PATH))

        # also send logs to STDOUT
        logger.addHandler(logging.StreamHandler(stream=sys.stdout))
        logger.handlers[0].setFormatter(formatter)

    return logger


# pass logger to decorator
def log_execution(func1):
    """Decorator function to log when a function starts & terminates"""
    @wraps(func1)
    def wrapper(*args, **kwargs):
        "Decorator wrapper"
        logger = get_logger(func1.__module__)

        logger.info(f"Running {func1.__name__}")
        result = func1(*args, **kwargs)
        logger.info(f"Finished {func1.__name__}")
        return result
    return wrapper