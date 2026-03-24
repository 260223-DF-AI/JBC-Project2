import logging
from functools import wraps
import os

log_str_to_obj: dict = {
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# Retrieve log path & format from config file, or use defaults
log_path: str = "logs/log.log"
log_format: str = "%(asctime)s | %(levelname)s | %(message)s"
logging_level: int = logging.INFO

# ensure logs folder exists
os.makedirs("logs/", exist_ok=True)

logging.basicConfig(
    filename=log_path,
    format=log_format,
    filemode="w",
    level=logging_level)

logger = logging.getLogger(__name__)
formatter = logging.Formatter(log_format)

# send logs to log file
logger.addHandler(logging.FileHandler(log_path))

# also send logs to STDOUT
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setFormatter(formatter)

# pass logger to decorator
def log_execution(func1):
    """Decorator function to log when a function starts & terminates"""
    @wraps(func1)
    def wrapper(*args, **kwargs):
        "Decorator wrapper"
        logger.info(f"Running {func1.__name__}")
        result = func1(*args, **kwargs)
        logger.info(f"Finished {func1.__name__}")
        return result
    return wrapper