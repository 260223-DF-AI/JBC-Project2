import logging
import time
from functools import wraps
from typing import Callable, Any

from google.cloud.exceptions import NotFound, ServiceUnavailable
from google.api_core.exceptions import TooManyRequests, GoogleAPICallError

logger = logging.getLogger(__name__)


class RetryableException(Exception):
    """Base class for exceptions that should trigger retries"""
    pass


def is_transient_error(exception: Exception) -> bool:
    """
    Determine if an exception is transient (should trigger retry)
    
    Args:
        exception: Exception to check
        
    Returns:
        True if exception is transient (temporary network/service issue), False if permanent
    """
    transient_exceptions = (
        ServiceUnavailable,
        TooManyRequests,
        TimeoutError,
        ConnectionError,
        ConnectionResetError,
        ConnectionAbortedError,
    )
    
    # Check for direct matches
    if isinstance(exception, transient_exceptions):
        return True
    
    # Check for Google API transient errors (e.g., 429, 503)
    if isinstance(exception, GoogleAPICallError):
        if hasattr(exception, "code"):
            # 429 = Too Many Requests, 503 = Service Unavailable, 500 = Internal Server Error
            return exception.code in [429, 500, 503]
    
    return False


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: int = 2,
    max_delay: int = 16
) -> Callable:
    """
    Decorator that retries a function with exponential backoff on transient failures
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds before first retry (default: 2)
        max_delay: Maximum delay capped at this value in seconds (default: 16)
        
    Returns:
        Decorated function that implements retry logic
        
    Example:
        @retry_with_backoff(max_retries=3, base_delay=2)
        def upload_to_gcs(blob, file_path):
            blob.upload_from_filename(file_path)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 0
            last_exception = None
            
            while attempt <= max_retries:
                try:
                    if attempt > 0:
                        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                        logger.info(
                            f"Retrying {func.__name__} (attempt {attempt}/{max_retries}) "
                            f"after {delay}s delay"
                        )
                        time.sleep(delay)
                    
                    return func(*args, **kwargs)
                
                except Exception as e:
                    last_exception = e
                    
                    if not is_transient_error(e):
                        logger.error(
                            f"Non-transient error in {func.__name__}: {type(e).__name__}: {e}. "
                            f"Not retrying."
                        )
                        raise
                    
                    if attempt >= max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exhausted for {func.__name__}. "
                            f"Last error: {type(e).__name__}: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"Transient error in {func.__name__} (attempt {attempt + 1}/{max_retries}): "
                        f"{type(e).__name__}: {e}"
                    )
                    attempt += 1
            
            # Should not reach here, but if we do, raise last exception
            if last_exception:
                raise last_exception
        
        return wrapper
    
    return decorator
