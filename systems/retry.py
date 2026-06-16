import time
import random
import functools
import logging

logger = logging.getLogger(__name__)


def retry(max_attempts=3, base_delay=0.1, max_delay=30.0, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        raise
                    delay = min(max_delay, base_delay * (2 ** attempt) + random.uniform(0, 1))
                    logger.warning(f"{func.__name__} failed attempt {attempt + 1}: {e}. retrying in {delay:.2f}s")
                    time.sleep(delay)
        return wrapper
    return decorator