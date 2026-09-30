"""Configure reusable colored logging helpers.

Responsibilities
----------------
- Format log records and attach consistent handlers to decorated callables.
"""

import asyncio
from functools import wraps
import inspect
import logging
import time


FORMAT = "%(asctime)s %(name)s %(levelname)s: %(message)s"


class ColoredFormatter(logging.Formatter):
    """Render log levels with ANSI colors while preserving standard formatting."""
    COLORS = {
        logging.DEBUG: "\033[33m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[35m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[41m",
    }
    RESET = "\033[0m"

    def format(self, record):
        """Format one log record and color its level label for terminal output."""
        color = self.COLORS.get(record.levelno, "")
        return f"{color}{super().format(record)}{self.RESET}"


def configure_logging(level=logging.INFO):
    """Configure the application-wide logging threshold once."""
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredFormatter(FORMAT))
        root.addHandler(handler)
    root.setLevel(level)
    logging.disable(logging.NOTSET)


def logged(logger_name=None, level=logging.INFO, *, log_result=True):
    """
    Log function calls, failures, and execution time.

    Parameters
    ----------
    logger_name : str, optional
        Logger receiving the call and return records.
    level : int, default=logging.INFO
        Logging level used for normal call and return records.
    log_result : bool, default=True
        Whether to include the callable's returned value. Disable this for
        signed requests, credentials, or other sensitive values.

    Returns
    -------
    Callable
        Decorator preserving synchronous, coroutine, and async-generator behavior.
    """

    def decorator(func):
        logger = logging.getLogger(logger_name or func.__module__)

        if inspect.isasyncgenfunction(func):
            @wraps(func)
            async def async_generator_wrapper(*args, **kwargs):
                logger.log(level, f"CALL {func.__name__}")
                start = time.perf_counter()
                try:
                    async for result in func(*args, **kwargs):
                        yield result
                except Exception:
                    logger.exception(f"EXCEPTION in {func.__name__}")
                    raise
                duration = time.perf_counter() - start
                logger.log(level, f"RETURN {func.__name__} in {duration:.4f}s")

            return async_generator_wrapper

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                logger.log(level, f"CALL {func.__name__}")
                start = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                except Exception:
                    logger.exception(f"EXCEPTION in {func.__name__}")
                    raise
                duration = time.perf_counter() - start
                returned = f" -> {result!r}" if log_result else ""
                logger.log(
                    level,
                    f"RETURN {func.__name__}{returned} in {duration:.4f}s",
                )
                return result

            return async_wrapper

        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.log(level, f"CALL {func.__name__}")
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            except Exception:
                logger.exception(f"EXCEPTION in {func.__name__}")
                raise
            duration = time.perf_counter() - start
            returned = f" -> {result!r}" if log_result else ""
            logger.log(
                level,
                f"RETURN {func.__name__}{returned} in {duration:.4f}s",
            )
            return result

        return wrapper

    return decorator
