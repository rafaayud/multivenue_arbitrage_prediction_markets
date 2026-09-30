"""Exercise logger behavior in the utils layer.

Responsibilities
----------------
- Verify logger contracts, edge cases, and failure handling.
"""

import asyncio
import logging

from prediction_markets.utils.decorators.logger import configure_logging, logged


def test_logged_uses_message_level_without_overwriting_logger_threshold(caplog):
    logger_name = "tests.logged.levels"
    caplog.set_level(logging.DEBUG)
    configure_logging(logging.INFO)

    @logged(logger_name=logger_name, level=logging.INFO)
    def important():
        return "important"

    @logged(logger_name=logger_name, level=logging.DEBUG)
    def detail():
        return "detail"

    important()
    detail()
    configure_logging(logging.DEBUG)
    detail()

    assert logging.getLogger(logger_name).level == logging.NOTSET
    assert [
        (record.levelno, record.message.split()[0]) for record in caplog.records
    ] == [
        (logging.INFO, "CALL"),
        (logging.INFO, "RETURN"),
        (logging.DEBUG, "CALL"),
        (logging.DEBUG, "RETURN"),
    ]


def test_logged_consumes_async_generators_before_logging_return(caplog):
    logger_name = "tests.logged.async_generator"
    caplog.set_level(logging.INFO, logger=logger_name)

    @logged(logger_name=logger_name)
    async def values():
        yield 1

    async def consume():
        return [value async for value in values()]

    assert asyncio.run(consume()) == [1]
    assert [record.message.split()[0] for record in caplog.records] == ["CALL", "RETURN"]


def test_logged_can_hide_sensitive_return_values(caplog):
    """Retain duration logging without exposing a callable's result."""
    logger_name = "tests.logged.sensitive"
    caplog.set_level(logging.INFO, logger=logger_name)

    @logged(logger_name=logger_name, log_result=False)
    def signed_request():
        return "secret-signature"

    assert signed_request() == "secret-signature"
    assert "secret-signature" not in caplog.text
    assert "RETURN signed_request in " in caplog.text
