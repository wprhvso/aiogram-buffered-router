import asyncio
import logging
from typing import Any

import pytest

from aiogram_buffered_router import MessageBuffer, chat_key
from aiogram_buffered_router.buffer import BufferClosedError
from tests.conftest import make_message

IDLE = 10.0
SHORT = 0.02
DEADLINE = 5.0


class Collector:
    def __init__(self) -> None:
        self.batches: list[list[str]] = []
        self.data: list[dict[str, Any]] = []
        self.called = asyncio.Event()

    async def __call__(self, messages: list[Any], data: dict[str, Any]) -> None:
        self.batches.append([message.text for message in messages])
        self.data.append(data)
        self.called.set()


async def test_consecutive_messages_collapse_into_one_batch() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        for index, text in enumerate(("a", "b", "c"), start=1):
            await buffer.add(make_message(message_id=index, text=text), {})
        await buffer.flush()

    assert collector.batches == [["a", "b", "c"]]


async def test_debounce_dispatches_without_explicit_flush() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=SHORT)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="a"), {})
        _ = await collector.called.wait()
        await buffer.aclose()

    assert collector.batches == [["a"]]


async def test_batches_are_isolated_per_key() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE, key=chat_key)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(chat_id=1, text="one"), {})
        await buffer.add(make_message(chat_id=2, text="two"), {})
        await buffer.flush()

    assert sorted(collector.batches) == [["one"], ["two"]]


async def test_max_size_dispatches_before_interval_elapses() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE, max_size=2)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(message_id=1, text="a"), {})
        await buffer.add(make_message(message_id=2, text="b"), {})
        _ = await collector.called.wait()

    assert collector.batches == [["a", "b"]]
    await buffer.aclose(flush=False)


async def test_max_size_splits_an_oversized_burst() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE, max_size=2)

    async with asyncio.timeout(DEADLINE):
        for index, text in enumerate(("a", "b", "c", "d", "e"), start=1):
            await buffer.add(make_message(message_id=index, text=text), {})
        await buffer.flush()

    assert collector.batches == [["a", "b"], ["c", "d"], ["e"]]


async def test_handler_receives_data_of_last_message() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(message_id=1, text="a"), {"n": 1})
        await buffer.add(make_message(message_id=2, text="b"), {"n": 2})
        await buffer.flush()

    assert collector.data == [{"n": 2}]


async def test_handler_failure_is_logged_and_does_not_propagate(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def failing(_messages: list[Any], _data: dict[str, Any]) -> None:
        raise RuntimeError

    buffer = MessageBuffer(handler=failing, interval=IDLE)

    with caplog.at_level(logging.ERROR, logger="aiogram_buffered_router.buffer"):
        async with asyncio.timeout(DEADLINE):
            await buffer.add(make_message(text="a"), {})
            await buffer.flush()

    assert caplog.records


async def test_buffer_survives_a_failing_batch() -> None:
    seen: list[str] = []

    async def flaky(messages: list[Any], _data: dict[str, Any]) -> None:
        if messages[0].text == "boom":
            raise RuntimeError
        seen.append(messages[0].text)

    buffer = MessageBuffer(handler=flaky, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="boom"), {})
        await buffer.flush()
        await buffer.add(make_message(text="ok"), {})
        await buffer.flush()

    assert seen == ["ok"]


async def test_pending_counts_undelivered_messages() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        assert buffer.pending == 0
        await buffer.add(make_message(text="a"), {})
        assert buffer.pending == 1
        await buffer.flush()

    assert buffer.pending == 0


async def test_aclose_flushes_pending_messages() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="a"), {})
        await buffer.aclose()

    assert collector.batches == [["a"]]


async def test_aclose_without_flush_drops_pending_messages() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="a"), {})
        await buffer.aclose(flush=False)

    assert collector.batches == []
    assert buffer.pending == 0


async def test_add_after_close_is_rejected() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE)
    await buffer.aclose()

    with pytest.raises(BufferClosedError):
        await buffer.add(make_message(text="a"), {})


async def test_flush_on_empty_buffer_is_a_noop() -> None:
    collector = Collector()
    buffer = MessageBuffer(handler=collector, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.flush()

    assert collector.batches == []
