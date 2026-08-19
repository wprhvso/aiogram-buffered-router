import asyncio
from typing import Any

import pytest
from aiogram.types import Message

from aiogram_buffered_router import BufferedRouter, MessageBuffer
from aiogram_buffered_router.buffer import BufferClosedError
from tests.conftest import make_message

IDLE = 10.0
SHORT = 0.02
DEADLINE = 5.0


class Blocking:
    def __init__(self, *, block: str) -> None:
        self.block = block
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()
        self.seen: list[str] = []

    async def __call__(self, messages: list[Message], _data: dict[str, Any]) -> None:
        self.entered.set()
        if messages[0].text == self.block:
            _ = await self.gate.wait()
        self.seen.append(messages[0].text or "")


async def test_a_cancelled_flush_does_not_strand_later_messages() -> None:
    handler = Blocking(block="slow")
    buffer = MessageBuffer(handler=handler, interval=IDLE)

    await buffer.add(make_message(message_id=1, text="slow"), {})
    with pytest.raises(TimeoutError):
        _ = await asyncio.wait_for(buffer.flush(), SHORT)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(message_id=2, text="next"), {})
        await buffer.flush()

    assert handler.seen == ["next"]


async def test_a_cancelled_flush_leaves_the_debounce_window_intact() -> None:
    handler = Blocking(block="slow")
    buffer = MessageBuffer(handler=handler, interval=IDLE)

    await buffer.add(make_message(message_id=1, text="slow"), {})
    with pytest.raises(TimeoutError):
        _ = await asyncio.wait_for(buffer.flush(), SHORT)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(message_id=2, text="a"), {})
        await buffer.add(make_message(message_id=3, text="b"), {})
        assert buffer.pending == 2
        await buffer.flush()

    assert handler.seen == ["a"]


async def test_a_size_dispatch_leaves_the_remainder_buffered() -> None:
    handler = Blocking(block="never")
    buffer = MessageBuffer(handler=handler, interval=IDLE, max_size=2)

    async with asyncio.timeout(DEADLINE):
        for index, text in enumerate(("a", "b", "c"), start=1):
            await buffer.add(make_message(message_id=index, text=text), {})
        _ = await handler.entered.wait()
        assert buffer.pending == 1
        await buffer.flush()

    assert handler.seen == ["a", "c"]


async def test_close_without_flush_cancels_a_running_handler() -> None:
    handler = Blocking(block="slow")
    buffer = MessageBuffer(handler=handler, interval=SHORT)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="slow"), {})
        _ = await handler.entered.wait()
        await buffer.aclose(flush=False)

    assert handler.seen == []
    assert buffer.pending == 0


async def test_aclose_is_idempotent() -> None:
    handler = Blocking(block="never")
    buffer = MessageBuffer(handler=handler, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="a"), {})
        await buffer.aclose()
        await buffer.aclose()

    assert handler.seen == ["a"]


async def test_flush_after_close_stays_a_noop() -> None:
    handler = Blocking(block="never")
    buffer = MessageBuffer(handler=handler, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.aclose()
        await buffer.flush()

    assert handler.seen == []
    with pytest.raises(BufferClosedError):
        await buffer.add(make_message(text="a"), {})


async def test_a_key_is_reusable_after_its_batch_is_delivered() -> None:
    handler = Blocking(block="never")
    buffer = MessageBuffer(handler=handler, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(message_id=1, text="a"), {})
        await buffer.flush()
        assert not buffer.is_buffering(make_message(message_id=2))
        await buffer.add(make_message(message_id=2, text="b"), {})
        await buffer.flush()

    assert handler.seen == ["a", "b"]


async def test_router_closes_every_attached_buffer() -> None:
    router = BufferedRouter(interval=IDLE)
    first = Blocking(block="never")
    second = Blocking(block="never")
    left = router.attach(first)
    right = router.attach(second)

    async with asyncio.timeout(DEADLINE):
        await left.add(make_message(message_id=1, text="a"), {})
        await right.add(make_message(message_id=2, text="b"), {})
        assert router.pending == 2
        await router.aclose()

    assert first.seen == ["a"]
    assert second.seen == ["b"]
    assert router.pending == 0


async def test_router_flush_drains_every_attached_buffer() -> None:
    router = BufferedRouter(interval=IDLE)
    first = Blocking(block="never")
    second = Blocking(block="never")
    left = router.attach(first)
    right = router.attach(second)

    async with asyncio.timeout(DEADLINE):
        await left.add(make_message(message_id=1, text="a"), {})
        await right.add(make_message(message_id=2, text="b"), {})
        await router.flush()

    assert first.seen == ["a"]
    assert second.seen == ["b"]
    assert router.pending == 0


async def test_flush_of_a_router_without_buffers_is_a_noop() -> None:
    router = BufferedRouter(interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await router.flush()
        await router.aclose()

    assert router.pending == 0
