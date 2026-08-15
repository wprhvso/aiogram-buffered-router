import asyncio
import contextvars
from typing import Any

from aiogram.types import Message

from aiogram_buffered_router import CONTEXTS_KEY, MessageBuffer
from tests.conftest import make_message

IDLE = 10.0
SHORT = 0.02
DEADLINE = 5.0

origin: contextvars.ContextVar[str] = contextvars.ContextVar("origin", default="none")


class Recorder:
    def __init__(self) -> None:
        self.seen: list[str] = []
        self.data: list[dict[str, Any]] = []
        self.called = asyncio.Event()

    async def __call__(self, _messages: list[Message], data: dict[str, Any]) -> None:
        self.seen.append(origin.get())
        self.data.append(data)
        self.called.set()


async def add_as(buffer: MessageBuffer, message: Message, tag: str) -> None:
    """Add a message from a context tagged like the update that carried it."""
    token = origin.set(tag)
    try:
        await buffer.add(message, {})
    finally:
        origin.reset(token)


async def test_batch_runs_in_the_context_of_the_update_that_opened_it() -> None:
    recorder = Recorder()
    buffer = MessageBuffer(handler=recorder, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await add_as(buffer, make_message(message_id=1, text="a"), "first")
        await add_as(buffer, make_message(message_id=2, text="b"), "second")
        await buffer.flush()

    assert recorder.seen == ["first"]


async def test_a_later_batch_does_not_inherit_the_earlier_one() -> None:
    # The batch task outlives a single batch, so without a per-batch context
    # every later turn in a busy chat would report under the first turn.
    recorder = Recorder()
    buffer = MessageBuffer(handler=recorder, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await add_as(buffer, make_message(message_id=1, text="a"), "turn-one")
        await buffer.flush()
        await add_as(buffer, make_message(message_id=2, text="b"), "turn-two")
        await buffer.flush()

    assert recorder.seen == ["turn-one", "turn-two"]


async def test_the_buffer_loop_itself_starts_from_a_clean_context() -> None:
    recorder = Recorder()
    buffer = MessageBuffer(handler=recorder, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        token = origin.set("caller")
        try:
            await buffer.add(make_message(text="a"), {})
        finally:
            origin.reset(token)
        # The handler still sees the caller because the message carried it,
        # but nothing leaks through the loop itself.
        await buffer.flush()

    assert recorder.seen == ["caller"]


async def test_contexts_are_exposed_when_asked_for() -> None:
    recorder = Recorder()
    buffer = MessageBuffer(handler=recorder, interval=IDLE, expose_contexts=True)

    async with asyncio.timeout(DEADLINE):
        await add_as(buffer, make_message(message_id=1, text="a"), "first")
        await add_as(buffer, make_message(message_id=2, text="b"), "second")
        await buffer.flush()

    contexts = recorder.data[0][CONTEXTS_KEY]
    assert len(contexts) == 2
    assert [context.run(origin.get) for context in contexts] == ["first", "second"]


async def test_contexts_are_absent_by_default() -> None:
    recorder = Recorder()
    buffer = MessageBuffer(handler=recorder, interval=IDLE)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="a"), {})
        await buffer.flush()

    assert CONTEXTS_KEY not in recorder.data[0]


async def test_error_handler_receives_the_failure() -> None:
    seen: list[tuple[BaseException, int]] = []

    async def failing(messages: list[Message], _data: dict[str, Any]) -> None:
        raise ValueError(f"boom {len(messages)}")

    buffer = MessageBuffer(
        handler=failing,
        interval=IDLE,
        on_error=lambda error, messages, _data: seen.append((error, len(messages))),
    )

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(text="a"), {})
        await buffer.flush()

    assert len(seen) == 1
    error, size = seen[0]
    assert isinstance(error, ValueError)
    assert size == 1


async def test_error_handler_runs_in_the_originating_context() -> None:
    seen: list[str] = []

    async def failing(_messages: list[Message], _data: dict[str, Any]) -> None:
        raise ValueError("boom")

    buffer = MessageBuffer(
        handler=failing,
        interval=IDLE,
        on_error=lambda *_: seen.append(origin.get()),
    )

    async with asyncio.timeout(DEADLINE):
        await add_as(buffer, make_message(text="a"), "the-update")
        await buffer.flush()

    assert seen == ["the-update"]


async def test_a_broken_error_handler_does_not_stop_the_buffer() -> None:
    calls: list[int] = []

    async def failing(_messages: list[Message], _data: dict[str, Any]) -> None:
        calls.append(1)
        raise ValueError("boom")

    def explode(*_: object) -> None:
        raise RuntimeError("hook is broken")

    buffer = MessageBuffer(handler=failing, interval=IDLE, on_error=explode)

    async with asyncio.timeout(DEADLINE):
        await buffer.add(make_message(message_id=1, text="a"), {})
        await buffer.flush()
        await buffer.add(make_message(message_id=2, text="b"), {})
        await buffer.flush()

    assert len(calls) == 2
