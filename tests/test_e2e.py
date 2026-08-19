import asyncio
import contextvars
from collections.abc import Awaitable, Callable
from typing import Any

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update

from aiogram_buffered_router import BufferedRouter, chat_key, thread_key
from tests.conftest import make_message

IDLE = 10.0
DEADLINE = 5.0

Next = Callable[[Message, dict[str, Any]], Awaitable[Any]]

trace: contextvars.ContextVar[str] = contextvars.ContextVar("trace", default="none")


class Sink:
    def __init__(self) -> None:
        self.batches: list[list[str]] = []
        self.data: list[dict[str, Any]] = []
        self.traces: list[str] = []
        self.called = asyncio.Event()

    async def __call__(self, messages: list[Message], data: dict[str, Any]) -> None:
        self.batches.append([message.text or "" for message in messages])
        self.data.append(data)
        self.traces.append(trace.get())
        self.called.set()


def _dispatcher(router: BufferedRouter) -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    return dispatcher


async def _feed(
    dispatcher: Dispatcher, bot: Bot, message: Message, update_id: int = 1
) -> None:
    _ = await dispatcher.feed_update(
        bot=bot, update=Update(update_id=update_id, message=message)
    )


async def test_a_conversation_arrives_as_a_single_batch(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    sink = Sink()
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        for index, text in enumerate(("answer briefly", "and in Russian"), start=1):
            await _feed(
                dispatcher, bot, make_message(message_id=index, text=text), index
            )
        await router.aclose()

    assert sink.batches == [["answer briefly", "and in Russian"]]
    assert sink.data[0]["bot"] is bot


async def test_middleware_data_and_context_reach_the_batch_handler(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    sink = Sink()

    async def middleware(handler: Next, event: Message, data: dict[str, Any]) -> Any:
        data["tenant"] = "acme"
        _ = trace.set(f"update-{event.message_id}")
        return await handler(event, data)

    router.message.middleware(middleware)
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(dispatcher, bot, make_message(message_id=1, text="a"), 1)
        await _feed(dispatcher, bot, make_message(message_id=2, text="b"), 2)
        await router.aclose()

    assert sink.data[0]["tenant"] == "acme"
    assert sink.traces == ["update-1"]


async def test_chats_never_share_a_batch(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    sink = Sink()
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(dispatcher, bot, make_message(chat_id=1, message_id=1, text="a"), 1)
        await _feed(dispatcher, bot, make_message(chat_id=2, message_id=2, text="b"), 2)
        await router.aclose()

    assert sorted(sink.batches) == [["a"], ["b"]]


async def test_topics_split_under_the_default_key(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    sink = Sink()
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(
            dispatcher, bot, make_message(message_id=1, text="a", thread_id=1), 1
        )
        await _feed(
            dispatcher, bot, make_message(message_id=2, text="b", thread_id=2), 2
        )
        await router.aclose()

    assert sorted(sink.batches) == [["a"], ["b"]]


async def test_topics_merge_under_the_chat_key(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE, key=chat_key)
    sink = Sink()
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(
            dispatcher, bot, make_message(message_id=1, text="a", thread_id=1), 1
        )
        await _feed(
            dispatcher, bot, make_message(message_id=2, text="b", thread_id=2), 2
        )
        await router.aclose()

    assert sink.batches == [["a", "b"]]


async def test_two_bots_do_not_share_a_batch(bot: Bot, other_bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE, key=chat_key)
    sink = Sink()
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(dispatcher, bot, make_message(message_id=1, text="a"), 1)
        await _feed(dispatcher, other_bot, make_message(message_id=2, text="b"), 2)
        await router.aclose()

    assert sorted(sink.batches) == [["a"], ["b"]]


async def test_a_continuation_joins_an_open_batch(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    sink = Sink()
    buffer = router.attach(sink, key=thread_key)

    async def collect(message: Message, **data: Any) -> None:
        await buffer.add(message, data)

    _ = router.message.register(collect, Command("system"))
    _ = router.message.register(collect, buffer.is_buffering)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(
            dispatcher, bot, make_message(message_id=1, text="/system briefly"), 1
        )
        await _feed(dispatcher, bot, make_message(message_id=2, text="in Russian"), 2)
        await router.aclose()

    assert sink.batches == [["/system briefly", "in Russian"]]


async def test_a_stray_message_is_ignored_when_no_batch_is_open(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    sink = Sink()
    buffer = router.attach(sink)

    async def collect(message: Message, **data: Any) -> None:
        await buffer.add(message, data)

    _ = router.message.register(collect, Command("system"))
    _ = router.message.register(collect, buffer.is_buffering)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(dispatcher, bot, make_message(text="in Russian"), 1)
        await router.aclose()

    assert sink.batches == []


async def test_a_failing_batch_is_reported_and_polling_survives(bot: Bot) -> None:
    failures: list[str] = []
    delivered: list[str] = []

    async def handler(messages: list[Message], _data: dict[str, Any]) -> None:
        if messages[0].text == "boom":
            raise RuntimeError("handler is broken")
        delivered.append(messages[0].text or "")

    router = BufferedRouter(
        interval=IDLE,
        on_error=lambda error, _messages, _data: failures.append(str(error)),
    )
    _ = router.buffered(F.text)(handler)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(dispatcher, bot, make_message(message_id=1, text="boom"), 1)
        await router.flush()
        await _feed(dispatcher, bot, make_message(message_id=2, text="ok"), 2)
        await router.aclose()

    assert failures == ["handler is broken"]
    assert delivered == ["ok"]


async def test_max_size_dispatches_without_waiting_for_the_window(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE, max_size=2)
    sink = Sink()
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(dispatcher, bot, make_message(message_id=1, text="a"), 1)
        await _feed(dispatcher, bot, make_message(message_id=2, text="b"), 2)
        _ = await sink.called.wait()

    assert sink.batches == [["a", "b"]]
    await router.aclose(flush=False)


async def test_shutdown_flushes_what_the_window_still_holds(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    sink = Sink()
    _ = router.buffered(F.text)(sink)
    dispatcher = _dispatcher(router)

    async with asyncio.timeout(DEADLINE):
        await _feed(dispatcher, bot, make_message(text="a"), 1)
        assert router.pending == 1
        await router.aclose()

    assert sink.batches == [["a"]]
    assert router.pending == 0
