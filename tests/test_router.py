from typing import Any

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, Update

from aiogram_buffered_router import BufferedRouter, chat_key
from tests.conftest import make_message

IDLE = 10.0


async def _feed(
    dispatcher: Dispatcher, bot: Bot, message: Message, update_id: int = 1
) -> None:
    _ = await dispatcher.feed_update(
        bot=bot, update=Update(update_id=update_id, message=message)
    )


def _dispatcher(router: BufferedRouter) -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    return dispatcher


async def test_router_batches_updates_that_pass_the_filter(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    seen: list[list[str]] = []

    async def handler(messages: list[Message], _data: dict[str, Any]) -> None:
        seen.append([message.text or "" for message in messages])

    _ = router.buffered(F.text)(handler)
    dispatcher = _dispatcher(router)

    await _feed(dispatcher, bot, make_message(message_id=1, text="a"), 1)
    await _feed(dispatcher, bot, make_message(message_id=2, text="b"), 2)
    await router.flush()

    assert seen == [["a", "b"]]


async def test_router_ignores_updates_rejected_by_the_filter(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    seen: list[list[str]] = []

    async def handler(messages: list[Message], _data: dict[str, Any]) -> None:
        seen.append([message.text or "" for message in messages])

    _ = router.buffered(F.text)(handler)
    dispatcher = _dispatcher(router)

    await _feed(dispatcher, bot, make_message(message_id=1, text=""), 1)
    await router.flush()

    assert seen == []
    assert router.pending == 0


async def test_router_passes_middleware_data_to_the_batch_handler(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    received: list[dict[str, Any]] = []

    async def handler(_messages: list[Message], data: dict[str, Any]) -> None:
        received.append(data)

    _ = router.buffered(F.text)(handler)
    dispatcher = _dispatcher(router)

    await _feed(dispatcher, bot, make_message(text="a"))
    await router.flush()

    assert received
    assert received[0]["bot"] is bot


async def test_per_handler_options_override_router_defaults(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE, max_size=0)
    seen: list[list[str]] = []

    async def handler(messages: list[Message], _data: dict[str, Any]) -> None:
        seen.append([message.text or "" for message in messages])

    _ = router.buffered(F.text, key=chat_key)(handler)
    dispatcher = _dispatcher(router)

    await _feed(dispatcher, bot, make_message(message_id=1, text="a", thread_id=1), 1)
    await _feed(dispatcher, bot, make_message(message_id=2, text="b", thread_id=2), 2)
    await router.flush()

    assert seen == [["a", "b"]]


async def test_attach_returns_an_independent_buffer() -> None:
    router = BufferedRouter(interval=IDLE)
    seen: list[list[str]] = []

    async def handler(messages: list[Message], _data: dict[str, Any]) -> None:
        seen.append([message.text or "" for message in messages])

    buffer = router.attach(handler, interval=IDLE, max_size=5)
    await buffer.add(make_message(text="a"), {})

    assert router.pending == 1
    await router.aclose()
    assert seen == [["a"]]


async def test_router_aclose_without_flush_drops_everything(bot: Bot) -> None:
    router = BufferedRouter(interval=IDLE)
    seen: list[list[str]] = []

    async def handler(messages: list[Message], _data: dict[str, Any]) -> None:
        seen.append([message.text or "" for message in messages])

    _ = router.buffered(F.text)(handler)
    dispatcher = _dispatcher(router)

    await _feed(dispatcher, bot, make_message(text="a"))
    await router.aclose(flush=False)

    assert seen == []


async def test_default_router_name() -> None:
    assert BufferedRouter().name == "buffered"
    assert BufferedRouter(name="chat").name == "chat"
