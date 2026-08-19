import asyncio
from typing import Any

from aiogram.types import Message

from aiogram_buffered_router import MessageBuffer, chat_key
from tests.conftest import make_message

IDLE = 10.0
SHORT = 0.02
DEADLINE = 10.0
CHATS = 4
PER_CHAT = 25


class Ledger:
    def __init__(self) -> None:
        self.batches: list[list[tuple[int, str]]] = []
        self.overlaps = 0
        self.running = 0

    async def __call__(self, messages: list[Message], _data: dict[str, Any]) -> None:
        self.running += 1
        if self.running > 1:
            self.overlaps += 1
        await asyncio.sleep(0)
        self.batches.append([(m.chat.id, m.text or "") for m in messages])
        self.running -= 1

    def flat(self) -> list[tuple[int, str]]:
        return [item for batch in self.batches for item in batch]


def _plan() -> list[tuple[int, str]]:
    return [
        (chat, f"{chat}-{index}")
        for index in range(PER_CHAT)
        for chat in range(1, CHATS + 1)
    ]


async def test_concurrent_producers_lose_nothing() -> None:
    ledger = Ledger()
    buffer = MessageBuffer(handler=ledger, interval=SHORT, max_size=3, key=chat_key)
    plan = _plan()

    async with asyncio.timeout(DEADLINE):
        await asyncio.gather(
            *(
                buffer.add(make_message(chat_id=chat, message_id=index, text=text), {})
                for index, (chat, text) in enumerate(plan, start=1)
            )
        )
        await buffer.aclose()

    assert sorted(ledger.flat()) == sorted(plan)


async def test_batches_stay_single_keyed_and_ordered_under_load() -> None:
    ledger = Ledger()
    buffer = MessageBuffer(handler=ledger, interval=SHORT, max_size=3, key=chat_key)
    plan = _plan()

    async with asyncio.timeout(DEADLINE):
        for index, (chat, text) in enumerate(plan, start=1):
            await buffer.add(
                make_message(chat_id=chat, message_id=index, text=text), {}
            )
        await buffer.aclose()

    assert all(len({chat for chat, _ in batch}) == 1 for batch in ledger.batches)
    assert all(0 < len(batch) <= 3 for batch in ledger.batches)
    for chat in range(1, CHATS + 1):
        assert [text for c, text in ledger.flat() if c == chat] == [
            text for c, text in plan if c == chat
        ]


async def test_a_key_never_runs_two_batches_at_once() -> None:
    ledger = Ledger()
    buffer = MessageBuffer(handler=ledger, interval=SHORT, max_size=1)
    plan = [(1, f"1-{index}") for index in range(10)]

    async with asyncio.timeout(DEADLINE):
        for index, (chat, text) in enumerate(plan, start=1):
            await buffer.add(
                make_message(chat_id=chat, message_id=index, text=text), {}
            )
        await buffer.aclose()

    assert ledger.overlaps == 0
    assert ledger.flat() == plan


async def test_flush_racing_with_producers_delivers_everything() -> None:
    ledger = Ledger()
    buffer = MessageBuffer(handler=ledger, interval=IDLE, key=chat_key)
    plan = _plan()

    async def produce() -> None:
        for index, (chat, text) in enumerate(plan, start=1):
            await buffer.add(
                make_message(chat_id=chat, message_id=index, text=text), {}
            )
            await asyncio.sleep(0)

    async def drain() -> None:
        for _ in range(10):
            await buffer.flush()
            await asyncio.sleep(0)

    async with asyncio.timeout(DEADLINE):
        await asyncio.gather(produce(), drain())
        await buffer.aclose()

    assert sorted(ledger.flat()) == sorted(plan)


async def test_many_keys_do_not_leak_batches() -> None:
    ledger = Ledger()
    buffer = MessageBuffer(handler=ledger, interval=SHORT, key=chat_key)

    async with asyncio.timeout(DEADLINE):
        for index in range(50):
            await buffer.add(
                make_message(chat_id=index, message_id=index + 1, text=str(index)), {}
            )
        await buffer.flush()

    assert buffer.pending == 0
    assert len(ledger.batches) == 50
    await buffer.aclose()
