import asyncio
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiogram_buffered_router import MessageBuffer, chat_key
from tests.conftest import make_message

DEADLINE = 10.0

PLAN = st.lists(
    st.tuples(st.integers(min_value=1, max_value=3), st.text(min_size=1, max_size=8)),
    max_size=40,
)


async def _drive(
    plan: list[tuple[int, str]], *, max_size: int, interval: float
) -> list[list[tuple[int, str]]]:
    delivered: list[list[tuple[int, str]]] = []

    async def handler(messages: list[Any], _data: dict[str, Any]) -> None:
        delivered.append(
            [(message.chat.id, message.text or "") for message in messages]
        )

    buffer = MessageBuffer(
        handler=handler, interval=interval, max_size=max_size, key=chat_key
    )
    async with asyncio.timeout(DEADLINE):
        for index, (chat, text) in enumerate(plan, start=1):
            await buffer.add(
                make_message(chat_id=chat, message_id=index, text=text), {}
            )
        await buffer.aclose()
    return delivered


@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(plan=PLAN, max_size=st.integers(min_value=0, max_value=4))
def test_every_message_is_delivered_exactly_once_in_order(
    plan: list[tuple[int, str]], max_size: int
) -> None:
    delivered = asyncio.run(_drive(plan, max_size=max_size, interval=10.0))
    flat = [item for batch in delivered for item in batch]

    for chat in {chat for chat, _ in plan}:
        assert [text for c, text in flat if c == chat] == [
            text for c, text in plan if c == chat
        ]
    assert sorted(flat) == sorted(plan)


@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(plan=PLAN, max_size=st.integers(min_value=1, max_value=4))
def test_batches_never_exceed_max_size_and_are_never_empty(
    plan: list[tuple[int, str]], max_size: int
) -> None:
    delivered = asyncio.run(_drive(plan, max_size=max_size, interval=10.0))

    assert all(0 < len(batch) <= max_size for batch in delivered)


@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(plan=PLAN)
def test_a_batch_never_mixes_keys(plan: list[tuple[int, str]]) -> None:
    delivered = asyncio.run(_drive(plan, max_size=0, interval=10.0))

    assert all(len({chat for chat, _ in batch}) == 1 for batch in delivered)
