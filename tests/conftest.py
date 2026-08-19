from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import Chat, Message

TEST_TOKEN = "42:TEST"
OTHER_TOKEN = "43:TEST"


def make_message(
    *,
    chat_id: int = 1,
    message_id: int = 1,
    text: str = "",
    thread_id: int | None = None,
) -> Message:
    return Message(
        message_id=message_id,
        date=datetime.now(UTC),
        chat=Chat(id=chat_id, type="private"),
        text=text,
        message_thread_id=thread_id,
    )


@pytest.fixture
async def bot() -> AsyncIterator[Bot]:
    instance = Bot(token=TEST_TOKEN, session=AiohttpSession())
    yield instance
    await instance.session.close()


@pytest.fixture
async def other_bot() -> AsyncIterator[Bot]:
    instance = Bot(token=OTHER_TOKEN, session=AiohttpSession())
    yield instance
    await instance.session.close()
