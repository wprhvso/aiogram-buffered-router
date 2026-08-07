from aiogram_buffered_router import chat_key, thread_key
from tests.conftest import make_message


def test_chat_key_ignores_thread() -> None:
    left = make_message(chat_id=7, thread_id=None)
    right = make_message(chat_id=7, thread_id=42)
    assert chat_key(left) == chat_key(right)


def test_thread_key_separates_topics() -> None:
    left = make_message(chat_id=7, thread_id=None)
    right = make_message(chat_id=7, thread_id=42)
    assert thread_key(left) != thread_key(right)


def test_keys_separate_chats() -> None:
    assert chat_key(make_message(chat_id=1)) != chat_key(make_message(chat_id=2))
    assert thread_key(make_message(chat_id=1)) != thread_key(make_message(chat_id=2))
