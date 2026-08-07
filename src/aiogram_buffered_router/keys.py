from collections.abc import Hashable

from aiogram.types import Message


def _bot_id(message: Message) -> int:
    bot = message.bot
    return bot.id if bot is not None else 0


def chat_key(message: Message) -> Hashable:
    return (_bot_id(message), message.chat.id)


def thread_key(message: Message) -> Hashable:
    return (_bot_id(message), message.chat.id, message.message_thread_id)
