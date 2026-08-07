# aiogram-buffered-router

Debounced message batching for aiogram 3. Consecutive messages from the same chat are collected into one batch and passed to a single handler.

## Install

```
uv add aiogram-buffered-router
```

## Usage

```python
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram_buffered_router import BufferedRouter

router = BufferedRouter(name="chat", interval=1.0)


@router.buffered(F.text)
async def handle_batch(messages: list[Message], data: dict[str, object]) -> None:
    bot = data["bot"]
    texts = [message.text for message in messages if message.text]
    print(bot, texts)


dispatcher = Dispatcher()
dispatcher.include_router(router)
```

On shutdown flush the pending batches:

```python
await router.aclose()
```

## Options

- `interval` — debounce window in seconds, restarted on nothing; the batch is dispatched `interval` seconds after the first message.
- `max_size` — dispatch immediately once the batch reaches this size, `0` disables the limit.
- `key` — `chat_key` (bot + chat) or `thread_key` (bot + chat + topic, default), or any callable returning a hashable.

Handler exceptions are logged by the `aiogram_buffered_router.buffer` logger and never break polling.

## License

MIT
