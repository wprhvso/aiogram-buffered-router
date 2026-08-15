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
- `on_error` — called with `(error, messages, data)` when a batch handler raises.
- `expose_contexts` — pass the per-message contexts to the handler under `CONTEXTS_KEY`.

Handler exceptions are logged by the `aiogram_buffered_router.buffer` logger and never break polling.

## Context

Debouncing means the handler runs after the update that scheduled it has already
been answered, in a task that outlives it. Context is carried across that gap
rather than left to `asyncio`'s implicit inheritance, which would pin every batch
in a chat to whichever update happened to open the first one.

Each message is snapshotted with `contextvars.copy_context()` as it is buffered,
the batch runs in the snapshot of the message that opened it, and the batch loop
itself starts from an empty context so nothing leaks between batches. Anything
built on context variables — trace context, request-scoped log binding — reaches
the handler as a result, with no dependency on the library's side.

`on_error` runs in that same context, so a failure can be mapped back onto the
update that caused it. It is the only route out: the batch runs detached from the
dispatcher, so aiogram's error handling can never see it.

```python
router = BufferedRouter(name="chat", interval=1.0, on_error=report)
```

A batch is many updates collapsing into one unit of work, and only the first of
them can be the parent. Set `expose_contexts=True` to receive all of them and
relate the rest yourself:

```python
router = BufferedRouter(name="chat", interval=1.0, expose_contexts=True)


@router.buffered(F.text)
async def handle_batch(messages: list[Message], data: dict[str, object]) -> None:
    contexts = data.get(CONTEXTS_KEY, ())
    ...
```

## Joining a batch that is already open

Sometimes only the *first* message of a batch is recognisable. A command takes an argument
and the rest of the thought arrives as ordinary messages:

```
/system answer briefly
and in Russian
```

The second message matches no command filter, so it never reaches the buffer. Ask the
buffer whether it is already collecting for that message's key and register a second entry
into the same buffer:

```python
buffer = router.attach(handle_system, key=thread_key)


async def collect(message: Message, **data: object) -> None:
    await buffer.add(message, data)


router.message.register(collect, Command("system"))
router.message.register(collect, buffer.is_buffering)
```

`is_buffering` uses the buffer's own `key`, and is true only while a batch actually holds
messages — a dispatched batch lingers in memory for one idle window, and that does not
count. Use `attach` rather than `buffered` here: `buffered` returns the handler, `attach`
returns the buffer.

Keep in mind that the window is fixed from the first message of a batch and is not
restarted by later ones, so a continuation only joins if it arrives inside `interval`.

## License

MIT
