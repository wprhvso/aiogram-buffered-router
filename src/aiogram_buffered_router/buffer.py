import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine, Hashable
from dataclasses import dataclass, field
from typing import Any, Final

from aiogram.types import Message

from aiogram_buffered_router.keys import thread_key

logger: Final = logging.getLogger(__name__)

BatchHandler = Callable[[list[Message], dict[str, Any]], Awaitable[None]]
KeyBuilder = Callable[[Message], Hashable]


class BufferClosedError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Buffer is closed")


@dataclass(slots=True)
class _Batch:
    messages: list[Message] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    drain: bool = False
    task: asyncio.Task[None] | None = None


class MessageBuffer:
    def __init__(
        self,
        *,
        handler: BatchHandler,
        interval: float = 1.0,
        max_size: int = 0,
        key: KeyBuilder = thread_key,
    ) -> None:
        self._handler: Final = handler
        self._interval: Final = interval
        self._max_size: Final = max_size
        self._key: Final = key
        self._batches: dict[Hashable, _Batch] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    @property
    def pending(self) -> int:
        return sum(len(batch.messages) for batch in self._batches.values())

    async def add(self, message: Message, data: dict[str, Any]) -> None:
        if self._closed:
            raise BufferClosedError

        key = self._key(message)
        batch = self._batches.get(key)
        if batch is None:
            batch = _Batch()
            self._batches[key] = batch

        async with batch.lock:
            batch.messages.append(message)
            batch.data = data
            if batch.task is None:
                batch.task = self._spawn(self._run(key, batch))
            if self._is_full(batch):
                batch.ready.set()

    async def flush(self) -> None:
        for batch in list(self._batches.values()):
            batch.drain = True
            batch.ready.set()
        await self._join()

    async def aclose(self, *, flush: bool = True) -> None:
        self._closed = True
        if flush:
            await self.flush()
        else:
            for task in list(self._tasks):
                _ = task.cancel()
            await self._join()
        self._batches.clear()

    def _is_full(self, batch: _Batch) -> bool:
        return 0 < self._max_size <= len(batch.messages)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _join(self) -> None:
        snapshot = list(self._tasks)
        if snapshot:
            _ = await asyncio.gather(*snapshot, return_exceptions=True)

    async def _wait(self, batch: _Batch) -> None:
        if batch.drain:
            return
        try:
            async with asyncio.timeout(self._interval):
                _ = await batch.ready.wait()
        except TimeoutError:
            return

    def _take(
        self, key: Hashable, batch: _Batch
    ) -> tuple[list[Message], dict[str, Any]]:
        limit = self._max_size if self._max_size > 0 else len(batch.messages)
        messages = batch.messages[:limit]
        del batch.messages[:limit]
        data = dict(batch.data)

        if not batch.messages:
            batch.ready.clear()
            if not messages:
                if self._batches.get(key) is batch:
                    _ = self._batches.pop(key, None)
                batch.task = None
        elif not (batch.drain or self._is_full(batch)):
            batch.ready.clear()

        return messages, data

    async def _run(self, key: Hashable, batch: _Batch) -> None:
        while True:
            await self._wait(batch)

            async with batch.lock:
                messages, data = self._take(key, batch)
                if not messages:
                    return

            try:
                await self._handler(messages, data)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "buffered_batch_handler_failed", extra={"batch_key": key}
                )
