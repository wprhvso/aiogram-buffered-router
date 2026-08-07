from collections.abc import Callable
from typing import Any, Final

from aiogram import Router
from aiogram.dispatcher.event.handler import CallbackType
from aiogram.types import Message

from aiogram_buffered_router.buffer import BatchHandler, KeyBuilder, MessageBuffer
from aiogram_buffered_router.keys import thread_key


class BufferedRouter(Router):
    def __init__(
        self,
        *,
        name: str | None = None,
        interval: float = 1.0,
        max_size: int = 0,
        key: KeyBuilder = thread_key,
    ) -> None:
        super().__init__(name=name or "buffered")
        self._interval: Final = interval
        self._max_size: Final = max_size
        self._key: Final = key
        self._buffers: list[MessageBuffer] = []

    @property
    def pending(self) -> int:
        return sum(buffer.pending for buffer in self._buffers)

    def buffered(
        self,
        *filters: CallbackType,
        interval: float | None = None,
        max_size: int | None = None,
        key: KeyBuilder | None = None,
    ) -> Callable[[BatchHandler], BatchHandler]:
        def wrapper(handler: BatchHandler) -> BatchHandler:
            buffer = self.attach(handler, interval=interval, max_size=max_size, key=key)

            async def entry(message: Message, **data: Any) -> None:
                await buffer.add(message, data)

            _ = self.message.register(entry, *filters)
            return handler

        return wrapper

    def attach(
        self,
        handler: BatchHandler,
        *,
        interval: float | None = None,
        max_size: int | None = None,
        key: KeyBuilder | None = None,
    ) -> MessageBuffer:
        buffer = MessageBuffer(
            handler=handler,
            interval=self._interval if interval is None else interval,
            max_size=self._max_size if max_size is None else max_size,
            key=self._key if key is None else key,
        )
        self._buffers.append(buffer)
        return buffer

    async def flush(self) -> None:
        for buffer in self._buffers:
            await buffer.flush()

    async def aclose(self, *, flush: bool = True) -> None:
        for buffer in self._buffers:
            await buffer.aclose(flush=flush)
