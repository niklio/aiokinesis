import asyncio
from collections import deque

from .utils import rate_limit_per_rolling_second


class Message:
    def __init__(self, partition_key, value):
        self.partition_key = partition_key
        self.value = value


class MessageAccumulator:
    def __init__(self, loop):
        self._loop = loop

        self._message_future = loop.create_future()
        self._accumulated_messages = deque()

    async def _await_message_future(self):
        if not self._message_future.done():
            futures = [self._message_future]
            finished_tasks, _ = await asyncio.wait(
                futures,
                loop=self._loop,
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in finished_tasks:
                task.result()

    async def __aiter__(self):
        return self

    @rate_limit_per_rolling_second(5)
    async def __anext__(self):
        # Await message future
        if len(self._accumulated_messages) == 0:
            await self._await_message_future()

        # Yield next record
        message = self._accumulated_messages.pop()
        return message

    def add_message(self, partition_key, value):
        new_message = Message(partition_key, value)
        self._accumulated_messages.appendleft(new_message)

        if not self._message_future.done():
            self._message_future.set_result(None)
        self._message_future = self._loop.create_future()
