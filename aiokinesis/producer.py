import asyncio
from asyncio import ensure_future
import json

import boto3

from .message_accumulator import MessageAccumulator


class AIOKinesisProducer:
    """
    Async client to produce to a kinesis topic
    """

    def __init__(self, stream_name, loop, region_name='us-east-1'):
        self._stream_name = stream_name
        self._region_name = region_name
        self._loop = loop

        self._message_accumulator = MessageAccumulator(loop)
        self._outstanding_tasks = set()

    async def start(self):
        # Instantiate kinesis client
        self._kinesis_client = boto3.client(
            'kinesis',
            region_name=self._region_name
        )

        # Start sender routine
        self._sender_task = ensure_future(
            self._sender_routine(),
            loop=self._loop
        )

    async def _send_produce_request(self, partition_key, message):
        self._kinesis_client.put_record(
            StreamName=self._stream_name,
            Data=message,
            PartitionKey=partition_key
        )

    async def _sender_routine(self):
        async for message in self._message_accumulator:
            _produce_request_future = self._send_produce_request(
                message.partition_key,
                message.value
            )
            task = ensure_future(
                _produce_request_future,
                loop=self._loop
            )
            self._outstanding_tasks.add(task)

        if self._outstanding_tasks:
            finished_tasks, _ = await asyncio.wait(
                self._outstanding_tasks,
                return_when=asyncio.FIRST_COMPLETED,
                loop=self._loop
            )

            for task in finished_tasks:
                task.result()

            self._outstanding_tasks -= finished_tasks

    async def send(self, partition_key, value):
        self._message_accumulator.add_message(
            partition_key,
            json.dumps(value)
        )

    async def stop(self):
        self._sender_task.cancel()
        if len(self._outstanding_tasks):
            self._loop.run_until_complete(
                asyncio.wait(self._outstanding_tasks)
            )

        self._loop.stop()
