import boto3
from botocore.exceptions import ClientError

from .utils import rate_limit_per_rolling_second


class AIOKinesisConsumer:
    """
    Async client to consume from a kinesis topic
    """

    def __init__(self, stream_name, loop, region_name='us-east-1',
                 shard_iterator_type='LATEST', starting_sequence_number=None,
                 timestamp=None):

        self._stream_name = stream_name
        self._region_name = region_name
        self._loop = loop

        self._shard_iterator_type = shard_iterator_type
        self._starting_sequence_number = starting_sequence_number
        self._timestamp = timestamp

    async def start(self):
        # Instantiate kinesis client
        self._kinesis_client = boto3.client(
            'kinesis',
            region_name=self._region_name
        )

        # Get shard
        kinesis_stream = self._kinesis_client.describe_stream(
            StreamName=self._stream_name
        )
        shard_id = kinesis_stream['StreamDescription']['Shards'][0]['ShardId']

        # Create a shard iterator
        shard_iterator = self._kinesis_client.get_shard_iterator(
            StreamName=self._stream_name,
            ShardId=shard_id,
            ShardIteratorType=self._shard_iterator_type,
            StartingSequenceNumber=self._starting_sequence_number,
            Timestamp=self._timestamp
        )
        self._next_shard_iterator = shard_iterator['ShardIterator']

    async def __aiter__(self):
        return self

    @rate_limit_per_rolling_second(5)
    async def __anext__(self):
        # Get next record
        try:
            response = self._kinesis_client.get_records(
                ShardIterator=self._next_shard_iterator,
                Limit=1
            )
            self._next_shard_iterator = response['NextShardIterator']
        except ClientError as e:
            raise StopAsyncIteration

        return response

    async def stop(self):
        pass
