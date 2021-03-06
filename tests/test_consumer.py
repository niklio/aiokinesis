import asyncio
from time import time
from uuid import uuid4
from datetime import datetime

from mock import MagicMock, patch
import pytest

from aiokinesis import AIOKinesisConsumer


@pytest.mark.asyncio
@pytest.mark.parametrize('stream_name, region_name, shard_iterator_type, \
        starting_sequence_number, timestamp', [
    ('test1', 'us-east-1', 'LATEST', None, None),
    ('test2', 'us-west-1', 'AFTER_SEQUENCE_NUMBER', '123', None),
    ('test3', 'us-central-2', 'AT_SEQUENCE_NUMBER', '456', None),
    ('test3', 'us-central-2', 'AT_TIMESTAMP', None, datetime.min),
])
async def test_consumer_start(stream_name, region_name, shard_iterator_type,
                              starting_sequence_number, timestamp):
    with patch('boto3.client') as mock_boto3_client:
        # Setup mock
        mock_kinesis_client = MagicMock()
        mock_boto3_client.return_value = mock_kinesis_client

        # Instantiate consumer
        loop = asyncio.get_event_loop()
        consumer = AIOKinesisConsumer(
            stream_name,
            loop,
            region_name=region_name,
            shard_iterator_type=shard_iterator_type,
            starting_sequence_number=starting_sequence_number,
            timestamp=timestamp
        )

        # Starting consumer should create a kinesis consumer exactly once.
        # It should describe stream exactly once to get shards.
        # Lastly it should call `get_shard_iterator` to get at least one
        # shard iterator
        await consumer.start()
        mock_boto3_client.assert_called_once_with(
            'kinesis',
            region_name=region_name
        )
        mock_kinesis_client.describe_stream.assert_called_once_with(
            StreamName=stream_name
        )
        call_kwargs = mock_kinesis_client\
            .get_shard_iterator.call_args_list[0][1]
        assert call_kwargs['StreamName'] == stream_name
        assert 'ShardId' in call_kwargs
        assert call_kwargs['ShardIteratorType'] == shard_iterator_type
        assert call_kwargs.get('StartingSequenceNumber') == \
            starting_sequence_number
        assert call_kwargs.get('Timestamp') == timestamp


@pytest.mark.asyncio
@pytest.mark.parametrize('shard_iterator', [
    ('test1'),
    ('test2'),
    ('test3'),
])
async def test_consumer_anext(shard_iterator):
    with patch('boto3.client') as mock_boto3_client:
        # Setup mock
        mock_kinesis_client = MagicMock()
        mock_kinesis_client.get_shard_iterator.return_value = {
            "ShardIterator": shard_iterator,
        }
        mock_boto3_client.return_value = mock_kinesis_client

        # Instantiate consumer
        loop = asyncio.get_event_loop()
        consumer = AIOKinesisConsumer(
            'test-stream-name',
            loop,
        )

        # Start consumer
        await consumer.start()

        # Calling anext on consumer shoud call get one with shard_iterator
        await consumer.__anext__()
        mock_kinesis_client.get_records.assert_called_once_with(
            ShardIterator=shard_iterator,
            Limit=1
        )


@pytest.mark.asyncio
async def test_consumer_rate_limit():
    records_request_times = []

    def mock_get_shard_iterator(*args, **kwargs):
        shard_iterator = str(uuid4())
        return {"ShardIterator": shard_iterator}

    def mock_get_records(*args, **kwargs):
        current_time = float(time())
        records_request_times.append(current_time)
        shard_iterator = str(uuid4())
        return {"NextShardIterator": shard_iterator}

    with patch('boto3.client') as mock_boto3_client:
        # Setup mock
        mock_kinesis_client = MagicMock()
        mock_kinesis_client.get_shard_iterator = mock_get_shard_iterator
        mock_boto3_client.return_value = mock_kinesis_client
        mock_kinesis_client.get_records = mock_get_records

        # Instantiate consumer
        loop = asyncio.get_event_loop()
        consumer = AIOKinesisConsumer(
            'test-stream-name',
            loop
        )

        # Start consumer
        await consumer.start()

        # Async iteration
        async for record in consumer:
            assert 'NextShardIterator' in record
            if len(records_request_times) == 50:
                break

        requests_per_rolling_sec = 5
        # Verify that we never make more than 5 requests per rolling second
        # and that we're making almost 5 requests per rolling second
        for i, request_time in enumerate(records_request_times):
            if i < requests_per_rolling_sec:
                continue

            prev_i = i - requests_per_rolling_sec
            prev_request_time = records_request_times[prev_i]
            assert request_time - prev_request_time > 1
            assert request_time - prev_request_time < 1.5
