import asyncio
import json

from mock import MagicMock, patch
import pytest

from aiokinesis import AIOKinesisProducer


@pytest.mark.asyncio
@pytest.mark.parametrize('stream_name, region_name', [
    ('test1', 'us-east-1'),
    ('test2', 'us-west-1'),
    ('test3', 'us-central-5'),
])
async def test_producer_start(stream_name, region_name):
    with patch('boto3.client') as mock_boto3_client:
        # Instantiate consumer
        loop = asyncio.get_event_loop()
        producer = AIOKinesisProducer(
            stream_name,
            loop,
            region_name=region_name
        )

        # Start producer
        await producer.start()

        # Starting consumer should create kinesis client exactly once
        mock_boto3_client.assert_called_once_with(
            'kinesis',
            region_name=region_name
        )

        await producer.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize('stream_name, partition_key, value', [
    ('test1', 1, {'test': 'blah'}),
    ('test2', 2, {'asdbaisdba': 'oasnfoaibsfa', 'abc': [1, 2, 3]}),
    ('test5', 3, ['hola', 1, 123414523123]),
    ('abacoisnfa', 1, {}),
    ('cat', 'abceadsasdfnoaisdnfoasindfaos', []),
])
async def test_producer_add_message(stream_name, partition_key, value):
    with patch('boto3.client'):
        # Instantiate consumer
        loop = asyncio.get_event_loop()
        producer = AIOKinesisProducer(
            stream_name,
            loop,
        )
        mock_add_message = MagicMock()
        producer._message_accumulator.add_message = mock_add_message

        # Start producer
        await producer.start()

        # Send stuff
        await producer.send(partition_key, value)

        mock_add_message.assert_called_once_with(
            partition_key,
            json.dumps(value),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize('stream_name, partition_key, value', [
    ('test1', 1, {'test': 'blah'}),
    ('test2', 2, {'asdbaisdba': 'oasnfoaibsfa', 'abc': [1, 2, 3]}),
    ('test5', 3, ['hola', 1, 123414523123]),
    ('abacoisnfa', 1, {}),
    ('cat', 'abceadsasdfnoaisdnfoasindfaos', []),
])
async def test_producer_send_message(stream_name, partition_key, value):
    with patch('boto3.client') as mock_boto3_client:
        # Setup mock
        mock_kinesis_client = MagicMock()
        mock_boto3_client.return_value = mock_kinesis_client

        # Instantiate consumer
        loop = asyncio.get_event_loop()
        producer = AIOKinesisProducer(
            stream_name,
            loop,
        )

        # Start producer
        await producer.start()

        # Send stuff
        await producer.send(partition_key, value)

        # Wait for the sender routine
        await asyncio.sleep(0.1)

        mock_kinesis_client.put_record.assert_called_once_with(
            StreamName=stream_name,
            Data=json.dumps(value),
            PartitionKey=partition_key
        )
