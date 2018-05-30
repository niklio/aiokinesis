import asyncio
from time import time

import pytest

from aiokinesis.message_accumulator import Message, MessageAccumulator


@pytest.mark.asyncio
@pytest.mark.parametrize('partition_key, value', [
    (1, {'test': 'blah'}),
    (2, {'asdbaisdba': 'oasnfoaibsfa', 'abc': [1, 2, 3]}),
    (3, ['hola', 1, 123414523123]),
    (1, {}),
    ('abceadsasdfnoaisdnfoasindfaos', []),
])
async def test_add_message(partition_key, value):
    # Create message accumulator
    loop = asyncio.get_event_loop()
    accumulator = MessageAccumulator(loop)

    # Add message
    accumulator.add_message(partition_key, value)

    # Check that message is the only thing in the accumulator's deque
    assert len(accumulator._accumulated_messages) == 1
    only_message = accumulator._accumulated_messages[0]
    assert only_message.partition_key
    assert only_message.value == value


@pytest.mark.asyncio
@pytest.mark.parametrize('partition_key, value', [
    (1, {'test': 'blah'}),
    (2, {'asdbaisdba': 'oasnfoaibsfa', 'abc': [1, 2, 3]}),
    (3, ['hola', 1, 123414523123]),
    (1, {}),
    ('abceadsasdfnoaisdnfoasindfaos', []),
])
async def test_anext_one_message(partition_key, value):
    # Create message accumulator
    loop = asyncio.get_event_loop()
    accumulator = MessageAccumulator(loop)

    # Add message
    accumulator.add_message(partition_key, value)

    # Consume one message
    message = await accumulator.__anext__()

    # Check that we got our message back
    assert type(message) == Message
    assert message.partition_key == partition_key
    assert message.value == value


@pytest.mark.asyncio
@pytest.mark.parametrize('partition_key, value', [
    (1, {'test': 'blah'}),
    (2, {'asdbaisdba': 'oasnfoaibsfa', 'abc': [1, 2, 3]}),
    (3, ['hola', 1, 123414523123]),
    (1, {}),
    ('abceadsasdfnoaisdnfoasindfaos', []),
])
async def test_anext_multiple_messages(partition_key, value):
    # Create message accumulator
    loop = asyncio.get_event_loop()
    accumulator = MessageAccumulator(loop)

    # Add some other messages
    messages = (
        (1, {}),
        (2, {}),
        (3, {}),
        (4, {}),
        (5, {}),
    )
    for k, v in messages:
        accumulator.add_message(k, v)

    # Add last message
    accumulator.add_message(partition_key, value)

    # Consume one message
    message = await accumulator.__anext__()

    # Check that we got the first message
    first_message = messages[0]
    assert type(message) == Message
    assert message.partition_key == first_message[0]
    assert message.value == first_message[1]


@pytest.mark.asyncio
async def test_anext_empty():
    # Create message accumulator
    loop = asyncio.get_event_loop()
    accumulator = MessageAccumulator(loop)

    # Try to consume message
    anext_future = accumulator.__anext__()
    try:
        await asyncio.wait_for(anext_future, timeout=1)
        assert False
    except asyncio.TimeoutError:
        pass


@pytest.mark.asyncio
async def test_rate_limit():
    # Create message accumulator
    loop = asyncio.get_event_loop()
    accumulator = MessageAccumulator(loop)

    message_count = 50
    # Fill accumulator with 50 messages
    for i in range(message_count):
        accumulator.add_message(i, {})

    message_yield_times = []
    # Empty accumulator
    async for message in accumulator:
        current_time = float(time())
        message_yield_times.append(current_time)
        assert type(message) == Message

        # Check if accumulator is empty
        if len(message_yield_times) == message_count - 1:
            break

    yields_per_rolling_sec = 5
    # Verify that we never make more than 5 yields per rolling second
    # and that we're making almost 5 yields per rolling second
    for i, yield_time in enumerate(message_yield_times):
        if i < yields_per_rolling_sec:
            continue

        prev_i = i - yields_per_rolling_sec
        prev_yield_time = message_yield_times[prev_i]
        assert yield_time - prev_yield_time > 1
        assert yield_time - prev_yield_time < 1.5
