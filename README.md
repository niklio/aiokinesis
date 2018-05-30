AIOKinesis
==========

Asyncio client library for AWS Kinesis

AIOKinesisProducer
------------------
Usage:
```python
 import asycio
 from aiokinesis import AIOKinesisProducer

 async def send_message():
     loop = asyncio.get_event_loop()
     producer = AIOKinesisProducer('my-stream-name', loop, region_name='us-east-1')
     await producer.start()

     producer.send('partition-key', {'data': 'blah'})

     await asyncio.sleep(1)
     await producer.stop()

 loop.run_until_complete(send_message())
```
Limitations:
   - Stopping the producer before all messages are sent will prevent in flight messages from being sent
   - AIOKinesis only supports one shard so the producer is rate limited to 5 requests per rolling second

AIOKinesisConsumer
------------------
Usage:
```python
 import asyncio
 from aiokinesis import AIOKinesisConsumer

 async def get_messages():
     loop = asyncio.get_event_loop()
     consumer = AIOKinesisConsumer('my-stream-name', loop, region_name='us-east-1')
     await consumer.start()

     try:
         async for message in consumer:
             print("Consumed message: ", message)
     except KeyboardInterrupt:
             await consumer.stop()

 loop.run_until_complete()
```
Limitations:
   - AIOKinesis only supports one shard so the consumer is rate limited to 5 requests per rolling second
