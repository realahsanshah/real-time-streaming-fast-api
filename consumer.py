import os
from dotenv import load_dotenv
import asyncio
from aiokafka import AIOKafkaConsumer


load_dotenv()

kafka_url = os.getenv("KAFKA_URL")

async def consume_messages():
    consumer = AIOKafkaConsumer("my-topic", bootstrap_servers="localhost:9092", group_id="my-group")
    await consumer.start()

    try:
        async for msg in consumer:
            print("Received message:", msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    finally:
        await consumer.stop()

asyncio.run(consume_messages())