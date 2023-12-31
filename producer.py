import os
from dotenv import load_dotenv
import asyncio
from aiokafka import AIOKafkaProducer


load_dotenv()

kafka_url = os.getenv("KAFKA_URL")

async def produce_messages():
    producer = AIOKafkaProducer(bootstrap_servers=kafka_url)  
    await producer.start()

    try:
        for i in range(10):
            message = f"Message {i}"
            await producer.send_and_wait("my-topic", message.encode("utf-8"))
    finally:
        await producer.stop()

asyncio.run(produce_messages())