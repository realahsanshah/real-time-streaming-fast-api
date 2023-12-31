from fastapi import FastAPI, WebSocket
import asyncio
from aiokafka import AIOKafkaConsumer

app = FastAPI()

async def create_consumer():
    return AIOKafkaConsumer("my-topic",
                            bootstrap_servers="localhost:9092",
                            group_id="my-group")

consumer = asyncio.run(create_consumer())
print("Consumer created", consumer)

async def consume_messages():
    async for msg in consumer:
        message = msg.value.decode("utf-8")
        print("Received message:", message)

async def start_app():
    await consumer.start()
    await app.start_task(consume_messages)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            print(data)
            # ... handle incoming WebSocket messages ...
    except websocket.ConnectionClosed:
        pass

# if __name__ == "__main__":
#     asyncio.run(start_app())
asyncio.run(start_app())