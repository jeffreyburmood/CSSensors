import asyncio
import websockets
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_COMMAND_TOPIC = 'command-events'
WEBSOCKET_SERVER_URL = 'ws://localhost:8765'

async def process_websocket(start_event, termination_event):
    while not termination_event.is_set():
        await start_event.wait()
        try:
            async with websockets.connect(WEBSOCKET_SERVER_URL) as websocket:
                while not termination_event.is_set():
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=65)
                        data = json.loads(message)
                        await handle_websocket_data(data)
                    except asyncio.TimeoutError:
                        print('No data received from WebSocket in the last minute.')
                    except websockets.ConnectionClosed:
                        print('WebSocket connection closed, reconnecting...')
                        break
        except Exception as e:
            if termination_event.is_set():
                break
            print(f"WebSocket error: {e}, retrying in 5 seconds...")
            await asyncio.sleep(5)
        await asyncio.sleep(1)

async def handle_websocket_data(data):
    print(f"Received WebSocket update: {data}")
    # Implement your data processing logic here

async def process_kafka(start_event, termination_event):
    consumer = AIOKafkaConsumer(
        KAFKA_COMMAND_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="my-command-consumers",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            command = msg.value
            should_terminate = await handle_command(command, start_event, termination_event)
            if should_terminate:
                break
    finally:
        await consumer.stop()

async def handle_command(command, start_event, termination_event):
    print(f"Received command: {command}")
    action = command.get("action")
    if action == "start":
        if not start_event.is_set():
            print("Start command received. Starting WebSocket processing.")
            start_event.set()
    elif action == "terminate":
        print("Terminate command received. Shutting down application.")
        termination_event.set()
        return True
    return False

async def main():
    start_event = asyncio.Event()
    termination_event = asyncio.Event()
    await asyncio.gather(
        process_websocket(start_event, termination_event),
        process_kafka(start_event, termination_event)
    )

if __name__ == "__main__":
    asyncio.run(main())
