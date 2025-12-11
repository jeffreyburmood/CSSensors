import asyncio
import json

from aiokafka import AIOKafkaConsumer

from weatherWebsocketResource import AsyncManagedWebsocketResource

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_COMMAND_TOPIC = 'weather-command'
KAFKA_GROUP_ID = 'weather-group'

async def process_websocket(start_event, stop_event, termination_event):
    while not termination_event.is_set():
        await start_event.wait()
        try:
            async with AsyncManagedWebsocketResource('weather') as websocket:
                while not termination_event.is_set() and not stop_event.is_set():
                    try:
                        await asyncio.sleep(2) # handling the data events via the websocket performed in the resource object
                    except Exception as e:
                        print(f'***** An exception occurred during websocket operation, looks like {e}')
                        break
        except Exception as e:
            if termination_event.is_set():
                break
            print(f"WebSocket error: {e}")
            await asyncio.sleep(2)
        await asyncio.sleep(1)

async def process_kafka(start_event, stop_event, termination_event):
    consumer = AIOKafkaConsumer(
        KAFKA_COMMAND_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="KAFKA_GROUP_ID",
        enable_auto_commit=True,
        auto_offset_reset='latest',
        value_deserializer=lambda v: v.decode('utf-8')
    )
    await consumer.start()
    try:
        while not termination_event.is_set():

            async for msg in consumer:
                print(
                    "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp))
                command = msg.value
                print(f'Command received by consumer = {command}')
                should_terminate = await handle_command(command, start_event, stop_event, termination_event)
                if should_terminate:
                    break
    finally:
        await consumer.stop()

async def handle_command(command, start_event, stop_event, termination_event):
    print(f"Received command: {command}")
    # action = command.get("action")
    if command == "start":
        if not start_event.is_set():
            print("Start command received. Starting WebSocket processing.")
            start_event.set()
            stop_event.clear()
    elif command == "stop":
        if not stop_event.is_set():
            print("Stop command event received, shutting down websocket connection")
            stop_event.set()
            start_event.clear()
    elif command == "terminate":
        print("Terminate command received. Shutting down application.")
        termination_event.set()
        return True
    return False

    # await asyncio.gather(
    #    process_websocket(start_event, stop_event, termination_event),
    #    process_kafka(start_event, stop_event, termination_event)

async def main() -> None:
    try:
        start_event = asyncio.Event()
        stop_event = asyncio.Event()
        termination_event = asyncio.Event()
        async with asyncio.TaskGroup() as tg:
            websocket_task = tg.create_task(process_websocket(start_event, stop_event, termination_event))
            kafka_task = tg.create_task(process_kafka(start_event, stop_event, termination_event))

    except asyncio.CancelledError as ex:
        print('All tasks have been cancelled')
        raise ex
    
if __name__ == "__main__":
    asyncio.run(main())
