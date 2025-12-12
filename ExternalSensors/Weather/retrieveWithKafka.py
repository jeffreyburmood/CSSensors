import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from weatherWebsocketResource import AsyncManagedWebsocketResource

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_COMMAND_TOPIC = 'weather-command'
KAFKA_GROUP_ID = 'weather-group'

async def process_websocket(start_event, stop_event, termination_event):
    try:
        while not termination_event.is_set():
            await start_event.wait()
            async with AsyncManagedWebsocketResource('weather') as websocket:
                while not termination_event.is_set() and not stop_event.is_set():
                    await asyncio.sleep(2) # handling the data events via the websocket performed in the resource object
    finally:
        await asyncio.sleep(1)

async def process_kafka(start_event, stop_event, termination_event):
    consumer = AIOKafkaConsumer(
        KAFKA_COMMAND_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="weather",
        enable_auto_commit=True,
        auto_offset_reset='latest',
        value_deserializer=lambda v: v.decode('utf-8')
    )
    await consumer.start()

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_idempotence=True,
        value_serializer=lambda v: v.encode('utf-8')
    )
    await producer.start()

    try:
        while not termination_event.is_set():

            async for msg in consumer:
                print(
                    "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp))
                command = msg.value
                print(f'Command received by consumer = {command}')
                await handle_command(command, start_event, stop_event, termination_event)
                if termination_event.is_set():
                    break

    finally:
        await consumer.stop()
        await producer.stop()

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
        stop_event.set()
        await asyncio.sleep(1)
        termination_event.set()
    else:
        print("Unknown command event received, and ignored")


    # await asyncio.gather(
    #    process_websocket(start_event, stop_event, termination_event),
    #    process_kafka(start_event, stop_event, termination_event)

class TerminateTaskGroup(Exception):
    """ An exception created and raised to terminate a task group """

async def force_terminate_task_group():
    """ A method used to terminate a task group """
    raise TerminateTaskGroup()

async def main() -> None:
    try:
        start_event = asyncio.Event()
        stop_event = asyncio.Event()
        termination_event = asyncio.Event()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(process_websocket(start_event, stop_event, termination_event))
            tg.create_task(process_kafka(start_event, stop_event, termination_event))
            await termination_event.wait()
            tg.create_task(force_terminate_task_group())

    except* TerminateTaskGroup:
        pass

    except* asyncio.CancelledError as eg:
        # Handle task cancellations (e.g., from termination_event or external cancel)
        print("Tasks were cancelled:")
        for exc in eg.exceptions:
            print(f"  CancelledError: {exc}")
        raise

    except* Exception as eg:
        # Handle other exceptions raised by any task in the TaskGroup
        print("One or more tasks raised an exception:")
        for exc in eg.exceptions:
            print(f"  Exception: {repr(exc)}")
        # Optional: re-raise or perform other cleanup/logic here
        raise

    finally:
        # Ensure that the termination event is set to signal shutdown
        if not termination_event.is_set():
            termination_event.set()
        print("Application shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Received KeyboardInterrupt, shutting down.")
