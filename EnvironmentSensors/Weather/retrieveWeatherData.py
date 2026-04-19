import asyncio
import json

from aiokafka.errors import KafkaError

from utilities.logger import Logger
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
    try:
        logger = Logger.get_logger()
        method_name = process_kafka.__name__

        consumer = AIOKafkaConsumer(
            KAFKA_COMMAND_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="weather",
            enable_auto_commit=True,
            auto_offset_reset='latest',
            value_deserializer=lambda v: v.decode('utf-8')
        )
        await consumer.start()

    except KafkaError as e:
        logger.error(f'Kafka error occurred when setting up Consumer in {method_name}, looks like: {e}')
        raise

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while setting up the Consumer, looks like {ex}')
        raise

    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            enable_idempotence=True,
            value_serializer=lambda v: v.encode('utf-8')
        )
        await producer.start()

    except KafkaError as e:
        logger.error(f'Kafka error occurred when setting up Producer in {method_name}, looks like: {e}')
        raise

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while setting up the Producer, looks like {ex}')
        raise

    try:
        while not termination_event.is_set():

            async for msg in consumer:
                logger.info(
                    "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp))
                command = msg.value
                logger.info(f'Command received by consumer = {command}')
                await handle_command(command, start_event, stop_event, termination_event)
                if termination_event.is_set():
                    break

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while looping, looks like {ex}')
        raise

    finally:
        await consumer.stop()
        await producer.stop()

async def handle_command(command, start_event, stop_event, termination_event):
    try:
        logger = Logger.get_logger()
        method_name = handle_command.__name__

        logger.info(f"Received command: {command}")
        # action = command.get("action")
        if command == "start":
            if not start_event.is_set():
                logger.info("Start command received. Starting WebSocket processing.")
                start_event.set()
                stop_event.clear()
        elif command == "stop":
            if not stop_event.is_set():
                logger.info("Stop command event received, shutting down websocket connection")
                stop_event.set()
                start_event.clear()
        elif command == "terminate":
            logger.info("Terminate command received. Shutting down application.")
            stop_event.set()
            await asyncio.sleep(1)
            termination_event.set()
        else:
            logger.info("Unknown command event received, and ignored")

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while processing Kafka commands, looks like {ex}')
        raise


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
        logger = Logger.get_logger()
        method_name = main.__name__

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
        logger.error(f'Tasks were cancelled in {method_name}, looks like: ')
        for exc in eg.exceptions:
            logger.error(f"  CancelledError: {exc}")
        raise

    except* Exception as eg:
        # Handle other exceptions raised by any task in the TaskGroup
        logger.error(f"One or more tasks raised an exception in {method_name}, looks like: ")
        for exc in eg.exceptions:
            logger.error(f"  Exception: {repr(exc)}")
        # Optional: re-raise or perform other cleanup/logic here
        raise

    finally:
        # Ensure that the termination event is set to signal shutdown
        if not termination_event.is_set():
            termination_event.set()
        logger.info("Application shutdown complete.")

if __name__ == "__main__":
    try:
        logger = Logger.get_logger()
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down.")
