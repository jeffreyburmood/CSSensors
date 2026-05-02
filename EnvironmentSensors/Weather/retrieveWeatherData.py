import asyncio
import os
import nats

from utilities.logger import Logger
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
from dotenv import load_dotenv

from EnvironmentSensors.Weather.weatherWebsocketResource import AsyncManagedWebsocketResource

start_event = asyncio.Event()
stop_event = asyncio.Event()
termination_event = asyncio.Event()

async def process_websocket():
    try:
        while not termination_event.is_set():
            await start_event.wait()
            async with AsyncManagedWebsocketResource('weather') as websocket:
                while not termination_event.is_set() and not stop_event.is_set():
                    await asyncio.sleep(2) # handling the data events via the websocket performed in the resource object
    finally:
        await asyncio.sleep(1)

async def handle_start_msg(msg):
    try:
        logger = Logger.get_logger()
        method_name = handle_start_msg.__name__

        logger.info(f"Received message on subject: {msg.subject}, Starting WebSocket processing.")
        if not start_event.is_set():
            start_event.set()
            stop_event.clear()
            await asyncio.sleep(1)
        else:
            logger.info(f'Received {msg.subject} but websocket processing already started')

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
        raise

async def handle_stop_msg(msg):
    try:
        logger = Logger.get_logger()
        method_name = handle_stop_msg.__name__

        logger.info(f"Received message on subject: {msg.subject}, shutting down websocket connection.")
        if not stop_event.is_set():
            stop_event.set() # this flag being set will cause the websocket processing task to shut down the websocket connection
            start_event.clear()
            await asyncio.sleep(1)
        else:
            logger.info(f'Received {msg.subject} but websocket processing already stopped')

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
        raise

async def handle_terminate_msg(msg):
    try:
        logger = Logger.get_logger()
        method_name = handle_terminate_msg.__name__

        logger.info(f"Received message on subject: {msg.subject}, Shutting down application.")
        stop_event.set()
        start_event.clear()
        await asyncio.sleep(1)
        termination_event.set()

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
        raise

async def process_messages():
    try:
        logger = Logger.get_logger()
        method_name = process_messages.__name__

        nc = await nats.connect("nats://localhost:4222")

        # set up subscribers
        sub_start = await nc.subscribe('cmd.esg.weather.start', cb=handle_start_msg)
        sub_stop = await nc.subscribe('cmd.esg.weather.stop', cb=handle_stop_msg)
        sub_terminate = await nc.subscribe('cmd.esg.weather.terminate', cb=handle_terminate_msg)
        subscriptions = [sub_start, sub_stop, sub_terminate]

        # don't do anymore in this task until a terminate message is received and the event flag is set
        await termination_event.wait()

        # once the terminate message is received, the websocket connection has already been closed so
        # shutdown and clean up the nats client
        logger.info("Shutting down...")
        for sub in subscriptions:
            await sub.unsubscribe()
        await nc.drain()
        await nc.close()
        logger.info("All nats connections closed.")

        # once the nats client is shutdown then cancel out of the task group to end the application
        raise asyncio.CancelledError

    except asyncio.CancelledError:
        raise

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')


class TerminateTaskGroup(Exception):
    """ An exception created and raised to terminate a task group """

async def force_terminate_task_group():
    """ A method used to terminate a task group """
    raise TerminateTaskGroup()

async def main() -> None:
    try:
        logger = Logger.get_logger()
        method_name = main.__name__

        load_dotenv(".env.development")
        # start_event = asyncio.Event()
        # stop_event = asyncio.Event()
        # termination_event = asyncio.Event()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(process_websocket())
            tg.create_task(process_messages())
            # await termination_event.wait()
            # tg.create_task(force_terminate_task_group())

    except* TerminateTaskGroup:
        pass

    except* asyncio.CancelledError as eg:
        # Handle task cancellations (e.g., from termination_event or external cancel)
        logger.error(f'Tasks were cancelled in {method_name}, looks like: ')
        for exc in eg.exceptions:
            logger.error(f"  CancelledError: {exc}")
        pass

    except* Exception as eg:
        # Handle other exceptions raised by any task in the TaskGroup
        logger.error(f"One or more tasks raised an exception in {method_name}, looks like: ")
        for exc in eg.exceptions:
            logger.error(f"  Exception: {repr(exc)}")
        # Optional: re-raise or perform other cleanup/logic here
        raise

    finally:
        # Ensure that the termination event is set to signal shutdown
        logger.info("Application shutdown complete.")

if __name__ == "__main__":
    try:
        logger = Logger.get_logger()
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down.")
