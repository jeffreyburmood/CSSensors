""" this file contains the code used to perform the various administration functions for the
    CSSensors system """
import asyncio
import functools
from datetime import datetime

from Nats.natsClientManager import NATSClientManager
from utilities.healthStatus import HealthContext
from utilities.logger import Logger

from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.interval import IntervalTrigger

start_event = asyncio.Event()
stop_event = asyncio.Event()
termination_event = asyncio.Event()
nats_shutdown_event = asyncio.Event()
scheduler = AsyncIOScheduler()

def tick():
    print("Hello, the time is", datetime.now())

async def perform_scheduled_tasks(health):

    # engine = create_async_engine(
    #     "postgresql+asyncpg://postgres:secret@localhost/testdb"
    # )
    # data_store = SQLAlchemyDataStore(engine)

    scheduler.add_job(tick, 'interval', seconds=5)
    scheduler.start()

async def process_messages(health: HealthContext):

    method_name = process_messages.__name__

    try:

        # set up nats servers and connect to the nats cluster
        servers = ['nats://nats-server-3:4222', 'nats://nats-server-4:4222']

        # bind a health parameter to the callback functions so they can handle health context correctly
        bound_handle_start_msg = functools.partial(handle_start_msg, health=health)
        bound_handle_stop_msg = functools.partial(handle_stop_msg, health=health)
        bound_handle_terminate_msg = functools.partial(handle_terminate_msg, health=health)
        bound_on_error = functools.partial(on_error, health=health)
        sub_start = {'subject': 'cmd.env.weather.start', 'callback': bound_handle_start_msg}
        sub_stop = {'subject': 'cmd.env.weather.stop', 'callback': bound_handle_stop_msg}
        sub_terminate = {'subject': 'cmd.env.weather.terminate', 'callback': bound_handle_terminate_msg}
        subscriptions = [sub_start, sub_stop, sub_terminate]

        nc = NATSClientManager(
            servers=servers,
            subscriptions=subscriptions,
            error_cb=bound_on_error,
        )

        await nc.connect()

        # don't do anymore in this task until a terminate message is received and the shutdown event flag is set
        await nats_shutdown_event.wait()

        # once the terminate message is received, shutdown and clean up the nats client
        logger.info("Shutting down nats client connections...")
        await nc.disconnect()
        logger.info("All nats connections closed.")

        termination_event.set()

        # once the nats client is shutdown then cancel out of the task group to end the application
        # raise asyncio.CancelledError

    # except asyncio.CancelledError:
    #     raise

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise


class TerminateTaskGroup(Exception):
    """ An exception created and raised to terminate a task group """

async def force_terminate_task_group():
    """ A method used to terminate a task group """
    raise TerminateTaskGroup()

async def main() -> None:

    method_name = main.__name__

    try:

        health = HealthContext()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(perform_scheduled_tasks(health))
            #tg.create_task(process_messages(health))
            await termination_event.wait()
            tg.create_task(force_terminate_task_group())

    except* KeyboardInterrupt:
        scheduler.shutdown(wait=False)

    except* TerminateTaskGroup:
        pass

    except* asyncio.CancelledError as eg:
        # Handle task cancellations (e.g., from termination_event or external cancel)
        scheduler.shutdown(wait=False)
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
    logger = Logger.get_logger()
    try:
        asyncio.run(main())

    except Exception as ex:
        logger.error(f"Exception encountered trying to start main(), looks like {ex}")
