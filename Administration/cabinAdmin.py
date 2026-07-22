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
