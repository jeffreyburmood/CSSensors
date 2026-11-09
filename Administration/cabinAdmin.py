""" this file contains the code used to perform the various administration functions for the
    CSSensors system """
import asyncio
import functools
import os
from datetime import datetime

import pytz
from apscheduler.schedulers.base import STATE_RUNNING, STATE_PAUSED
from nats.extra import request_many

from dotenv import load_dotenv
from httpx import AsyncClient, HTTPStatusError
from Nats.natsClientManager import NATSClientManager
from utilities.handleCoreMessages import CoreMessages
from SensorDataMgmt.adminDataModel import HealthContext, HealthColor
from utilities.logger import logger

# Currently using version 3.11.x for aspscheduler, the 4.x versions (master in github) are PRE-RELEASE!
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nats.client import connect

nc = NATSClientManager()

start_event = asyncio.Event()
stop_event = asyncio.Event()
termination_event = asyncio.Event()
nats_shutdown_event = asyncio.Event()
scheduler = AsyncIOScheduler(logger=logger)

load_dotenv()

async def add_healthcheck_data_to_database(new_healthcheck_data: HealthContext, health: HealthContext) -> None:
    """ the coroutine makes the actual fastapi call to add the new basement data to the db"""
    method_name = add_healthcheck_data_to_database.__name__

    try:
        db_url = os.getenv('NEO4J_ADMIN_API_URL')

        async with AsyncClient() as client:
            response = await client.post(db_url+'/add-new-health-check-data', json=new_healthcheck_data.model_dump())
            response.raise_for_status()
            logger.info(f'request to add new health check data completed successfully!')

    except HTTPStatusError as http_error:
        logger.error(f'Http error status returned for {method_name}, looks like {http_error}')
        health.report_error(
            color=HealthColor.YELLOW,
            error_type="WebSocketContextManagerError",
            message=f"HTTP Exception encountered from data server: looks like {http_error}"
        )

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        health.report_error(
            color=HealthColor.YELLOW,
            error_type="WebSocketContextManagerError",
            message=f"Exception encountered while adding basement data to database: looks like {ex}"
        )

async def health_check(health: HealthContext):
    method_name = health_check.__name__

    try:
        logger.info("Performing another health check")

        nats_client = await connect('nats://nats-server-4:4222')
        health_responses = await request_many(nats_client, 'rst.sys.sys.healthcheck', b'go')
        # health_responses = await nc.request_many("rst.sys.sys.healthcheck", "perform health check")

        logger.info(f'received {health_responses.messages_received} message responses to health check request')

        received = [message.data async for message in health_responses]

        for response in received:
            health_data = eval(response.decode())

            await add_healthcheck_data_to_database(health_data, health)

            if health_data["error_count"] == 0:
                logger.info(f"Health check response - no errors")
            else:
                logger.warning(f"Health check response with errors = {health_data}")

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} while performing periodic health checks, looks like {ex}")


async def perform_scheduled_tasks(health):
    """ RUnState = starting, started, stopping, stopped """

    # engine = create_async_engine(
    #     "postgresql+asyncpg://postgres:secret@localhost/testdb"
    # )
    # data_store = SQLAlchemyDataStore(engine)

    bound_health_check_method = functools.partial(health_check, health=health)

    scheduler.add_job(bound_health_check_method, 'interval', seconds=300, id='healthcheck')

    try:
        while not termination_event.is_set():
            if start_event.is_set():
                if scheduler.state != STATE_RUNNING:
                    if scheduler.state != STATE_PAUSED:
                        scheduler.start()
                    else:
                        scheduler.resume()
            else:
                if stop_event.is_set():
                    if scheduler.state == STATE_RUNNING:
                        scheduler.pause()

            await asyncio.sleep(5)  # handling the data events via the websocket performed in the resource object

    finally:
        await asyncio.sleep(1)

async def process_messages(health: HealthContext):

    method_name = process_messages.__name__

    try:

        # set up nats servers and connect to the nats cluster
        servers = ['nats://nats-server-3:4222', 'nats://nats-server-4:4222']

        # set up core messages
        coreMessages = CoreMessages(start_event, stop_event, nats_shutdown_event)

        # bind a health parameter to the callback functions so they can handle health context correctly
        bound_handle_start_msg = functools.partial(coreMessages.handle_start_msg, health=health)
        bound_handle_stop_msg = functools.partial(coreMessages.handle_stop_msg, health=health)
        bound_handle_terminate_msg = functools.partial(coreMessages.handle_terminate_msg, health=health)
        bound_on_error = functools.partial(coreMessages.on_error, health=health)
        sub_start = {'subject': 'cmd.adm.health.start', 'callback': bound_handle_start_msg}
        sub_stop = {'subject': 'cmd.adm.health.stop', 'callback': bound_handle_stop_msg}
        sub_terminate = {'subject': 'cmd.adm.health.terminate', 'callback': bound_handle_terminate_msg}
        subscriptions = [sub_start, sub_stop, sub_terminate]

        logger.info('Connecting client to nats server .....')
        await nc.connect(
            servers=servers,
            subscriptions=subscriptions,
            error_cb=bound_on_error,
        )

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

        # set up the HealthContext object with the specific information to this component
        data = {
            "subsystem": "admin",
            "component": "healthcheck",
            "last_reset": datetime.now(tz=pytz.timezone('America/Denver')).isoformat()
        }
        health = HealthContext(**data)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(perform_scheduled_tasks(health))
            tg.create_task(process_messages(health))
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
    try:
        asyncio.run(main())

    except Exception as ex:
        logger.error(f"Exception encountered trying to start main(), looks like {ex}")
