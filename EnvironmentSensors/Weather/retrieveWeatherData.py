import asyncio
import functools

from EnvironmentSensors.Weather.weatherWebsocketResource import AsyncManagedWebsocketResource
from Nats.natsClientManager import NATSClientManager
from utilities.healthStatus import HealthContext, HealthColor
from utilities.logger import Logger

start_event = asyncio.Event()
stop_event = asyncio.Event()
termination_event = asyncio.Event()
nats_shutdown_event = asyncio.Event()

async def process_websocket(health: HealthContext):
    try:
        while not termination_event.is_set():
            await start_event.wait()
            async with AsyncManagedWebsocketResource('weather', health=health) as websocket:
                while not termination_event.is_set() and not stop_event.is_set():
                    await asyncio.sleep(2)  # handling the data events via the websocket performed in the resource object
    finally:
        await asyncio.sleep(1)

async def handle_start_msg(msg, health: HealthContext):

    method_name = handle_start_msg.__name__

    try:

        logger.info(f"Received message on subject: {msg.subject}, Starting WebSocket processing.")
        if not start_event.is_set():
            start_event.set()
            stop_event.clear()
            await asyncio.sleep(1)
        else:
            logger.info(f'Received {msg.subject} but websocket processing already started')

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
        health.report_error(
            color=HealthColor.RED,
            error_type="HandleMsgError",
            message=f"Exception encounter while handling start msg: looks like {ex}",
            component=method_name,
        )

async def handle_stop_msg(msg, health: HealthContext):

    method_name = handle_stop_msg.__name__

    try:

        logger.info(f"Received message on subject: {msg.subject}, shutting down websocket connection.")
        if not stop_event.is_set():
            stop_event.set() # this flag being set will cause the websocket processing task to shut down the websocket connection
            start_event.clear()
            await asyncio.sleep(1)
        else:
            logger.info(f'Received {msg.subject} but websocket processing already stopped')

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
        health.report_error(
            color=HealthColor.RED,
            error_type="HandleMsgError",
            message=f"Exception encounter while handling stop msg: looks like {ex}",
            component=method_name,
        )

async def handle_terminate_msg(msg, health: HealthContext):

    method_name = handle_terminate_msg.__name__

    try:

        logger.info(f"Received message on subject: {msg.subject}, Shutting down application.")
        stop_event.set()
        start_event.clear()
        nats_shutdown_event.set()
        await asyncio.sleep(1)

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
        health.report_error(
            color=HealthColor.RED,
            error_type="HandleMsgError",
            message=f"Exception encounter while handling terminate msg: looks like {ex}",
            component=method_name,
        )


async def on_error(e, health: HealthContext):
    method_name = on_error.__name__
    health.report_error(
        color=HealthColor.RED,
        error_type="NATSClientError",
        message=f"Failed to establish NATS client: {e}",
        component=method_name,
    )
    logger.error(f"Application received the following NATS error: {e}")

async def process_messages(health: HealthContext):

    method_name = process_messages.__name__

    try:

        # load_dotenv()

        # nats_server_url = os.getenv('NATS_SERVER')
        # nc = await nats.connect(nats_server_url)

        # set up nats servers and connect to the nats cluster
        servers = ['nats://nats-server-1:4222', 'nats://nats-server-2:4222']

        # set up subscribers
        # sub_start = await nc.subscribe('cmd.env.weather.start', cb=handle_start_msg)
        # sub_stop = await nc.subscribe('cmd.env.weather.stop', cb=handle_stop_msg)
        # sub_terminate = await nc.subscribe('cmd.env.weather.terminate', cb=handle_terminate_msg)

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

        # don't do anymore in this task until a terminate message is received and the event flag is set
        await nats_shutdown_event.wait()

        # once the terminate message is received, the websocket connection has already been closed so
        # shutdown and clean up the nats client
        logger.info("Shutting down nats client connections...")
        #for sub in subscriptions:
        #    await sub.unsubscribe()
        #await nc.drain()
        #await nc.close()
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
            tg.create_task(process_websocket(health))
            tg.create_task(process_messages(health))
            await termination_event.wait()
            tg.create_task(force_terminate_task_group())

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
    logger = Logger.get_logger()
    try:
        asyncio.run(main())

    except Exception as ex:
        logger.error(f"Exception encountered trying to start main(), looks like {ex}")
