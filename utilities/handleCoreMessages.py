""" this file contains a class for handling core system messages such as start, stop, and terminate. """
import asyncio

from utilities.healthStatus import HealthContext, HealthColor
from utilities.logger import Logger

class CoreMessages:
    
    def __init__(self, start_event, stop_event, nats_shutdown_event):
        self.logger = Logger.get_logger()
        self.start_event = start_event
        self.stop_event = stop_event
        self.nats_shutdown_event = nats_shutdown_event

    async def handle_start_msg(self, msg, health: HealthContext):

        method_name = self.handle_start_msg.__name__

        try:

            self.logger.info(f"Received message on subject: {msg.subject}, Starting processing.")
            if not self.start_event.is_set():
                self.start_event.set()
                self.stop_event.clear()
                await asyncio.sleep(1)
            else:
                self.logger.info(f'Received {msg.subject} but websocket processing already started')

        except Exception as ex:
            self.logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
            health.report_error(
                color=HealthColor.RED,
                error_type="HandleMsgError",
                message=f"Exception encounter while handling start msg: looks like {ex}",
                component=method_name, subsystem="env",
            )

    async def handle_stop_msg(self, msg, health: HealthContext):

        method_name = self.handle_stop_msg.__name__

        try:

            self.logger.info(f"Received message on subject: {msg.subject}, shutting down connection.")
            if not self.stop_event.is_set():
                self.stop_event.set() # this flag being set will cause the websocket processing task to shut down the websocket connection
                self.start_event.clear()
                await asyncio.sleep(1)
            else:
                self.logger.info(f'Received {msg.subject} but websocket processing already stopped')

        except Exception as ex:
            self.logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
            health.report_error(
                color=HealthColor.RED,
                error_type="HandleMsgError",
                message=f"Exception encounter while handling stop msg: looks like {ex}",
                component=method_name, subsystem="env",
            )

    async def handle_terminate_msg(self, msg, health: HealthContext):

        method_name = self.handle_terminate_msg.__name__

        try:

            self.logger.info(f"Received message on subject: {msg.subject}, Shutting down application.")
            self.stop_event.set()
            self.start_event.clear()
            self.nats_shutdown_event.set()
            await asyncio.sleep(1)

        except Exception as ex:
            self.logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
            health.report_error(
                color=HealthColor.RED,
                error_type="HandleMsgError",
                message=f"Exception encounter while handling terminate msg: looks like {ex}",
                component=method_name, subsystem="env",
            )

    async def on_error(self, e, health: HealthContext):
        method_name = self.on_error.__name__
        health.report_error(
            color=HealthColor.RED,
            error_type="NATSClientError",
            message=f"Failed to establish NATS client: {e}",
            component=method_name, subsystem="env",
        )
        self.logger.error(f"Application received the following NATS error: {e}")
