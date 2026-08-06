""" this file contains a class for handling core system messages such as start, stop, and terminate. """

from utilities.healthStatus import HealthContext
from utilities.logger import Logger

class CoreMessages:
    
    def __init__(self):
        self.logger = Logger()
        
    async def handle_start_msg(self, msg, health: HealthContext):

        method_name = self.handle_start_msg.__name__

        try:

            self.self.logger.info(f"Received message on subject: {msg.subject}, Starting WebSocket processing.")
            if not start_event.is_set():
                start_event.set()
                stop_event.clear()
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

            self.logger.info(f"Received message on subject: {msg.subject}, shutting down websocket connection.")
            if not stop_event.is_set():
                stop_event.set() # this flag being set will cause the websocket processing task to shut down the websocket connection
                start_event.clear()
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
            stop_event.set()
            start_event.clear()
            nats_shutdown_event.set()
            await asyncio.sleep(1)

        except Exception as ex:
            self.logger.error(f'Exception encountered in {method_name} while processing nats subject, looks like {ex}')
            health.report_error(
                color=HealthColor.RED,
                error_type="HandleMsgError",
                message=f"Exception encounter while handling terminate msg: looks like {ex}",
                component=method_name, subsystem="env",
            )

