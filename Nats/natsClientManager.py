import asyncio
import logging
from typing import Callable, Optional
import nats
from nats.aio.client import Client as NATSClient
from nats.aio.subscription import Subscription
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError


logger = logging.getLogger(__name__)


class NATSClientManager:
    """
    A generic NATS client class that manages connections to one or more NATS servers.
    Supports publishing and subscribing to messages with custom callbacks per subscription.
    """

    def __init__(
        self,
        servers: list[str],
        subscriptions: list[dict[str, Callable]] = None,
        max_reconnect_attempts: int = -1,
        reconnect_time_wait: int = 2,
        connect_timeout: int = 10,
        ping_interval: int = 20,
        max_outstanding_pings: int = 3,
        error_cb: Optional[Callable] = None,
    ):
        """
        Initialize the NATS client manager.

        :param servers: List of NATS server URLs (e.g., ["nats://localhost:4222", "nats://localhost:4223"])
        :param subscriptions: List of dicts with keys:
                                - "subject": str - the subject to subscribe to
                                - "callback": Callable - the async callback function to handle messages
                                - "queue_group": str (optional) - queue group name for load balancing
        :param max_reconnect_attempts: Number of reconnect attempts. -1 for unlimited.
        :param reconnect_time_wait: Seconds to wait between reconnect attempts.
        :param connect_timeout: Seconds to wait for initial connection.
        :param ping_interval: Seconds between server pings to check connectivity.
        :param max_outstanding_pings: Max unanswered pings before connection is considered lost.
        :param error_cb: Optional custom async error callback function for the caller to handle errors.

        Recommendation: For applications that need to subscribe to subjects with a queue group
        (e.g., load balancing across multiple instances of the same application), pass the
        "queue_group" key in the subscriptions list. For applications that do not need load
        balancing, omit the "queue_group" key.
        """
        self._servers = servers
        self._subscriptions_config = subscriptions or []
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_time_wait = reconnect_time_wait
        self._connect_timeout = connect_timeout
        self._ping_interval = ping_interval
        self._max_outstanding_pings = max_outstanding_pings
        self._custom_error_cb = error_cb

        self._nc: Optional[NATSClient] = None
        self._subscriptions: list[Subscription] = []
        self._is_connected = False

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    async def _error_cb(self, e: Exception):
        logger.error(f"NATS error: {e}")
        if self._custom_error_cb:
            await self._custom_error_cb(e)

    async def _disconnected_cb(self):
        self._is_connected = False
        logger.warning("Disconnected from NATS server")

    async def _reconnected_cb(self):
        self._is_connected = True
        logger.info(f"Reconnected to NATS server: {self._nc.connected_url.netloc}")

    async def _closed_cb(self):
        self._is_connected = False
        logger.info("NATS connection closed")

    async def connect(self):
        """
        Establish a connection to the NATS server(s) and set up subscriptions.
        """
        try:
            logger.info(f"Connecting to NATS servers: {self._servers}")
            self._nc = await nats.connect(
                servers=self._servers,
                error_cb=self._error_cb,
                disconnected_cb=self._disconnected_cb,
                reconnected_cb=self._reconnected_cb,
                closed_cb=self._closed_cb,
                max_reconnect_attempts=self._max_reconnect_attempts,
                reconnect_time_wait=self._reconnect_time_wait,
                connect_timeout=self._connect_timeout,
                ping_interval=self._ping_interval,
                max_outstanding_pings=self._max_outstanding_pings,
                allow_reconnect=True,
            )
            self._is_connected = True
            logger.info(f"Connected to NATS server: {self._nc.connected_url.netloc}")
            await self._setup_subscriptions()

        except NoServersError:
            logger.error(f"Could not connect to any NATS server in: {self._servers}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during NATS connection: {e}")
            raise

    async def _setup_subscriptions(self):
        """
        Set up subscriptions based on the configuration passed to the constructor.
        """
        for sub_config in self._subscriptions_config:
            subject = sub_config.get("subject")
            callback = sub_config.get("callback")
            queue_group = sub_config.get("queue_group", "")

            if not subject or not callable(callback):
                logger.warning(
                    f"Invalid subscription config, skipping: {sub_config}. "
                    f"Ensure 'subject' and 'callback' keys are provided."
                )
                continue

            try:
                if queue_group:
                    sub = await self._nc.subscribe(subject, queue=queue_group, cb=callback)
                    logger.info(f"Subscribed to subject '{subject}' with queue group '{queue_group}'")
                else:
                    sub = await self._nc.subscribe(subject, cb=callback)
                    logger.info(f"Subscribed to subject '{subject}'")

                self._subscriptions.append(sub)

            except Exception as e:
                logger.error(f"Failed to subscribe to subject '{subject}': {e}")
                raise

    async def publish(self, subject: str, message: bytes | str, headers: Optional[dict] = None):
        """
        Publish a message to a NATS subject.

        :param subject: The subject to publish to.
        :param message: The message payload. If str, it will be encoded to bytes.
        :param headers: Optional dict of headers to include with the message.
        """
        if not self._is_connected or self._nc is None:
            logger.error("Cannot publish: not connected to NATS server")
            raise ConnectionClosedError

        if isinstance(message, str):
            message = message.encode()

        try:
            await self._nc.publish(subject, message, headers=headers)
            await self._nc.flush()
            logger.debug(f"Published message to subject '{subject}'")
        except Exception as e:
            logger.error(f"Failed to publish message to subject '{subject}': {e}")
            raise

    async def disconnect(self):
        """
        Unsubscribe from all subjects and close the NATS connection.
        """
        if self._nc is None:
            logger.warning("Disconnect called but no active NATS connection")
            return

        try:
            for sub in self._subscriptions:
                await sub.unsubscribe()
                logger.info(f"Unsubscribed from subject '{sub.subject}'")

            self._subscriptions.clear()
            await self._nc.drain() # the connection is closed automatically after flushing
            logger.info("NATS connection drained and closed")

        except Exception as e:
            logger.error(f"Error during NATS disconnect: {e}")
            raise

