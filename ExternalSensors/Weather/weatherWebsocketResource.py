""" This file contains the class and associated methods for managing the weather data websocket in a Context Manager """
import asyncio
import os
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from aioambient import Websocket
from dotenv import load_dotenv

def convert_utc_to_timezone(utc_date: str, tz: str) -> str:
    """
    Converts a python datetime string in UTC to the target IANA time zone.
    :param utc_date: str, e.g. "2025-11-12 14:06:00+00:00"
    :param tz: IANA time zone string, e.g. "America/New_York"
    :return: String of converted datetime in same format
    """
    # Parse the datetime string as UTC and convert to a datetime object
    dt = datetime.strptime(utc_date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    # convert datetime object to target timezone
    dt_target = dt.astimezone(ZoneInfo(tz))

    return dt_target.strftime("%Y-%m-%d %H:%M:%S")

def process_weather_data(current_data):
    """
    Takes the current real time weather data and extracts the relevant data and stores it in the database
    :param current_data: Dictionary of retrieved weather station data
    """
    mac_addr = os.getenv('CABIN_MAC')

    if current_data['macAddress'] == mac_addr:
        local_datetime = convert_utc_to_timezone(current_data['date'], current_data['tz'])
        print(f'local date time = {local_datetime}')
    else:
        pass

# Define a method that should be fired when the websocket client
# connects:
def connect_method():
    """Print a simple "hello" message."""
    print("Client has connected to the websocket")

# Define a method that should be run upon subscribing to the Ambient
# Weather cloud:
def subscribed_method(data):
    """Print the data received upon subscribing."""
    print(f"Subscription data received: {data}")

# Alternatively, define a coroutine handler:
async def data_coroutine(data):
    """Wait for 3 seconds, then print the data received."""
    await asyncio.sleep(3)
    print(f"Data received async: {data}")
    process_weather_data(data)

# Define a method that should be run when the websocket client
# disconnects:
async def disconnect_coroutine(data):
    """Wait for 3 seconds, then print a simple "goodbye" message."""
    await asyncio.sleep(3)
    print("Client has disconnected from the websocket async")


def configure_websocket() -> Websocket:
    """ this method will perform the weather websocket set needed before attempting to connect """
    load_dotenv()
    API_KEY = os.getenv('AMBIENT_API_KEY')
    APP_KEY = os.getenv('AMBIENT_APPLICATION_KEY')

    websocket = Websocket(APP_KEY, API_KEY)
    websocket.on_connect(connect_method)
    websocket.on_subscribed(subscribed_method)
    websocket.async_on_data(data_coroutine)
    websocket.async_on_disconnect(disconnect_coroutine)

    return websocket


class AsyncManagedWebsocketResource:
    """ this class defines a websocket resource to be managed within an async Context Manager """
    def __init__(self, name,
                 simulate_acquire_fail: bool = False,
                 simulate_release_fail: bool = False,
                 suppress_release_exception: bool = False):
        """
        name: identifier for the managed resource (stored and used by enter/exit).
        simulate_acquire_fail: if True, acquisition will raise.
        simulate_release_fail: if True, release will raise.
        suppress_release_exception: if True, __aexit__ will suppress release errors.
        """
        self.name = name
        # self.simulate_acquire_fail = simulate_acquire_fail
        # self.simulate_release_fail = simulate_release_fail
        # self.suppress_release_exception = suppress_release_exception
        # resource will be set in __aenter__ if acquisition succeeds
        self.resource = None

    async def __aenter__(self):
        try:
            self.resource = await self._acquire()
            print(f"[{self.name}] __aenter__ -> acquired: {self.resource}")
            return self.resource
        except Exception as e:
            print(f"[{self.name}] __aenter__ failed: {e}")
            # Re-raise so callers know acquisition failed and the with-block never runs
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Only try to release if resource was acquired
        if self.resource is None:
            print(f"[{self.name}] __aexit__: nothing to release.")
            return False  # don't suppress exceptions from the with-block

        try:
            await self._release()
            print(f"[{self.name}] __aexit__ -> released successfully.")
        except Exception as e:
            print(f"[{self.name}] __aexit__ failed to release: {e}")
            # Decide whether to suppress the release exception
        # Return False to propagate any exception from the with-block (default behavior)
        return False

    async def _acquire(self) -> Websocket:
        # simulate async acquisition work
        print(f"[{self.name}] acquiring (async)...")
        websocket = configure_websocket()
        await websocket.connect()
        return websocket

    async def _release(self):
        # simulate async release work
        print(f"[{self.name}] releasing (async)...")
        await self.resource.disconnect()
        return None
