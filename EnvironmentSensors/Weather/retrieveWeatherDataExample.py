""" This file contains code to register with the AWN websocket to receive weather data updates from the
    cabin weather station. The code utilizes the asyncio-based aioamobient library.
    https://github.com/bachya/aioambient/tree/dev """

import asyncio
import os

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from aioambient import Websocket

from dotenv import load_dotenv

async def main() -> None:
    load_dotenv()
    API_KEY = os.getenv('AMBIENT_API_KEY')
    APP_KEY = os.getenv('AMBIENT_APPLICATION_KEY')
    mac_addr = os.getenv('CABIN_MAC')

    websocket = Websocket(APP_KEY, API_KEY)

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
        if current_data['macAddress'] == mac_addr:
            local_datetime = convert_utc_to_timezone(current_data['date'], current_data['tz'])
            print(f'local date time = {local_datetime}')
        else:
            pass



    # Note that you can watch multiple API keys at once:
    # websocket = Websocket("YOUR APPLICATION KEY", ["<API KEY 1>", "<API KEY 2>"])

    # Define a method that should be fired when the websocket client
    # connects:
    def connect_method():
        """Print a simple "hello" message."""
        print("Client has connected to the websocket")

    # websocket.on_connect(connect_method)

    # Alternatively, define a coroutine handler:
    async def connect_coroutine():
        """Waits for 3 seconds, then print a simple "hello" message."""
        await asyncio.sleep(3)
        print("Client has connected to the websocket async")

    websocket.async_on_connect(connect_coroutine)

    # Define a method that should be run upon subscribing to the Ambient
    # Weather cloud:
    def subscribed_method(data):
        """Print the data received upon subscribing."""
        print(f"Subscription data received: {data}")

    # websocket.on_subscribed(subscribed_method)

    # Alternatively, define a coroutine handler:
    async def subscribed_coroutine(data):
        """Waits for 3 seconds, then print the incoming data."""
        await asyncio.sleep(3)
        print(f"Subscription data received async: {data}")

    websocket.async_on_subscribed(subscribed_coroutine)

    # Define a method that should be run upon receiving data:
    def data_method(data):
        """Print the data received."""
        print(f"Data received: {data}")

    # websocket.on_data(data_method)

    # Alternatively, define a coroutine handler:
    async def data_coroutine(data):
        """Wait for 3 seconds, then print the data received."""
        await asyncio.sleep(3)
        print(f"Data received async: {data}")
        process_weather_data(data)

    websocket.async_on_data(data_coroutine)

    # Define a method that should be run when the websocket client
    # disconnects:
    def disconnect_method(data):
        """Print a simple "goodbye" message."""
        print("Client has disconnected from the websocket")

    # websocket.on_disconnect(disconnect_method)

    # Alternatively, define a coroutine handler:
    async def disconnect_coroutine(data):
        """Wait for 3 seconds, then print a simple "goodbye" message."""
        await asyncio.sleep(3)
        print("Client has disconnected from the websocket async")

    websocket.async_on_disconnect(disconnect_coroutine)

    # Connect to the websocket:
    await websocket.connect()

    await asyncio.sleep(300)

    # At any point, disconnect from the websocket:
    await websocket.disconnect()


asyncio.run(main())