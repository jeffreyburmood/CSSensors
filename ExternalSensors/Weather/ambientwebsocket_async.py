""" This file contains code to utilize the aioamobient library.
    https://github.com/bachya/aioambient/tree/dev """

import asyncio
import os

from aiohttp import ClientSession

from aioambient import Websocket

from dotenv import load_dotenv

async def main() -> None:
    """Create the aiohttp session and run the example."""
    load_dotenv()
    API_KEY = os.getenv('AMBIENT_API_KEY')
    APP_KEY = os.getenv('AMBIENT_APPLICATION_KEY')

    websocket = Websocket(APP_KEY, API_KEY)

    # Note that you can watch multiple API keys at once:
    # websocket = Websocket("YOUR APPLICATION KEY", ["<API KEY 1>", "<API KEY 2>"])

    # Define a method that should be fired when the websocket client
    # connects:
    def connect_method():
        """Print a simple "hello" message."""
        print("Client has connected to the websocket")

    websocket.on_connect(connect_method)

    # Alternatively, define a coroutine handler:
    async def connect_coroutine():
        """Waits for 3 seconds, then print a simple "hello" message."""
        await asyncio.sleep(3)
        print("Client has connected to the websocket async")

    # websocket.async_on_connect(connect_coroutine)

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

    websocket.async_on_data(data_coroutine)

    # Define a method that should be run when the websocket client
    # disconnects:
    def disconnect_method(data):
        """Print a simple "goodbye" message."""
        print("Client has disconnected from the websocket")

    websocket.on_disconnect(disconnect_method)

    # Alternatively, define a coroutine handler:
    async def disconnect_coroutine(data):
        """Wait for 3 seconds, then print a simple "goodbye" message."""
        await asyncio.sleep(3)
        print("Client has disconnected from the websocket async")

    # websocket.async_on_disconnect(disconnect_coroutine)

    # Connect to the websocket:
    await websocket.connect()

    await asyncio.sleep(300)

    # At any point, disconnect from the websocket:
    await websocket.disconnect()


asyncio.run(main())