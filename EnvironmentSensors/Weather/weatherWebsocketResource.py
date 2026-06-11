""" This file contains the class and associated methods for managing the weather data websocket in a Context Manager """
import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Coroutine
from zoneinfo import ZoneInfo

from httpx import AsyncClient, HTTPStatusError

from SensorDataMgmt.environmentDataModel import WeatherData

from aioambient import Websocket
from dotenv import load_dotenv
from utilities.logger import Logger

import statistics
from collections import defaultdict

# persistent state for accumulating temperature data
_weather_accumulator = defaultdict(list)
_last_processed_hour = None

logger = Logger.get_logger()
load_dotenv()

def convert_utc_to_timezone(utc_date: str, tz: str) -> str:
    """
    Converts a python datetime string in UTC to the target IANA time zone.
    :param utc_date: str, e.g. "2025-11-12 14:06:00+00:00"
    :param tz: IANA time zone string, e.g. "America/New_York"
    :return: String of converted datetime in same format
    """

    method_name = convert_utc_to_timezone.__name__

    try:
        # Parse the datetime string as UTC and convert to a datetime object
        dt = datetime.strptime(utc_date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        # convert datetime object to target timezone
        dt_target = dt.astimezone(ZoneInfo(tz))

        return dt_target.strftime("%Y-%m-%d %H:%M:%S")

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name} while setting up the Consumer, looks like {ex}')
        raise

async def add_weather_data_to_database(new_weather_data: WeatherData) -> None:
    """ the coroutine makes the actual fastapi call to add the new weather data to the db"""
    method_name = add_weather_data_to_database.__name__

    try:
        db_url = os.getenv('NEO4J_DATA_API_URL')

        async with AsyncClient() as client:
            response = await client.post(db_url+'/add-new-weather-data', json=new_weather_data.model_dump())
            response.raise_for_status()
            logger.info(f'request to add new weather data completed successfully!')

    except HTTPStatusError as http_error:
        logger.error(f'Http error status returned for {method_name}, looks like {http_error}')
    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise

async def process_weather_data(current_data):
    """
    Takes the current real time weather data and extracts the relevant data and stores it in the database.
    Accumulates temperature data and reports the median temperature once per hour.
    :param current_data: Dictionary of retrieved weather station data
    """

    method_name = process_weather_data.__name__

    global _weather_accumulator, _last_processed_hour

    try:
        mac_addr = os.getenv('CABIN_MAC')

        if current_data['macAddress'] == mac_addr:
            local_datetime = convert_utc_to_timezone(current_data['date'], current_data['tz'])
            logger.debug(f'local date time = {local_datetime}')

            parsed_datetime = datetime.strptime(local_datetime, '%Y-%m-%d %H:%M:%S')
            hour_key = (parsed_datetime.date(), parsed_datetime.hour)

            # accumulate temperature readings for the current hour
            _weather_accumulator[hour_key].append({
                'tempf': current_data['tempf'],
                'humidity': current_data['humidity'],
                'windspeed': current_data['windspeedmph'],
                'solarad': current_data['solarradiation'],
                'rainfallhrly': current_data['hourlyrainin'],
                'weatherdate': parsed_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                'weatheryear': parsed_datetime.strftime("%Y"),
                'weathermonth': parsed_datetime.strftime("%Y-%m"),
                'weatherday': parsed_datetime.strftime("%Y-%m-%d"),
                'weatherhour': parsed_datetime.strftime("%Y-%m-%d:%H")
            })

            # when the hour changes, process the completed hour's accumulated data
            if _last_processed_hour is not None and _last_processed_hour != hour_key:
                completed_hour_readings = _weather_accumulator.pop(_last_processed_hour, [])

                if completed_hour_readings:
                    median_tempf = statistics.median(r['tempf'] for r in completed_hour_readings)

                    # use the last reading of the hour for non-accumulated fields
                    last_reading = completed_hour_readings[-1]

                    data = {
                        'location': 'cabin-outside',
                        'sensor': 'ambientweather',
                        'weatherdate': last_reading['weatherdate'],
                        'weatheryear': last_reading['weatheryear'],
                        'weathermonth': last_reading['weathermonth'],
                        'weatherday': last_reading['weatherday'],
                        'weatherhour': last_reading['weatherhour'],
                        'tempf': median_tempf,
                        'humidity': last_reading['humidity'],
                        'windspeed': last_reading['windspeed'],
                        'solarad': last_reading['solarad'],
                        'rainfallhrly': last_reading['rainfallhrly'],
                    }
                    weather_data = WeatherData(**data)
                    logger.debug(f'Weather Data object (median temp for hour {_last_processed_hour[1]:02d}:00= {median_tempf})')

                    await add_weather_data_to_database(weather_data)

            _last_processed_hour = hour_key

        else:
            pass

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise

# Define a method that should be fired when the websocket client
# connects:
def connect_method():
    """Print a simple "connected" message."""

    method_name = connect_method.__name__

    try:

        logger.debug(f"Client has connected to the websocket in {method_name}")

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise

# Define a method that should be run upon subscribing to the Ambient
# Weather cloud:
def subscribed_method(data):
    """Print the data received upon subscribing."""

    method_name = subscribed_method.__name__

    try:
        logger.debug(f"Subscription data received in {method_name}: {data}")

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise

# Alternatively, define a coroutine handler:
async def data_coroutine(data):
    """ process the data received by adding it to the database """

    method_name = data_coroutine.__name__

    try:
        logger.debug(f"Data received async: {data}")
        await process_weather_data(data)

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise

# Define a method that should be run when the websocket client
# disconnects:
async def disconnect_coroutine(data):
    """Wait for 3 seconds, then print a simple "goodbye" message."""

    method_name = disconnect_coroutine.__name__

    try:
        await asyncio.sleep(3)
        logger.debug("Client has disconnected from the websocket async")

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise


def configure_websocket() -> Websocket:
    """ this method will perform the weather websocket set needed before attempting to connect """

    method_name = configure_websocket.__name__

    try:
        load_dotenv()
        API_KEY = os.getenv('AMBIENT_API_KEY')
        APP_KEY = os.getenv('AMBIENT_APPLICATION_KEY')

        websocket = Websocket(APP_KEY, API_KEY)
        websocket.on_connect(connect_method)
        websocket.on_subscribed(subscribed_method)
        websocket.async_on_data(data_coroutine)
        websocket.async_on_disconnect(disconnect_coroutine)

        return websocket

    except Exception as ex:
        logger.error(f'Exception encountered in {method_name}, looks like {ex}')
        raise

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
            logger.debug(f"[{self.name}] __aenter__ -> acquired: {self.resource}")
            return self.resource

        except Exception as e:
            logger.error(f"[{self.name}] __aenter__ failed: {e}")
            # Re-raise so callers know acquisition failed and the with-block never runs
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):

        try:
            # Only try to release if resource was acquired
            if self.resource is None:
                logger.debug(f"[{self.name}] __aexit__: nothing to release.")
                return False  # don't suppress exceptions from the with-block

            await self._release()
            logger.debug(f"[{self.name}] __aexit__ -> released successfully.")
        except Exception as e:
            logging.error(f"[{self.name}] __aexit__ failed to release: {e}")
            # Decide whether to suppress the release exception
            # Return False to propagate any exception from the with-block (default behavior)
            return False

    async def _acquire(self) -> Websocket | None:

        try:
            # simulate async acquisition work
            logger.debug(f"[{self.name}] acquiring (async)...")
            websocket = configure_websocket()
            await websocket.connect()
            return websocket

        except Exception as e:
            logging.error(f"[{self.name}] __acquire__ failed to acquire: {e}")
            # Decide whether to suppress the release exception
            # Return None to propagate any exception from the with-block (default behavior)
            return None

    async def _release(self):

        try:
            # simulate async release work
            logger.debug(f"[{self.name}] releasing (async)...")
            await self.resource.disconnect()
            return None

        except Exception as e:
            logging.error(f"[{self.name}] __release_ failed to release: {e}")
            # Decide whether to suppress the release exception
            # Return None to propagate any exception from the with-block (default behavior)
            return None
