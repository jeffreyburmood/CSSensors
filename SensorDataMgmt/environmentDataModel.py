""" this file contains the pydantic definition of the sensor-based data models used in the CABIN system """

from pydantic import BaseModel, Field
from typing import Any, Annotated, Optional
from datetime import datetime

class WeatherData(BaseModel):
    location: str
    sensor: str
    weatherdate: str
    weatheryear: str
    weathermonth: str
    weatherday: str
    weatherhour: str
    tempf: float
    humidity: float
    windspeed: float
    solarad: float
    rainfallhrly: float

