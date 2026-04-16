""" this file contains the pydantic definition of the sensor-based data models used in the CABIN system """

from pydantic import BaseModel, Field
from typing import Any, Annotated, Optional
from datetime import date

class WeatherData(BaseModel):
    weatherdate: date
    tempf: float
    humidity: float
    windspeed: float
    solarad: float
    rainfallhrly: float

