""" this file contains code to practice with parsing datetime objects """
import asyncio
import time
from datetime import datetime, date

from SensorDataMgmt.DataServer.neo4jdata_api import add_new_weather_data
from SensorDataMgmt.environmentDataModel import WeatherData

async def main() -> None:
    date1 = '2026-05-16 13:01:12'
    my_datetime = datetime.strptime(date1, '%Y-%m-%d %H:%M:%S')  # change from datetime.now()
    print(f'the my_datetime value is {my_datetime} is type {type(my_datetime)}')
    my_date = my_datetime.date()
    print(f'the my_date value is {my_date} is type {type(my_date)}')
    my_time = my_datetime.time()
    print(f'the my_time value is {my_time} is type {type(my_time)}')
    # my_date_string = datetime.strptime('2026-05-16 13:01:12', '%Y-%m-%d %H:%M:%S')
    # print(f'the my_date_string value is {my_date_string} is type {type(my_date_string)}')
    year = my_date.year
    month = my_date.month
    day = my_date.day
    hour = my_time.hour
    print(f'the year = {year} is type {type(year)}')
    print(f'the month = {month} is type {type(month)}')
    print(f'the day = {day} is type {type(day)}')
    print(f'the hour = {hour} is type {type(hour)}')

    # create the weather data object
    data = {
        'location': 'cabin-outside',
        'sensor': 'ambientweather',
        'weatherdate': datetime.strptime(date1, '%Y-%m-%d %H:%M:%S'),
        'tempf': '51.8',
        'humidity': '25',
        'windspeed': '3.1',
        'solarad': '0.0',
        'rainfallhrly': '0.0',
    }
    weather_data = WeatherData(**data)

    result = await add_new_weather_data(weather_data)
    print(f'add weather data result = {result}')

    time.sleep(10)

    date2 = '2026-05-16 14:01:12'

    # create the weather data object
    data = {
        'location': 'cabin-outside',
        'sensor': 'ambientweather',
        'weatherdate': datetime.strptime(date2, '%Y-%m-%d %H:%M:%S'),
        'tempf': '52.0',
        'humidity': '27',
        'windspeed': '3.7',
        'solarad': '10.0',
        'rainfallhrly': '0.5',
    }
    weather_data = WeatherData(**data)

    result = await add_new_weather_data(weather_data)
    print(f'add weather data result = {result}')

    print('All Done!')

if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("Received KeyboardInterrupt, shutting down.")

