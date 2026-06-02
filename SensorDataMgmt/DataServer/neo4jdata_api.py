""" this file contains the code used to store and retrieve data from the Neo4j database """
import csv
import uuid
from datetime import datetime
from typing import List

import neo4j.exceptions
from fastapi import FastAPI, HTTPException

from SensorDataMgmt.DataServer.neo4jConnector import Neo4jEnv
from SensorDataMgmt.neo4jDataModel import DBCounters
from SensorDataMgmt.environmentDataModel import WeatherData
from utilities.logger import Logger

app = FastAPI()

logger = Logger.get_logger()


# trading-bot data server routes
# these routes are used to access data withing the neo4j database

@app.get("/")
def root() -> str:
    method_name = root.__name__
    logger.info(f'received GET request to the {method_name} route')

    return "Welcome to the CSS Data Server!!!"


# @app.get("/test-connection")
def test_db_connection() -> str:
    method_name = test_db_connection.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        driver = Neo4jEnv().get_db_driver()
        if driver is not None:
            logger.info(f'Database connection has been created successfully!')
            return "Database connection has been created successfully!"
        else:
            logger.info(f'Database connection not created, no driver value returned from GET request')
            raise HTTPException(status_code=404, detail=f"Error occurred, no driver returned!!!")
    except:
        raise HTTPException(status_code=404, detail=f"Error occurred when trying to create the database connection")


# @app.get("/close-connection")
def close_db_connection() -> str:
    method_name = close_db_connection.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        Neo4jEnv().close_db_connection()
        logger.info(f'Database connection has been closed successfully!')
        return "Database connection has been closed successfully!"
    except Exception as ex:
        raise HTTPException(status_code=404, detail=f"Exception occurred when trying to close the database connection, looks like {ex}")

# @app.get("/clear-db")
def clear_db() -> DBCounters:
    method_name = clear_db.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        driver = Neo4jEnv().get_db_driver()

        db_counters = DBCounters()

        clean_summary = driver.execute_query(
            "MATCH (c) DETACH DELETE (c)",
            database_="neo4j").summary
        logger.info(f'Database has been cleared successfully!')

        db_counters.update_counts(clean_summary.counters)

        return db_counters

    except Exception as ex:
        raise HTTPException(status_code=404, detail=f"Exception occurred when trying to clear the database, looks like {ex}")

async def get_location_name(location: str) -> str:

    method_name = get_location_name.__name__

    try:
        location_name = ''

        driver = Neo4jEnv().get_db_driver()

        query_records = driver.execute_query("""
                MERGE (l:Location {name: $name})
                RETURN l
             """,
            name=location,
            database_='neo4j',
            ).records

        for record in query_records:
            location_data = record.data()['t']
            logger.info(f'a transaction record looks like {location_data}')

            if location_data is not None:
               location_name = location_data['name']

        return location_name

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")


# @app.post("/add-new-weather-data")
async def add_new_weather_data(new_weather_data: WeatherData) -> dict[str:int]:
    """ this method will add new weather data to the neo4j database """

    method_name = add_new_weather_data.__name__

    try:
        logger.info(f'received POST request to the {method_name} route')

        # need to deconstruct the date / time to get the ids for year, month, day, hour
        year_name = datetime.strftime(new_weather_data.weatherdate, '%Y')
        month_name = datetime.strftime(new_weather_data.weatherdate, '%Y-%m')
        day_name = datetime.strftime(new_weather_data.weatherdate, '%Y-%m-%d')
        hour_name = datetime.strftime(new_weather_data.weatherdate, '%Y-%m-%d:%H')

        reading_id = new_weather_data.weatherdate.strftime('%Y-%m-%d:%H:%M:%S')

        driver = await Neo4jEnv().get_db_driver()

        db_counters = DBCounters()

        # create the new weather data node  for the hour
        records, summary, keys = await driver.execute_query("""
                            MERGE (l: Location {locationname: $locationname})
                            MERGE (s: Sensor {sensorname: $sensorname})
                            MERGE (s)-[:INSTALLED_AT]->(l)
                            MERGE (y: Year {yearname: $yearname})
                            MERGE (l)-[:HAS_YEAR]->(y)
                            MERGE (m: Month {monthname: $monthname})
                            MERGE (y)-[:HAS_MONTH]->(m)
                            MERGE (d: Day {dayname: $dayname})
                            MERGE (m)-[:HAS_DAY]->(d)
                            MERGE (h: Hour {hourname: $hourname})
                            MERGE (d)-[:HAS_HOUR]->(h)
                            MERGE (r: Reading {readingid: $readingid, tempf: $tempf, humidity: $humidity, windspeed: $windspeed, solarad: $solarad, rainfallhrly: $rainfallhrly})
                            MERGE (r)-[:RECORDED_BY]-(s)
                            MERGE (r)-[:OCCURRED_AT]->(h)
                        """,
                        locationname=new_weather_data.location,
                        sensorname=new_weather_data.sensor,
                        yearname=year_name,
                        monthname=month_name,
                        dayname=day_name,
                        hourname=hour_name,
                                                            readingid=reading_id,
                                                            tempf=new_weather_data.tempf,
                                                            humidity=new_weather_data.humidity,
                                                            windspeed=new_weather_data.windspeed,
                                                            solarad=new_weather_data.solarad,
                                                            rainfallhrly=new_weather_data.rainfallhrly,
                        database_='neo4j',
                        )

        row_summary = summary
        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.nodes_created} nodes created")

        return {'weather_data_added': db_counters.nodes_created}

    except neo4j.exceptions.ConstraintError as con:
        logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")
        return {'weather_data_added': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST new weather data to the db, looks like {ex}")



