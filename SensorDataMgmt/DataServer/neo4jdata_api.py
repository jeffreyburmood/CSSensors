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
    logger.debug(f'received GET request to the {method_name} route')

    return "Welcome to the CSS Data Server!!!"


@app.get("/test-connection")
def test_db_connection() -> str:
    method_name = test_db_connection.__name__

    try:
        logger.debug(f'received GET request to the {method_name} route')

        driver = Neo4jEnv().get_db_driver()
        if driver is not None:
            logger.debug(f'Database connection has been created successfully!')
            return "Database connection has been created successfully!"
        else:
            logger.debug(f'Database connection not created, no driver value returned from GET request')
            raise HTTPException(status_code=404, detail=f"Error occurred, no driver returned!!!")
    except:
        raise HTTPException(status_code=404, detail=f"Error occurred when trying to create the database connection")


@app.get("/close-connection")
def close_db_connection() -> str:
    method_name = close_db_connection.__name__

    try:
        logger.debug(f'received GET request to the {method_name} route')

        Neo4jEnv().close_db_connection()
        logger.debug(f'Database connection has been closed successfully!')
        return "Database connection has been closed successfully!"
    except Exception as ex:
        raise HTTPException(status_code=404, detail=f"Exception occurred when trying to close the database connection, looks like {ex}")

@app.get("/clear-db")
def clear_db() -> DBCounters:
    method_name = clear_db.__name__

    try:
        logger.debug(f'received GET request to the {method_name} route')

        driver = Neo4jEnv().get_db_driver()

        db_counters = DBCounters()

        clean_summary = driver.execute_query(
            "MATCH (c) DETACH DELETE (c)",
            database_="neo4j").summary
        logger.debug(f'Database has been cleared successfully!')

        db_counters.update_counts(clean_summary.counters)

        return db_counters

    except Exception as ex:
        raise HTTPException(status_code=404, detail=f"Exception occurred when trying to clear the database, looks like {ex}")


@app.post("/add-new-weather-data")
async def add_new_weather_data(new_weather_data: WeatherData) -> None:
    """ this method will add new weather data to the neo4j database """

    method_name = add_new_weather_data.__name__

    try:
        logger.debug(f'received POST request to the {method_name} route')

        driver = Neo4jEnv().get_db_driver()

        db_counters = DBCounters()

        # extract the individual datetime values for use as noe properties
        year, month, day = new_weather_data.weatherdate[:10].split("-")
        hour, minute, second = new_weather_data.weatherdate[11:].split(":")

        # create the new weather data node  for the hour
        records, summary, keys = await driver.execute_query("""
                            MERGE (l: Location {locationname: $locationname})
                            MERGE (s: Sensor {sensorname: $sensorname})
                            MERGE (s)-[:INSTALLED_AT]->(l)
                            MERGE (y: Year {yearname: $yearname, year: $year})
                            MERGE (l)-[:HAS_YEAR]->(y)
                            MERGE (m: Month {monthname: $monthname, year: $year, month: $month})
                            MERGE (y)-[:HAS_MONTH]->(m)
                            MERGE (d: Day {dayname: $dayname, year: $year, month: $month, day: $day})
                            MERGE (m)-[:HAS_DAY]->(d)
                            MERGE (h: Hour {hourname: $hourname, year: $year, month: $month, day: $day, hour: $hour})
                            MERGE (d)-[:HAS_HOUR]->(h)
                            CREATE (r: Reading {readingid: $readingid, tempf: $tempf, humidity: $humidity, windspeed: $windspeed, solarad: $solarad, rainfallhrly: $rainfallhrly})
                            CREATE (r)-[:RECORDED_BY]->(s)
                            CREATE (r)-[:OCCURRED_AT]->(h)
                        """,
                        locationname=new_weather_data.location,
                        sensorname=new_weather_data.sensor,
                        yearname=new_weather_data.weatheryear,
                        monthname=new_weather_data.weathermonth,
                        dayname=new_weather_data.weatherday,
                        hourname=new_weather_data.weatherhour,
                                                            year=year,
                                                            month=month,
                                                            day=day,
                                                            hour=hour,
                                                            readingid=new_weather_data.weatherdate,
                                                            tempf=new_weather_data.tempf,
                                                            humidity=new_weather_data.humidity,
                                                            windspeed=new_weather_data.windspeed,
                                                            solarad=new_weather_data.solarad,
                                                            rainfallhrly=new_weather_data.rainfallhrly,
                        database_='neo4j',
                        )

        row_summary = summary
        db_counters.update_counts(row_summary.counters)

        logger.debug(f"There were {db_counters.nodes_created} nodes created and {db_counters.relationships_created} relationships created")

    except neo4j.exceptions.ConstraintError as con:
        logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST new weather data to the db, looks like {ex}")



