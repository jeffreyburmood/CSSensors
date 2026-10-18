""" this file contains code for managing admin data in the Neo4j DB """

import os
from dotenv import load_dotenv

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession

import neo4j.exceptions
from fastapi import FastAPI, HTTPException

from SensorDataMgmt.DataServer.neo4jConnector import Neo4jEnv
from SensorDataMgmt.environmentDataModel import WeatherData, InteriorData, BasementData
from SensorDataMgmt.neo4jDataModel import DBCounters
from utilities.healthStatus import HealthContext
from utilities.logger import logger

# set up for the FastAPI server
app = FastAPI()

# initialize the logger used for all methods
logger = logger

# initialize the DB credentials used for all db endpoints
load_dotenv()

URI      = os.getenv("NEO4J_ADMIN_URI")
AUTH     = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
DATABASE = os.getenv("NEO4J_ADMIN_DATABASE", "neo4j")

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

# ---------------------------------------------------------------------------
# Session helper: returns a configured AsyncSession
# ---------------------------------------------------------------------------
def get_session(driver: AsyncDriver) -> AsyncSession:
    """
    driver.session() is a lightweight factory call.
    Specify the target database explicitly so the session is pinned to it
    rather than relying on the server default.
    The caller is responsible for closing the session (use `async with`).
    """
    return driver.session(database=DATABASE)

# ---------------------------------------------------------------------------
# WRITE operations — each session.run() is an auto-commit transaction
# ---------------------------------------------------------------------------
async def create_health_check_entry(session: AsyncSession, title: str, released: int, tagline: str) -> None:
    """
    MERGE avoids duplicate Movie nodes on repeated runs.
    session.run() fires an auto-commit transaction immediately;
    the result must be consumed before the session is reused.
    """
    result = await session.run(
        """
        MERGE (m:Movie {title: $title})
        ON CREATE SET m.released = $released, m.tagline = $tagline
        RETURN m.title AS title
        """,
        title=title,
        released=released,
        tagline=tagline,
    )
    record = await result.single()
    print(f"[CREATE] Movie: '{record['title']}'")

@app.post("/add-new-health-check-data")
async def add_new_health_check_data(health_check_data: HealthContext) -> None:
        """ this method will add new weather data to the neo4j database """

        method_name = add_new_health_check_data.__name__

        try:
            logger.info(f'received POST request to the {method_name} route')

            async with AsyncGraphDatabase.driver(URI, auth=AUTH) as driver:

                # Verify the driver can reach the server before doing any work.
                await driver.verify_connectivity()
                print(f"[DRIVER] Connected to Neo4j at {URI}\n")

                # ── SEED ────────────────────────────────────────────────────────────
                print("=== Seeding Movies ===")
                async with get_session(driver) as session:
                # Each session.run() call is its own auto-commit transaction.
                # Grouping them inside a single session context means they
                # share the same underlying network connection for efficiency,
                # but each query commits independently.
                   await create_health_check_entry(session, **health_check_data)

        except neo4j.exceptions.ConstraintError as con:
            logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")

        except Exception as ex:
            logger.error(f"Exception encountered in {method_name} looks like {ex}")
            raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST new weather data to the db, looks like {ex}")

