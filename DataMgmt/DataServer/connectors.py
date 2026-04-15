""" This file contains the code to build database connectors and associated drivers for the Neo4j and Postgres DBs """

from neo4j import AsyncGraphDatabase

import os

from dotenv import load_dotenv
from utilities.logger import Logger

class Neo4j:
    """ Base class for the Neo4j database connectors """
    def __init__(self, userName, password, connectionStr):
        self.username = userName
        self.password = password
        self.auth = (self.username, self.password)
        # connection string examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
        self.connection_str = connectionStr  # this is the docker network IP address for this container
        self.logger = Logger.get_logger()

class Neo4jEnv(Neo4j):
    driver = None

    def __init__(self):
        load_dotenv()
        super().__init__(os.getenv('ENV_USER_NAME'), os.getenv('ENV_PASSWORD'), os.getenv('ENV_CONNECTION_STR'))
        if Neo4jEnv.driver is None:
            try:
                Neo4j.driver = AsyncGraphDatabase.driver(self.connection_str, auth=self.auth)
                self.logger.info(f'new environment graph database connection driver created')

            except Exception as ex:
                self.logger.error(f"exception encounter in Neo4j constructor trying to create the neo4j connection, looks like {ex}")
