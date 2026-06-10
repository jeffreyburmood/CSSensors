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

    async def get_driver(self, driver):

        method_name = self.get_driver.__name__

        try:
            self.logger.debug(f'received request to {method_name}')
            await driver.verify_connectivity()
            self.logger.debug(f'graph db connection verified!')
            return driver

        except Exception as ex:
            self.logger.error(f"exception encounter in {method_name} trying to get the neo4j driver, looks like {ex}")

    def close_connection(self, driver):
        """
        Closes the Neo4j driver connection.
        """
        method_name = self.close_connection.__name__

        try:
            self.logger.info(f'received request to {method_name}')

            if driver is not None:
                driver.close()
                self.logger.info(f'graph db connection closed!')

        except Exception as ex:
            self.logger.error(f'Exception encounter in {method_name}, looks like {ex}')

class Neo4jEnv(Neo4j):
    driver = None

    def __init__(self):
        load_dotenv()
        super().__init__(os.getenv('NEO4J_USER_NAME'), os.getenv('NEO4J_PASSWORD'), os.getenv('NEO4J_ENV_CONNECTION_STR'))
        if self.driver is None:
            try:
                self.driver = AsyncGraphDatabase.driver(self.connection_str, auth=self.auth)
                self.logger.info(f'new environment graph database connection driver created')

            except Exception as ex:
                self.logger.error(f"exception encounter in Neo4j constructor trying to create the neo4j connection, looks like {ex}")

    def get_db_driver(self):

        method_name = self.get_db_driver.__name__

        try:
            return self.driver

        except Exception as ex:
            self.logger.error(f"exception encounter in {method_name} trying to get the neo4j env driver, looks like {ex}")
            raise

    def close_db_connection(self):
        """
        Closes the Neo4j driver connection.
        """
        super().close_connection(self.driver)
