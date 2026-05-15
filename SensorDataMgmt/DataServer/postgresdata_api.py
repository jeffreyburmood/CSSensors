""" this file contains code to store and retrieve data from the Postgres database """
from typing import List

import psycopg
from fastapi import FastAPI, HTTPException
from psycopg.rows import class_row

from data_server.connectors import PostgresDB
from data_server.data_model import DBConnectionInfo, VectorNorm
from utilities.logger import Logger

logger = Logger.get_logger()

# trading-bot structured data server routes
# these routes are used to access data withing the postgres database

# these are the graphql types, built from the pyndantic data models, used to define the graphql schema.

app = FastAPI()

@app.get("/")
def root() -> str:
    method_name = root.__name__
    logger.info(f'received GET request to the {method_name} route')

    return "Welcome to the trading-bot Structured Data Server!!!"


@app.get("/postgres/connection-info")
def query_connection_info() -> DBConnectionInfo:
    method_name = query_connection_info.__name__
    logger.info(f'received GET request to the {method_name} route')
    db = PostgresDB()
    with psycopg.connect(conninfo=db.connection_str) as conn:
        connection = db.get_connection_info(conn)
        connection_info = DBConnectionInfo(**
        {
            "status": str(connection.status.value),
            "host": connection.host,
            "hostaddr": connection.hostaddr,
            "port": str(connection.port),
            "dbname": connection.dbname,
            "user": connection.user
        })

    return connection_info

@app.post("/add-new-vector-norm")
def add_new_vector_norm_to_db(vectornorm: VectorNorm) -> dict:
    method_name = add_new_vector_norm_to_db.__name__

    try:
        logger.info(f'received POST request to the {method_name} route')

        db = PostgresDB()
        with psycopg.connect(conninfo=db.connection_str) as conn:

            logger.info("successful connection made to the database")

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.vector_norm (currencyname, vectornormvalue)
                    VALUES (%s, %s);
                    """, (vectornorm.currencyname, vectornorm.vectornormvalue))
                row_count = cur.rowcount

        return {'number_of_vector_norms_added': row_count}

    except psycopg.Error as err:
        logger.error(f"********** psycopg exception encountered in {method_name} looks like {err}")
        return {'number_of_vector_norms_added': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST a new vector norm to the db, looks like {ex}")

@app.get("/vector-norm")
def get_vector_norm(currency: str) -> List[VectorNorm]:
    method_name = get_vector_norm.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        db = PostgresDB()
        with psycopg.connect(conninfo=db.connection_str) as conn:

            logger.info("successful connection made to the database")

            if currency == 'all':

                with conn.cursor(row_factory=class_row(VectorNorm)) as cur:
                    cur.execute(
                        "SELECT * FROM public.vector_norm"
                    )

                    vector_norm_list = cur.fetchall()

            else:
                with conn.cursor(row_factory=class_row(VectorNorm)) as cur:
                    cur.execute("""
                        SELECT * FROM public.vector_norm
                        WHERE currencyname = %s;
                        """, (currency,))

                    vector_norm_list = cur.fetchall()

        return vector_norm_list

    except psycopg.Error as err:
        logger.error(f"********** psycopg exception encountered in {method_name} looks like {err}")
        return list()

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to GET a vector norm to the db, looks like {ex}")


@app.delete("/delete-vector-norm")
def delete_vector_norm_from_db(currency_name: str) -> dict:
    method_name = delete_vector_norm_from_db.__name__

    try:
        logger.info(f'received DELETE request to the {method_name} route')

        db = PostgresDB()
        with psycopg.connect(conninfo=db.connection_str) as conn:

            logger.info("successful connection made to the database")

            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM public.vector_norm
                    WHERE currencyname = %s;
                    """, (currency_name,))
                row_count = cur.rowcount

            logger.info(f'row count for delete = {row_count}')

        return {"number_of_vector_norms_deleted": row_count}

    except psycopg.Error as err:
        logger.error(f"********** psycopg exception encountered in {method_name} looks like {err}")
        return {'number_of_vector_norms_deleted': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to DELETE a vector norm to the db, looks like {ex}")



