""" this file contains the API calls to manage the environment subsystem data in the database(s) """

import csv
from datetime import date
from typing import List

import neo4j.exceptions
from fastapi import FastAPI, HTTPException

from SensorDataMgmt.DataServer.neo4jConnector import Neo4j
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

    return "Welcome to the trading-bot Data Server!!!"


@app.get("/test-connection")
def test_db_connection() -> str:
    method_name = test_db_connection.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        driver = Neo4j().get_driver()
        if driver is not None:
            logger.info(f'Database connection has been created successfully!')
            return "Database connection has been created successfully!"
        else:
            logger.info(f'Database connection not created, no driver value returned from GET request')
            raise HTTPException(status_code=404, detail=f"Error occurred, no driver returned!!!")
    except:
        raise HTTPException(status_code=404, detail=f"Error occurred when trying to create the database connection")


@app.get("/close-connection")
def close_db_connection() -> str:
    method_name = close_db_connection.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        db_connection = Neo4j()
        db_connection.close_connection()
        logger.info(f'Database connection has been closed successfully!')
        return "Database connection has been closed successfully!"
    except Exception as ex:
        raise HTTPException(status_code=404, detail=f"Exception occurred when trying to close the database connection, looks like {ex}")

@app.get("/clear-db")
def clear_db() -> DBCounters:
    method_name = clear_db.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        clean_summary = driver.execute_query(
            "MATCH (c) DETACH DELETE (c)",
            database_="neo4j").summary
        logger.info(f'Database has been cleared successfully!')

        db_counters.update_counts(clean_summary.counters)

        return db_counters

    except Exception as ex:
        raise HTTPException(status_code=404, detail=f"Exception occurred when trying to clear the database, looks like {ex}")

@app.get("/currencies")
def query_existing_currencies() -> List[Currency]:
    """ the purpose of this method endpoint is to retrieve the list of existing currencies from the DB """
    method_name = query_existing_currencies.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')
        currencies = []

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                MATCH (c:Currency)
                RETURN c
             """,
            database_='neo4j',
            ).records

        for record in query_records:
            currency_data = record.data()['c']
            logger.info(f'a currency record looks like {currency_data}')
            # send the dictionary so the new Currency object is filled and verified by pydantic
            currency = Currency(**currency_data)
            # continue building the Transaction list
            currencies.append(currency)

        return currencies

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"Existing currencies not found, exception looks like {ex}")

@app.get("/accounts")
def query_existing_accounts() -> List[Account]:
    """ the purpose of this method endpoint is to retrieve the list of existing currencies from the DB """
    method_name = query_existing_accounts.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')
        accounts = []

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                MATCH (a:Account)
                RETURN a
             """,
            database_='neo4j',
            ).records

        for record in query_records:
            account_data = record.data()['a']
            logger.info(f'an account record looks like {account_data}')
            # send the dictionary so the new Account object is filled and verified by pydantic
            account = Account(**account_data)
            # continue building the Account list
            accounts.append(account)

        return accounts

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"Existing accounts not found, exception looks like {ex}")

@app.put("/test-transaction")
def test_transaction(transaction: Transaction) -> dict:
    """ this method will check to see if the provided transaction already exists in the DB """
    method_name = test_transaction.__name__

    try:
        logger.info(f'received PUT request to the {method_name} route looks like {transaction}')
        return {"result":"true"}

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")

@app.get("/check-transaction")
def check_for_existing_transaction(transaction: Transaction) -> dict:
    """ this method will check to see if the provided transaction already exists in the DB, no DB updates are performed """
    logger = Logger.get_logger()
    method_name = check_for_existing_transaction.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')
        result = 'false'

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                MATCH (t:Transaction {ordertype:$ordertype, spent:$spent, received:$received, fee:$fee, unitprice:$unitprice, transdate:$transdate})
                RETURN t
             """,
            ordertype=transaction.ordertype,
            spent=transaction.spent,
            received=transaction.received,
            fee=transaction.fee,
            unitprice=transaction.unitprice,
            transdate=transaction.transdate,
            database_='neo4j',
            ).records

        if (len(query_records) > 0):
            result = 'true'

        return {'result':result}

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")

@app.get("/transaction-id")
def get_transaction_id(transaction: Transaction) -> dict:
    """ this method will return the id of the specified transaction, if it exists in the database """
    logger = Logger.get_logger()
    method_name = get_transaction_id.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                MATCH (t:Transaction {ordertype:$ordertype, spent:$spent, received:$received, fee:$fee, unitprice:$unitprice, transdate:$transdate})
                RETURN t
             """,
            ordertype=transaction.ordertype,
            spent=transaction.spent,
            received=transaction.received,
            fee=transaction.fee,
            unitprice=transaction.unitprice,
            transdate=transaction.transdate,
            database_='neo4j',
            ).records

        for record in query_records:
            transaction_data = record.data()['t']
            logger.info(f'a transaction record looks like {transaction_data}')

            if transaction_data is None:
                transaction_id = ""
            else:
                transaction_id = transaction_data['transactionid']

        return {'transactionid':transaction_id}

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")

@app.get("/transactions/{currency_name}")
def query_transactions_by_currency(currency_name: str, account_name: str, order_type: str = "Buy") -> List[Transaction]:
    method_name = query_transactions_by_currency.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')
        transactions = []

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                MATCH (:Account {accountname:$accountname})-[:PLACED_ORDER]->(t:Transaction {ordertype:$ordertype})-[:ORDER]->(c:Currency {currencyname: $currencyname}) 
                RETURN t
             """,
            accountname=account_name,
            ordertype=order_type,
            currencyname=currency_name,
            database_='neo4j',
            ).records

        for record in query_records:
            transaction_data = record.data()['t']
            logger.info(f'a transaction record looks like {transaction_data}')
            # before forming the Transaction object (and type checking by pydantic) we need to convert
            # the 'date' field retrieved from the DB to a python 'date' field since that's what the data model uses
            transaction_data['transdate'] = transaction_data['transdate'].to_native()
            # send the dictionary so the new Transaction object is filled and verified by pydantic
            transaction = Transaction(**transaction_data)
            # continue building the Transaction list
            transactions.append(transaction)

        return transactions

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"Transaction with order type {order_type} and currency name {currency_name} not found, exception looks like {ex}")


@app.post("/add-new-currency")
def add_new_currency_to_db(currency: Currency) -> dict:
    method_name = add_new_currency_to_db.__name__

    try:
        logger.info(f'received POST request to the {method_name} route')

        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        row_summary = driver.execute_query("""
                        CREATE (c: Currency {currencyname: $currencyname, currencyid: $currencyid})
                    """,
                        currencyid=currency.currencyid,
                        currencyname=currency.currencyname,
                        database_='neo4j',
                        ).summary

        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.nodes_created} nodes created")

        return {'number_currencies_added': db_counters.nodes_created}

    except neo4j.exceptions.ConstraintError as con:
        logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")
        return {'number_currencies_added': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST a new currency to the db, looks like {ex}")

@app.post("/add-new-transaction")
def add_new_transaction_to_db(transaction: Transaction) -> dict:
    method_name = add_new_transaction_to_db.__name__

    try:
        logger.info(f'received POST request to the {method_name} route')

        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        row_summary = driver.execute_query("""
                        CREATE (t: Transaction {transactionid: $transactionid, ordertype: $ordertype, spent:$spent, received: $received, fee: $fee, unitprice: $unitprice, transdate: $transdate})
                    """,
                    transactionid=transaction.transactionid,
                    ordertype=transaction.ordertype,
                    spent=transaction.spent,
                    received=transaction.received,
                    fee=transaction.fee,
                    unitprice=transaction.unitprice,
                    transdate=transaction.transdate,
                    database_='neo4j',
                    ).summary

        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.nodes_created} nodes created")

        return {'number_transactions_added': db_counters.nodes_created}

    except neo4j.exceptions.ConstraintError as con:
        logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")
        return {'number_transactions_added': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST a new transaction to the db, looks like {ex}")


@app.post("/add-new-order")
def add_new_order_to_db(order: Order) -> dict:
    method_name = add_new_order_to_db.__name__

    try:
        logger.info(f'received POST request to the {method_name} route')

        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        row_summary = driver.execute_query("""
                        MATCH (t:Transaction {transactionid: $transactionid})
                        MATCH (c:Currency {currencyid: $currencyid})
                        CREATE (t)-[:ORDER {orderid: $orderid, ordertype:$ordertype}]->(c)
                     """,
                    orderid=order.orderid,
                    transactionid=order.transactionid,
                    currencyid=order.currencyid,
                    ordertype=order.ordertype,
                    database_='neo4j',
                    ).summary

        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.relationships_created} relationships created")

        return {'number_orders_added': db_counters.relationships_created}

    except neo4j.exceptions.ConstraintError as con:
        logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")
        return {'number_orders_added': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST a new order to the db, looks like {ex}")

@app.get("/check-order")
def check_for_existing_order(order: Order) -> dict:
    """ this method will check to see if the provided order already exists in the DB, no DB updates are performed """
    logger = Logger.get_logger()
    method_name = check_for_existing_order.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')
        result = 'false'

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                MATCH (t:Transaction {transactionid: $transactionid})
                MATCH (c:Currency {currencyid: $currencyid})
                MATCH (t)-[o:ORDER {orderid: $orderid}]->(c)
                RETURN o
             """,
            orderid=order.orderid,
            transactionid=order.transactionid,
            currencyid=order.currencyid,
            database_='neo4j',
            ).records

        if (len(query_records) > 0):
            result = 'true'

        return {'result':result}

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")

@app.get("/order-id")
def get_order_id(order: Order) -> dict:
    """ this method will check to see if the provided order already exists in the DB, no DB updates are performed """
    logger = Logger.get_logger()
    method_name = get_order_id.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        driver = Neo4j().get_driver()

        order_id = ""

        query_records = driver.execute_query("""
                MATCH (t:Transaction {transactionid: $transactionid})
                MATCH (c:Currency {currencyid: $currencyid})
                MATCH (t)-[o:ORDER {ordertype: $ordertype}]->(c)
                RETURN o
             """,
            transactionid=order.transactionid,
            currencyid=order.currencyid,
            ordertype=order.ordertype,
            database_='neo4j',
            ).records

        logger.info(f'the query results look like: {query_records}')
        for record in query_records:
            order_record = record['o']
            order_id = order_record.get('orderid')
            logger.info(f'the order id looks like {order_id}')

        return {'orderid':order_id}

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")

@app.delete("/delete-node/{node_id}")
def delete_node(node_id: str, type: str) -> dict:
    logger = Logger.get_logger()
    method_name = delete_node.__name__

    try:
        logger.info(f'received DELETE request to the {method_name} route')
        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        if type == 'currency':
            row_summary = driver.execute_query("""
                            MATCH (n: Currency {currencyid: $nodeid})
                            DETACH DELETE n
                        """,
                            nodeid=node_id,
                            database_='neo4j',
                            ).summary
        elif type == 'transaction':
            row_summary = driver.execute_query("""
                            MATCH (n: Transaction {transactionid: $nodeid})
                            DETACH DELETE n
                        """,
                            nodeid=node_id,
                            database_='neo4j',
                            ).summary
        elif type == 'account':
            row_summary = driver.execute_query("""
                            MATCH (n: Account {accountid: $nodeid})
                            DETACH DELETE n
                        """,
                            nodeid=node_id,
                            database_='neo4j',
                            ).summary
        else:
            logger.error(f'***** Unknown node type found in {method_name}')
            return {'number_nodes_deleted' : -1}

        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.nodes_deleted} nodes deleted")

        return {'number_nodes_deleted': db_counters.nodes_deleted}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                    detail=f"Error occurred when trying to DELETE a node, looks like {ex}")

@app.delete("/delete-relationship/{relationship_id}")
def delete_relationship(relationship_id: str, type: str) -> dict:
    logger = Logger.get_logger()
    method_name = delete_relationship.__name__

    try:
        logger.info(f'received DELETE request to the {method_name} route')
        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        if type == 'order':
            row_summary = driver.execute_query("""
                            MATCH (:Transaction)-[o:ORDER {orderid:$orderid}]->(:Currency)
                            DELETE o
                        """,
                            orderid=relationship_id,
                            database_='neo4j',
                            ).summary
        elif type == 'placed_order':
            row_summary = driver.execute_query("""
                            MATCH (:Account)-[po:PLACED_ORDER {placedorderid: $placedorderid}]->(:Transaction)
                            DELETE po
                        """,
                            placedorderid=relationship_id,
                            database_='neo4j',
                            ).summary
        else:
            logger.error(f'***** Unknown relationship type found in {method_name}')
            return {'number_relationships_deleted' : -1}

        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.relationships_deleted} relationships deleted")

        return {'number_relationships_deleted': db_counters.relationships_deleted}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                    detail=f"Error occurred when trying to DELETE a relationship, looks like {ex}")


@app.post("/add-new-account")
def add_new_account_to_db(account: Account) -> dict:
    method_name = add_new_account_to_db.__name__

    try:
        logger.info(f'received POST request to the {method_name} route')

        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        row_summary = driver.execute_query("""
                        CREATE (a: Account {accountname: $accountname, accountid: $accountid})
                    """,
                        accountid=account.accountid,
                        accountname=account.accountname,
                        database_='neo4j',
                        ).summary

        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.nodes_created} nodes created")

        return {'number_accounts_added': db_counters.nodes_created}

    except neo4j.exceptions.ConstraintError as con:
        logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")
        return {'number_accounts_added': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST a new account to the db, looks like {ex}")

@app.post("/add-new-placed-order")
def add_new_placed_order_to_db(placed_order: PlacedOrder) -> dict:
    method_name = add_new_placed_order_to_db.__name__

    try:
        logger.info(f'received POST request to the {method_name} route')

        driver = Neo4j().get_driver()

        db_counters = DBCounters()

        row_summary = driver.execute_query("""
                        MATCH (t:Transaction {transactionid: $transactionid})
                        MATCH (a:Account {accountid: $accountid})
                        CREATE (a)-[:PLACED_ORDER {placedorderid: $placedorderid}]->(t)
                     """,
                    placedorderid=placed_order.placedorderid,
                    transactionid=placed_order.transactionid,
                    accountid=placed_order.accountid,
                    database_='neo4j',
                    ).summary

        db_counters.update_counts(row_summary.counters)

        logger.info(f"There were {db_counters.relationships_created} relationships created")

        return {'number_placed_orders_added': db_counters.relationships_created}

    except neo4j.exceptions.ConstraintError as con:
        logger.error(f"********** Constraint exception encountered in {method_name} looks like {con}")
        return {'number_placed_orders_added': 0}

    except Exception as ex:
        logger.error(f"Exception encountered in {method_name} looks like {ex}")
        raise HTTPException(status_code=404,
                            detail=f"Error occurred when trying to POST a new placed order to the db, looks like {ex}")

@app.get("/check-placed-order")
def check_for_existing_placed_order(placed_order: PlacedOrder) -> dict:
    """ this method will check to see if the provided order already exists in the DB, no DB updates are performed """
    logger = Logger.get_logger()
    method_name = check_for_existing_placed_order.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')
        result = 'false'

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                        MATCH (t:Transaction {transactionid: $transactionid})
                        MATCH (a:Account {accountid: $accountid})
                        MATCH (a)-[po:PLACED_ORDER {placedorderid: $placedorderid}]->(t)
                        RETURN po
                     """,
                    placedorderid=placed_order.placedorderid,
                    transactionid=placed_order.transactionid,
                    accountid=placed_order.accountid,
            database_='neo4j',
            ).records

        if (len(query_records) > 0):
            result = 'true'

        return {'result':result}

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")

@app.get("/placed-order-id")
def get_placed_order_id(placed_order: PlacedOrder) -> dict:
    """ this method will check to see if the provided placed order already exists in the DB, and return the id """
    logger = Logger.get_logger()
    method_name = get_placed_order_id.__name__

    try:
        logger.info(f'received GET request to the {method_name} route')

        driver = Neo4j().get_driver()

        query_records = driver.execute_query("""
                        MATCH (t:Transaction {transactionid: $transactionid})
                        MATCH (a:Account {accountid: $accountid})
                        MATCH (a)-[po:PLACED_ORDER]->(t)
                        RETURN po
             """,
            transactionid=placed_order.transactionid,
            accountid=placed_order.accountid,
            database_='neo4j',
            ).records

        for record in query_records:
            placed_order_record = record['po']
            placed_order_id = placed_order_record.get('placedorderid')
            logger.info(f'the placed order id looks like {placed_order_id}')

        return {'placedorderid':placed_order_id}

    except Exception as ex:
        logger.error(f'Encountered an exception, looks like {ex}')
        raise HTTPException(status_code=404, detail=f"An exception was encountered in {method_name}, looks like {ex}")

