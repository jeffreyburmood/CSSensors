""" this file contains code to test the apscheduler functionslity """
import asyncio
import logging
from datetime import datetime

# Currently using version 3.11.x for aspscheduler, the 4.x versions (master in github) are PRE-RELEASE!
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utilities.logger import Logger

logger = Logger.get_logger()
logger.setLevel(logging.INFO)

scheduler = AsyncIOScheduler(logger=logger)

def tick():
    print("Hello, the time is", datetime.now())

async def main():
    """ RUnState = starting, started, stopping, stopped """
    try:
        # engine = create_async_engine(
        #     "postgresql+asyncpg://postgres:secret@localhost/testdb"
        # )
        # data_store = SQLAlchemyDataStore(engine)

        scheduler.add_job(tick, 'interval', seconds=5, id='ticker')
        job_list = scheduler.get_jobs()
        for a_job in job_list:
            logger.info(f'a job looks like: {a_job}')

        state = scheduler.state
        logger.info(f'the scheduler state = {state}')

        scheduler.start()
        state = scheduler.state
        logger.info(f'now the scheduler state = {state}')

        await asyncio.sleep(15)

        logger.info('now getting ready to terminate....')
        print('now getting ready to terminate....')

    except Exception as ex:
        print(f'exception encountered in main(), looks like: {ex}')

if __name__ == "__main__":
    try:
        asyncio.run(main())

    except Exception as ex:
        logger.error(f"Exception encountered trying to start main(), looks like {ex}")
