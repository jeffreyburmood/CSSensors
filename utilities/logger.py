""" this file contains the logger class to be used by the application as a common logging utility """

import logging
import os
from dotenv import load_dotenv

# Source - https://stackoverflow.com/a/47104004
# Posted by Wyrmwood, modified by community. See post 'Timeline' for change history
# Retrieved 2026-08-11, License - CC BY-SA 4.0

import datetime
import pytz

class Formatter(logging.Formatter):
    """override logging.Formatter to use an aware datetime object"""
    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        tzinfo = pytz.timezone('America/Denver')
        return tzinfo.localize(dt)

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec='milliseconds')
            except TypeError:
                s = dt.isoformat()
        return s

class Logger:
    _logger = None

    @staticmethod
    def get_logger():
        if Logger._logger is None:
            load_dotenv()
            log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

            # setup logger and provide name
            Logger._logger = logging.getLogger("appLogger")

            # setup logging level at the logger level
            Logger._logger.setLevel(getattr(logging, log_level))

            # setup the handlers
            console_handler = logging.StreamHandler()
            # console_handler.setLevel(logging.DEBUG)  # set the logging level at the handler level

            # file_handler = logging.FileHandler("logs.log")
            # file_handler.setLevel(logging.DEBUG)  # set the logging level at the handler level

            # create the formatter (same for both handlers in this example
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

            # add formatter to handlers
            console_handler.setFormatter(formatter)
            # file_handler.setFormatter(formatter)

            # add handler to logger
            Logger._logger.addHandler(console_handler)
            # Logger._logger.addHandler(file_handler)

        return Logger._logger
