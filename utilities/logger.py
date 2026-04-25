""" this file contains the logger class to be used by the application as a common logging utility """

import logging
import os
from dotenv import load_dotenv

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
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s")

            # add formatter to handlers
            console_handler.setFormatter(formatter)
            # file_handler.setFormatter(formatter)

            # add handler to logger
            Logger._logger.addHandler(console_handler)
            # Logger._logger.addHandler(file_handler)

        return Logger._logger
