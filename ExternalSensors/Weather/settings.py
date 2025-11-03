""" This file loads the required API keys from the local environment.
    The code is based on the ambient API library by Amos Vryhof """

import os

try:
    AMBIENT_ENDPOINT = "https://rt.ambientweather.net/v1"
    AMBIENT_APPLICATION_KEY = os.environ["AMBIENT_APPLICATION_KEY"]
    AMBIENT_API_KEY = os.environ["AMBIENT_API_KEY"]
except KeyError:
    pass
