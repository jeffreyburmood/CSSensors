
from ambientapi import AmbientAPI
import time

api = AmbientAPI()

devices = api.get_devices()

for device in devices:
    print(device)

time.sleep(1) #pause for a second to avoid API limits

# print(device.get_data())