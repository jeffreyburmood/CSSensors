
import socketio

with socketio.SimpleClient() as sio:
    try:
        sio.connect('https://rt2.ambientweather.net/?api=2&applicationKey=93aad5b84cc34b66ac5e1b05f02fff965fca512785614f309b8ca937077f09ec')
        print(sio.sid)
        print(sio.transport())
        print(sio.connected)
        sio.disconnect()

    except Exception as ex:
        print(f'Exception encountered trying to connect looks like {ex}')
        sio.disconnect()
