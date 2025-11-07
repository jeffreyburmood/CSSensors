import os

import socketio
from dotenv import load_dotenv

# Create a Socket.IO client instance
sio = socketio.Client()

# Events
@sio.event
def connect():
    print("Connected to server")
    print(f'the connection id = {sio.sid}')
    # Example: Subscribe when connected
    api_key = os.getenv("AMBIENT_API_KEY")
    subscribe_data = { 'apiKeys': [api_key] }
    sio.emit('subscribe', subscribe_data)

@sio.event
def disconnect():
    print("Disconnected from server")

@sio.on('subscribed')
def on_subscribed(data):
    print(f"Subscribed: {data}")

@sio.on('data')
def on_data(data):
    print(f"Received data: {data}")
    # Example: Unsubscribe after data is received
    api_key = os.getenv("AMBIENT_API_KEY")
    sio.emit('unsubscribe', { 'apiKeys': [api_key] })

def main():
    try:
        load_dotenv()
        application_key = os.getenv("AMBIENT_APPLICATION_KEY")

        sio.connect('https://rt2.ambientweather.net/?api=2&applicationKey='+application_key)
        print(sio.sid)
        print(sio.connected)
        print(sio.transport())
        # Run for some time, then disconnect
        sio.sleep(30)

    except Exception as ex:
        print(f'Exception encountered trying to connect looks like {ex}')

    finally:
        sio.disconnect()

if __name__ == '__main__':
    main()
