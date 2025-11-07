""" This file contains the code for the web socket client used to access real time weather station data. """
import asyncio
import socketio
import os

from dotenv import load_dotenv

sio = socketio.AsyncClient()

@sio.event
async def connect():
    try:
        if sio.connected:
            print("Connected to server")
            print(f'Connection ID = {sio.sid}')
            print(f'Connection transport type = {sio.transport()}')
            # build the subscribe data
            api_key = os.getenv("AMBIENT_API_KEY")
            subscribe_data = { 'apiKeys': [api_key] }
            await sio.emit('subscribe', subscribe_data)
        else:
            print(f'Did not connect, connected = {sio.connected}')
    except Exception as ex:
        print(f'Exception encountered trying to connect looks like {ex}')


@sio.event
async def disconnect():
    print("Disconnected from server")

@sio.on('subscribed')
async def on_subscribed(data):
    print(f"Subscribed: {data}")

@sio.on('data')
async def on_data(data):
    print(f"Received data: {data}")

async def main():
    load_dotenv()
    application_key = os.getenv("AMBIENT_APPLICATION_KEY")
    try:
        await sio.connect('https://rt2.ambientweather.net/?api=2&applicationKey='+application_key)
        await sio.wait()
    except KeyboardInterrupt:
        pass
    except Exception as ex:
        print(f'Exception encountered in main loop looks like {ex}')
    finally:
        await sio.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
