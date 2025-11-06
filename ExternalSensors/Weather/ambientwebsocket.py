""" This file contains the code for the web socket client used to access real time weather station data. """
import asyncio
import socketio
import os

from dotenv import load_dotenv

sio = socketio.AsyncClient()

@sio.event
async def connect():
    try:
        load_dotenv()
        application_key = os.getenv("AMBIENT_APPLICATION_KEY")
        api_key = os.getenv("AMBIENT_API_KEY")
        print(f'Application key = ' + application_key)
        # await sio.connect('https://rt2.ambientweather.net/?api=1&applicationKey='+application_key)
        await sio.connect('https://rt2.ambientweather.net/v1/?applicationKey='+application_key)
        if sio.connected:
            print("Connected to server")
            # build the subscribe data
            subscribe_data = { 'apiKeys': [api_key] }
            await sio.emit('subscribe', subscribe_data)
        else:
            print('Could not connect')
            exit()
    except Exception as ex:
        print(f'Exception encountered trying to connect looks like {ex}')
        await disconnect()


@sio.event
async def disconnect():
    await sio.disconnect()
    print("Disconnected from server")
    exit()

@sio.on('subscribed')
async def on_subscribed(data):
    print(f"Subscribed: {data}")

@sio.on('data')
async def on_data(data):
    print(f"Received data: {data}")

async def main():
    await connect()
    try:
        await sio.wait()
    except KeyboardInterrupt:
        pass
    finally:
        await sio.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
