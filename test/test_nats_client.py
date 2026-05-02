""" this file contains the code to exercise the nats message client when listenting for
    multiple subject messages. """

import asyncio
import signal
import nats
# from nats.aio.client import Client as NATS

stop_event = asyncio.Event()

async def process_message(subject: str, data: str):
    # Add your message processing logic here
    print(f"Processing message - Subject: {subject}, Data: {data}")


async def main():
    # nc = NATS()

    nc = await nats.connect("nats://localhost:4222")

    subjects = ["subject.one", "subject.two", "subject.three"]

    async def message_handler(msg):
        subject = msg.subject
        data = msg.data.decode()
        print(f"Received message on subject '{subject}': {data}")
        await process_message(subject, data)

    subscriptions = []
    for subject in subjects:
        sub = await nc.subscribe(subject, cb=message_handler)
        subscriptions.append(sub)
        print(f"Subscribed to subject: {subject}")

    print("Waiting for messages. Press Ctrl+C to exit.")

    # stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()

    print("Shutting down...")
    for sub in subscriptions:
        await sub.unsubscribe()
    await nc.drain()
    await nc.close()
    print("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
