#!/usr/bin/env python

"""
Run RabbitMQ:

$ docker run --detach --name rabbitmq --publish 127.0.0.1:5672:5672 --publish 127.0.0.1:15672:15672 rabbitmq:4.1.0-beta.4-management

Install python dependencies:
$ pip install aio_pika==9.5.4

Run the test:
$ python channel-half-closed.py
"""

import aio_pika
import asyncio
import logging


CONNECTION_URL = "amqp://guest:guest@localhost/"
# logging.basicConfig(level=logging.DEBUG)


async def on_message(message, channel) -> None:
    await message.ack()

    try:
        # Send incorrect message to force the channel error
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=b"outgoing message",
                expiration=-1,
            ),
            routing_key="",
        )
    except asyncio.CancelledError:
        pass


async def main() -> None:
    connection = await aio_pika.connect(CONNECTION_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    loop = asyncio.get_running_loop()
    connection.close_callbacks.add(lambda *a: loop.stop())

    queue = await channel.declare_queue(name="test-queue", durable=True)

    # Fill the queue to consume later
    for i in range(100):
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"incoming messages"),
            routing_key="test-queue",
        )

    # Start consuming
    await queue.consume(lambda m: on_message(m, channel))


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
    print("\n====== Feel free to ignore logs below this line =======\n")
