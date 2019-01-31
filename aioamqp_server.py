#!/usr/bin/python

"""
RPC server, aioamqp implementation of RPC examples from RabbitMQ tutorial
"""

import asyncio
import signal

import aioamqp


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


class FibonacciRpcServer:
    def __init__(self):
        self.transport = None
        self.protocol = None
        self.channel = None
        self.callback_queue = None

        self.keep_running = True

    async def connect(self):
        self.transport, self.protocol = await aioamqp.connect(login_method="PLAIN")

        self.channel = await self.protocol.channel()

        await self.channel.queue_declare(queue_name="rpc_queue")
        await self.channel.basic_qos(
            prefetch_count=1, prefetch_size=0, connection_global=False
        )
        await self.channel.basic_consume(self.on_request, queue_name="rpc_queue")
        print(" [x] Awaiting RPC requests")

    async def disconnect(self):
        print(" [x] Disconnect")

    async def stop(self):
        self.keep_running = False
        print(" [x] Stopping")

        await self.channel.close()
        print(" [x] Channel closed")

        await self.protocol.close()
        print(" [x] Protocol closed")

    async def main_loop(self):
        try:
            while self.keep_running:
                await self.connect()
                await self.protocol.wait_closed()
                await self.disconnect()
            print(" [x] Not running")
        except Exception as err:
            print(" [X] Exception: %r" % (err,))
            await self.disconnect()

    async def on_request(self, channel, body, envelope, properties):
        n = int(body)

        print(" [.] fib(%s)" % n)
        response = fib(n)

        await channel.basic_publish(
            payload=str(response),
            exchange_name="",
            routing_key=properties.reply_to,
            properties={"correlation_id": properties.correlation_id},
        )

        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


async def shutdown(loop):
    tasks = [
        t
        for t in asyncio.Task.all_tasks()
        if t is not asyncio.Task.current_task(loop) and not t.done
    ]
    print("Outstanding tasks: {!r}".format(tasks))

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks)
    print("Outstanding tasks canceled")

    loop.stop()
    print("Shutdown complete.")


loop = asyncio.get_event_loop()
server = FibonacciRpcServer()

signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
for s in signals:
    loop.add_signal_handler(s, lambda s=s: loop.create_task(server.stop()))

loop.run_until_complete(server.main_loop())
loop.run_until_complete(shutdown(loop))
