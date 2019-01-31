#!/usr/bin/python

"""
RPC client, aioamqp implementation of RPC examples from RabbitMQ tutorial
"""

import asyncio
import signal
import uuid

import aioamqp


class FibonacciRpcClient:
    def __init__(self):
        self.transport = None
        self.protocol = None
        self.channel = None
        self.callback_queue = None
        self.waiter = None

    async def connect(self):
        """ an `__init__` method can't be a coroutine"""
        self.transport, self.protocol = await aioamqp.connect(login_method="PLAIN")
        self.channel = await self.protocol.channel()

        result = await self.channel.queue_declare(queue_name="", exclusive=True)
        self.callback_queue = result["queue"]

        await self.channel.basic_consume(
            self.on_response, no_ack=True, queue_name=self.callback_queue
        )

    async def on_response(self, channel, body, envelope, properties):
        if self.corr_id == properties.correlation_id:
            self.response = body

        self.waiter.set()

    async def call(self, n):
        if not self.protocol:
            await self.connect()
        self.response = None
        self.waiter = asyncio.Event()
        self.corr_id = str(uuid.uuid4())
        await self.channel.basic_publish(
            payload=str(n),
            exchange_name="",
            routing_key="rpc_queue",
            properties={
                "reply_to": self.callback_queue,
                "correlation_id": self.corr_id,
            },
        )
        await self.waiter.wait()

        self.waiter = None
        return int(self.response)

    async def close(self):
        await self.channel.close()
        await self.protocol.close()


async def rpc_client():
    fibonacci_rpc = FibonacciRpcClient()
    print(" [x] Requesting fib(30)")
    response = await fibonacci_rpc.call(30)
    print(" [.] Got %r" % response)

    print(" [x] Requesting fib(10)")
    response = await fibonacci_rpc.call(10)
    print(" [.] Got %r" % response)

    await fibonacci_rpc.close()


loop = asyncio.get_event_loop()

stop = asyncio.Future()
loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

loop.run_until_complete(rpc_client())
