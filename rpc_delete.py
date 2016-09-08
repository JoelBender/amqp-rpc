#!/usr/bin/env python

"""
This application deletes the rpc_queue which is usually created 'durable' so
it survives even if the rpc_server.py application crashes or disconnects. The
parameters provide to the server when the queue is created must be consistent
each time the server is run, so if you want to change the parameters, you have
to make sure there are no servers running and then delete the queue.
"""

import sys
import amqp
import time

from argparse import ArgumentParser

#
#   __main__
#

# many options
parser = ArgumentParser(description=__doc__)
parser.add_argument(
    '--queue', dest='queue',
    help='name of the queue to delete (default: rpc_queue)',
    default='rpc_queue',
    )
parser.add_argument(
    '--host', dest='host',
    help='AMQP server to connect to (default: localhost)',
    default='localhost',
    )
parser.add_argument(
    '--userid', dest='userid',
    help='userid to authenticate as (default: guest)',
    default='guest',
    )
parser.add_argument(
    '--password', dest='password',
    help='password to authenticate with (default: guest)',
    default='guest',
    )
parser.add_argument(
    '--ssl', dest='ssl', action='store_true',
    help='enable SSL (default: not enabled)',
    default=False,
    )

args = parser.parse_args()
print("args: %r" % (args,))

connection = channel = request_queue = None

for i in range(5):
    try:
        # get a connection
        connection = amqp.Connection(
            args.host,
            userid=args.userid,
            password=args.password,
            ssl=args.ssl,
            )
        print("connection: %r" % (connection,))

        # try to connect
        rslt = connection.connect()
        print("connect: %r" % (rslt,))

        # get a channel
        channel = connection.channel()
        print("channel: %r" % (channel,))

        # delete the queue
        result = channel.queue_delete(queue=args.queue)
        print("queue_delete: %r" % (result,))

        break

    except ConnectionError as err:
        print("connection error: %r" % (err,))
        time.sleep(5.0)
else:
    print("no connection")
    sys.exit(1)

# close down
if channel:
    channel.close()
if connection:
    connection.close()
