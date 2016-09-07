#!/usr/bin/env python

"""
This application creates an RPC client queue exclusive to this process
which will be used for the RPC responses.  It then makes an RPC request
by publishing a message to the rpc_queue using the nameless exchange.  The
RPC server will receive this message, perform some function, and then
publish the response by sending it back to the client queue.
"""

import amqp
import uuid

from argparse import ArgumentParser

# globals
args = None

def callback(msg):
    """This function is called for each message in its queue."""
    global args

    print("callback %r" % (msg,))

    print("    - properties...")
    for key, val in msg.properties.items():
        print ('        - %s: %s' % (key, str(val)))

    print("    - delivery_info...")
    for key, val in msg.delivery_info.items():
        print ('        - %s: %s' % (key, str(val)))

    print('    - body: %r' % (msg.body,))

    if not args.noack:
        rslt = msg.channel.basic_ack(msg.delivery_tag)
        print("    - basic_ack: %r" % (rslt,))
    else:
        print("    - no acknowledgement needed")

    # complete
    rslt = msg.channel.basic_cancel(msg.delivery_info['consumer_tag'])
    print("    - basic_cancel: %r" % (rslt,))
    print("")

#
#   __main__
#

# many options
parser = ArgumentParser(description=__doc__)
parser.add_argument(
    'arg',
    help='argument to RPC function',
    )
parser.add_argument(
    '--queue', dest='queue',
    help='queue name (default rpc_queue)',
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
parser.add_argument(
    '--noack', dest='noack', action='store_true',
    help='no acknowledgement needed (default: False)',
    default=False,
    )

args = parser.parse_args()
print("args: %r" % (args,))

# get a connection
connection = amqp.Connection(
    args.host,
    userid=args.userid,
    password=args.password,
    ssl=args.ssl,
    )
print("connection: %r" % (connection,))

# connect the connection
# rslt = connection.connect()
# print("connect: %r" % (rslt,))

# get a channel
channel = connection.channel()
print("channel: %r" % (channel,))

# make an interesting (and unique) queue name
queue_name = "rpc.client." + str(uuid.uuid1())
print("queue_name: %r" % (queue_name,))

# create the queue for the response
result_queue = channel.queue_declare(queue_name, exclusive=True)
print("queue_declare: %r" % (result_queue,))

# call the callback for rpc responses
rslt = channel.basic_consume(
    queue_name,
    callback=callback,
    no_ack=args.noack,
    )
print("consuming, rslt: %r" % (rslt,))

# request/response key
correlation_id = str(uuid.uuid1())
print("correlation_id: %r" % (correlation_id,))

# wrap it in a message
msg = amqp.Message(
    args.arg,
    content_type='text/plain',
    reply_to=queue_name,
    correlation_id=correlation_id,
    )
print("msg: %r" % (msg,))

# publish it on the channel, sending it to the exchange
rslt = channel.basic_publish(
    msg,
    exchange='',
    routing_key=args.queue,
    )
print("basic_publish: %r" % (rslt,))

# loop for incoming RPC responses, ^C to quit
try:
    while channel.callbacks:
        rslt = channel.wait()
        print("wait: %r" % (rslt,))
    print("done")
except KeyboardInterrupt:
    pass

# close down
channel.close()
connection.close()
