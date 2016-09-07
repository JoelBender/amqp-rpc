#!/usr/bin/env python

"""
This application creates an RPC queue called rpc_queue accessible via the
nameless exchange.  It listens for incoming requests, then responds by
publishing its response to the queue that was provided by the client.
"""

import amqp

from argparse import ArgumentParser

# globals
args = None

#
#   callback
#

def callback(msg):
    """This function is called for each message in its queue."""

    print("callback %r" % (msg,))

    print("properties...")
    for key, val in msg.properties.items():
        print ('    - %s: %s' % (key, str(val)))

    print("delivery_info...")
    for key, val in msg.delivery_info.items():
        print ('    - %s: %s' % (key, str(val)))

    print('    - body: %r' % (msg.body,))

    # parse the request, build a response
    request = int(msg.body)
    response = str(request * 10000)

    # wrap it in a message
    response_msg = amqp.Message(
        response,
        content_type='text/plain',
        correlation_id=msg.correlation_id,
        )
    print("    - response_msg: %r" % (response_msg,))

    # send this message back to the caller
    rslt = msg.channel.basic_publish(
        response_msg,
        exchange='',
        routing_key=msg.reply_to,
        )
    print("    - basic_publish: %r" % (rslt,))

    # ack that we processed this request
    if not args.noack:
        rslt = msg.channel.basic_ack(msg.delivery_tag)
        print("    - basic_ack: %r" % (rslt,))
    else:
        print("    - no acknowledgement needed")
    print("")

#
#   __main__
#

# many options
parser = ArgumentParser(description=__doc__)
parser.add_argument(
    '--queue', dest='queue',
    help='queue name (default rpc_queue)',
    default='rpc_queue',
    )
parser.add_argument(
    '--host', dest='host',
    help='server to connect to (default: localhost)',
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
    help='no acknowledgement needed (default: false)',
    default=False,
    )
parser.add_argument(
    '--durable', dest='durable', action='store_true',
    help='Durable queue, remains active when server restarts (default: false)',
    default=False,
    )
parser.add_argument(
    '--auto_delete', dest='auto_delete', action='store_true',
    help='Auto-delete (default: false)',
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

# connect the connection?
# rslt = connection.connect()
# print("connect: %r" % (rslt,))

# get a channel
channel = connection.channel()
print("channel: %r" % (channel,))

# create the queue for the requests
request_queue = channel.queue_declare(
    args.queue,
    exclusive=False,
    durable=args.durable,
    auto_delete=args.auto_delete,
    )
print("queue_declare: %r" % (request_queue,))

# prefetch messages, no specific limit on size, but only one, and
# only for this channel
channel.basic_qos(
    prefetch_size=0,
    prefetch_count=1,
    a_global=False,
    )

# call the callback when consuming
rslt = channel.basic_consume(
    args.queue,
    callback=callback,
    no_ack=args.noack,
    )
print("basic_consume: %r" % (rslt,))

# loop for incoming RPC requests, ^C to quit
try:
    while True:
        rslt = channel.wait()
        print("wait: %r" % (rslt,))
    print("done")
except amqp.exceptions.ConsumerCancelled:
    print("consumer canceled")
except KeyboardInterrupt:
    pass

# close down
channel.close()
connection.close()
