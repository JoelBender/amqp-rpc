# amqp-rpc
An experimental RPC client and server application for exploring the amqp library.

This code works with amqp version 1.4.9 but not for subsequent releases: I can't
figure out what to pass as the first parameter to `channel.wait()` and the `basic_cancel()`
call in the client doesn't break the `wait()` call.


