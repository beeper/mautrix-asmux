# No longer maintained
mautrix-asmux powered the old Beeper infrastructure from 2020 to 2022. Beeper has since switched
to a new infrastructure based on hungryserv, and this project is no longer maintained.

## mautrix-asmux
A Matrix application service proxy and multiplexer.

The general idea of this project is to make it possible to dynamically provision appservices. The
dynamically provisioned appservices connect to mautrix-asmux with individual access tokens, which
asmux checks and then proxies the requests to Synapse with its own global access token. Synapse is
only aware of one appservice. In the other direction, mautrix-asmux maintains a room ID ->
appservice mapping, which it uses to send incoming events to the correct appservice. As a side
effect, mautrix-asmux implements [MSC2190](https://github.com/matrix-org/matrix-doc/pull/2190).

### Usage
Setup is somewhat similar to Python mautrix bridges (<https://docs.mau.fi/bridges/python/setup.html>),
i.e. copy the example config and fill it, generate registration by running the program with the `-g`
flag, then run it normally.

To actually create dynamic appservices, use the shared secret in the config
and call `POST /_matrix/asmux/appservice/<username>/<prefix>`. The request body
should contain `{"address": "..."}`. It'll return a registration file.

Appservices can also be registered by users by using `/_matrix/asmux/mxauth` as
the prefix instead of `/_matrix/asmux` and a Matrix access token instead of the
shared secret.

To register websocket bridges, set `{"push": false}` instead of address in the
request body.

More details on the management API can be found by reading the code:
<https://github.com/beeper/mautrix-asmux/blob/master/mautrix_asmux/api/management_api.py#L94-L114>
