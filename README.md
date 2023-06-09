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
