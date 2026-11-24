A client's new calls now cross the connection age: the server sends the
HTTP/2 GOAWAY at `MAX_CONNECTION_AGE` (30 minutes), so a client moves
its new calls to a new connection while its calls in flight finish, and
the connection is closed at the grace (10 minutes) later, with any call
still in it. Before, tonic's own age timer was given both the age and
the grace, and a build that sets both never sends the GOAWAY: the
connection was force-closed at age + grace under whatever call it held
- the nightly soak's bench lost a call that was in flight at exactly 40
minutes every run and stopped there. The close at the grace is now
ours, on the connection's own IO, and it fires with no traffic.

The client half: a call hyper never put on the wire is sent again. At
the GOAWAY a call still queued in hyper's dispatch comes back canceled
or dispatch-gone (taken by the dispatcher, its HTTP/2 stream pending
open), and tonic drops it, so a back-to-back writer lost a call the
server never saw. The new `Resend` middleware, wrapped around every
scion client's channel, sends such a call again on a connection of its
own - the gate is a hyper error in the chain and the request body
still in its slot, which proves no byte reached the wire - and a call
the server may have seen is never resent.
