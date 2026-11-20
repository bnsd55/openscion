# Which pairs work at versions N and N-1

The v0.2.0 gate asks for "clients, servers, caches and daemons of
version N and N-1 work together, per a written compatibility policy"
([ROADMAP.md](ROADMAP.md)). This file is that policy. The contracts
themselves live in [stability.md](stability.md); this file says who may
talk to whom, what that talk must deliver, and what a break needs. All
five crates carry one version ([RELEASING.md](RELEASING.md)), so "at
version N" names a release, not a per-crate number.

**N-1 means the previous release, including the previous minor**: a
0.2 client, tier or daemon works with a 0.1 server, and the reverse.
Per-node daemons and clients do not all upgrade at once, so the
promise reaches back one minor, not one release. A patch (`0.1.z`)
keeps every pair below working in both directions.

## The moving pieces

- `scion-server`: the store (metadata + blobs) and the v1 gRPC services.
- `scion-cache` and `scion-daemon`: a tier in front of a server (or of
  another tier); the daemon adds a per-node unix socket and snapshots.
- `scn`: the client — commands, checkouts, and the mount.
- The store on disk: a Postgres schema (the metadata) plus the data
  directory's blob files, which a server opens together.

## The pairs, and what "works" means

| Pair | Versions | "Works" means |
|---|---|---|
| `scn` ↔ `scion-server` | N vs. N-1, both directions | Every RPC of `scion.v1.*` the client sends is answered within [protocol-v1.md](protocol-v1.md)'s status codes and limits; the ticket it holds verifies; the commands it runs do what [cli.md](cli.md) says |
| `scion-cache` / `scion-daemon` ↔ `scion-server` | N vs. N-1, both directions | Reads forwarded within the same v1 contract; the ticket verifies; snapshots and tickets follow their versioned formats (stability.md) |
| `scn` ↔ `scion-daemon` (the `unix://` path) | N vs. N-1, both directions | Reads over the socket behave as [cli.md](cli.md) documents; the CLI's `--json` rules are stability.md's |
| `scion-server` N ↔ a store of N-1 | One direction only | The open migrates forward, one transaction per released migration; a store newer than the build is refused (stability.md's schema rows) |
| `scn` ↔ repositories of any 0.x | Forever, both directions | Object bytes and IDs never change (object format v1): a checkout of anything any 0.x server wrote reads on any later build |
| tier ↔ tier (a chain of caches) | N vs. N-1, both directions | Forwarding stays within the v1 object contract; a chain works when every hop does |

A pair this table does not list has no cross-version promise.

## What a break needs

[stability.md](stability.md) sets the base rule: a patch (`0.1.z`)
fixes bugs and never changes a contract. A minor (`0.x.0`) may change
anything stability.md does not pin, and this file adds what a change
that touches a listed pair must carry:

- A minor that changes the wire contract serves the new protocol
  version beside the old one — the version is the gRPC service name, so
  both live on one server — keeps the old version through that whole
  minor, and drops it no earlier than the next minor; the release
  notes of minor N name what minor N+1 will drop.
- The same reaches the other versioned formats: a ticket reader of N
  accepts a ticket of N-1, and a snapshot reader of N accepts the
  magic of N-1. A minor that issues a new ticket or snapshot format
  keeps reading the old one through that minor and drops it no earlier
  than the next minor, named in the release notes the same way.
- In the objects, a new format version sits beside v1, never a rewrite
  of the bytes an ID names.
- In the store, the migrations stay forward-only, appended at the end
  of the list, so a store of N-1 opens on N; the reverse direction
  stays refused. The store pair is one-directional by design.
- The release notes of every minor name each pair it narrows.

Within a minor line, a newer client keeps working against an N-1
server: it sends nothing the old server cannot answer. A minor that
adds a call also makes its client handle `UNIMPLEMENTED` from an N-1
server: it fails that one command with an error that names the server
version it needs, and every other command keeps working.

## What is proven today, and what lands later

Today the promises rest on the contracts and their tests: the golden
tests pin the object bytes and IDs; the conformance job exercises
protocol v1 with a non-Rust client; the migrations tests prove the
schema migrates; the demo and the release smoke run a server and
`scn` from one tree.

Cross-version is the version matrix's check, and it runs daily
([.github/workflows/version-matrix.yml](../.github/workflows/
version-matrix.yml), on the schedule and on a dispatch that names the
N-1 ref): it builds the tagged N-1 binaries and HEAD's binaries and
runs the pairs of the table as
[scripts/version-matrix.sh](../scripts/version-matrix.sh) names them -
`scn` ↔ server both directions, a tier ↔ server both directions,
`scn` ↔ daemon over `unix://`, the store one direction, the ticket
window both directions, the snapshot window both directions, and the
transcript corpus's N-1 legs. The tier↔tier chain is not a pair of its
own: the policy's chain rule follows from every hop, and each hop is
the tier pair. `scn` ↔ repositories needs no built binaries - the
golden object tests pin the bytes and IDs. A minor that adds a call
must leave the client able to handle `UNIMPLEMENTED` from an N-1
server; the matrix cannot force that from two arbitrary commits (which
call is new is not known until the pair exists), so a release that
adds a call proves the fallback in its own PR, and the conformance job
covers the status codes.
