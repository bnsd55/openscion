<p align="center">
  <img src="https://github.com/bnsd55/openscion/raw/main/docs/assets/banner.jpg" alt="openscion logo: a young scion grafted onto a rootstock whose roots are circuit traces" width="720">
</p>

# openscion

[![CI](https://github.com/bnsd55/openscion/actions/workflows/ci.yml/badge.svg)](https://github.com/bnsd55/openscion/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.88+](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)

A version control server for large monorepos where most of the writers are
AI agents. The server keeps the history and makes the commits. Clients read
only the files they need and never clone. Written in Rust.

> **Alpha (0.x).** One server process on one machine. Not ready for
> production code yet. What holds between versions:
> [docs/stability.md](docs/stability.md). What comes next:
> [docs/ROADMAP.md](docs/ROADMAP.md).

openscion is an independent, open-source implementation of the design in
Anthropic's
[Redesigning source control](https://www.anthropic.com/engineering/redesigning-source-control)
(October 2026). It is not affiliated with or endorsed by Anthropic, and it
contains no Anthropic code. It is not related to the
[SCION Internet architecture](https://github.com/scionproto/scion), to
[GoogleCloudPlatform/scion](https://github.com/GoogleCloudPlatform/scion), or
to the `scion` crate on crates.io.

## Why

Git assumes that every clone is a full copy and that every merge starts on a
developer's machine. That breaks down when thousands of agents and CI jobs
write to one large repository all day: the article reports more than 30,000
merges and about 25 million CI checkouts per day on about 100 GB of history.
Each job copies data it does not need, and each merge needs a fresh clone, a
rebase and a push that fails when someone else pushed first.

## How it works

```
 git:        agent ── clone everything ──> work ── rebase, push ──> server
 openscion:  agent ── "src/a.rs changed" ──> server: commit, rebase, move main
```

- **The server does the work.** History, commits and rebases live on the
  server. An agent reads files over the API, sends the files it changed, and
  asks the server to land them.
- **A changeset lists only what changed.** Files and trees are stored once,
  by content hash (BLAKE3), and never change. A changeset costs as much as
  the change, not as much as the repository.
- **Landing is a server-side rebase.** If none of a changeset's paths
  changed on `main` since its base, the server replays it on the newest
  `main` and moves `main` with one compare-and-swap. Agents that change
  different files never conflict and never pull.
- **Derived data is computed from the parents.** Full trees, path history and
  read views are computed from the parent plus the change, then cached.

```
agent A changes a.rs ──land──> main 101   ok
agent B changes b.rs ──land──> main 102   ok: the server rebased B onto 101
agent C changes a.rs ──land──> conflict   a.rs changed after C's base
```

For an agent, the whole loop is three commands:

```sh
scn cat main:src/lib.rs                                    # read one file, no clone
cs=$(scn commit -m "fix parser" --put src/lib.rs=./lib.rs) # create a changeset
scn land "$cs"                                             # rebase onto main and move it
```

`scn land` exits non-zero on a conflict and prints the paths. A retry of the
same land returns the first result. The interface is text: commands, plain
files and log lines. Every command takes `--json`, the stable form for
scripts and assistive tools ([docs/stability.md](docs/stability.md)).

## Quick start

You need Rust 1.88+, `protoc` (`brew install protobuf` or
`apt install protobuf-compiler`) and Docker, or your own PostgreSQL 16-18
server. The metadata goes to Postgres, and the file contents to the data
directory.

```sh
cargo build --release
export PATH="$PWD/target/release:$PATH"
export SCION_TOKEN=dev SCION_SERVER=http://127.0.0.1:7878

# Postgres for the metadata.
docker run -d --name scion-pg -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=scion postgres:17
until docker exec scion-pg pg_isready -q -h 127.0.0.1; do sleep 1; done
export SCION_POSTGRES_URL=postgres://postgres:scion@127.0.0.1:5432/postgres

# Import a git repository and start the server.
git clone https://github.com/tokio-rs/tokio /tmp/tokio
install -d -m 0700 /tmp/scion
scion-server --data /tmp/scion import /tmp/tokio
scion-server --data /tmp/scion ticket-key --out /tmp/scion/ticket.key > /tmp/scion/ticket.pub
scion-server --data /tmp/scion serve --ticket-key /tmp/scion/ticket.key &
until scn log -n 1 >/dev/null 2>&1; do sleep 1; done

# Read, check out, change and land.
scn log -n 5
scn cat main:README.md
scn checkout main --into /tmp/work --sparse tokio/src
echo hello > hello.txt
cs=$(scn commit -m "add hello" --put hello.txt=hello.txt)
scn land "$cs"
```

Every `scn` command and flag is in [docs/cli.md](docs/cli.md). The server's
commands and a real deployment (an access file, TLS, the cache tiers, backups
and upgrades) are in [docs/operations.md](docs/operations.md). The checkout
benchmark against git is in [docs/benchmarks.md](docs/benchmarks.md).

## Components

| Binary | Role |
|---|---|
| `scion-server` | The server: storage, commits, lands, reads, git import, `fsck`, `gc`, backup and restore |
| `scn` | The command-line client: read, check out, mount (read-only NFS), commit, land, diff, log, blame |
| `scion-cache` | A regional read cache in front of the server |
| `scion-daemon` | A per-machine cache on a Unix socket, in front of a regional cache |

| Crate | Contents |
|---|---|
| [`scion-core`](crates/scion-core) | The object model and the frozen byte format |
| [`scion-proto`](crates/scion-proto) | The gRPC API ([`proto/scion.proto`](proto/scion.proto)) |
| [`scion-server`](crates/scion-server) | The server |
| [`scion-tier`](crates/scion-tier) | `scion-cache` and `scion-daemon` |
| [`scn`](crates/scn) | The client |

## Documentation

| Read this | To learn |
|---|---|
| [operations.md](docs/operations.md) | How to deploy and run a server, and what each failure means |
| [cli.md](docs/cli.md) · [cli-vs-git.md](docs/cli-vs-git.md) | Every `scn` command, and its closest `git` equivalent |
| [architecture.md](docs/architecture.md) | How the parts fit together, and the choices the article leaves open |
| [protocol-v1.md](docs/protocol-v1.md) · [format-v1.md](docs/format-v1.md) | The API contract and the byte format |
| [threat-model.md](docs/threat-model.md) · [faults.md](docs/faults.md) | The security invariants, and each fault with the test that proves it |
| [stability.md](docs/stability.md) · [compatibility.md](docs/compatibility.md) | What each release promises, and which versions work together |
| [support.md](docs/support.md) | The platform matrix and what each severity level gets |
| [benchmarks.md](docs/benchmarks.md) | How the benchmarks run and how to rerun them |
| [verify.md](docs/verify.md) | How to check a release: the signed tag, checksums and attestations |

## Platforms

Linux on x86-64 and macOS on Apple Silicon are supported. Linux on arm64 is
best effort. Windows is not supported. CI tests PostgreSQL 16, 17 and 18. The
exact matrix, with the CI job behind each cell, is in
[docs/support.md](docs/support.md).

## Build and test

```sh
cargo build --release --locked
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

[CONTRIBUTING.md](CONTRIBUTING.md) has the full setup, the Postgres test
suite and fuzzing.

## Contributing

Issues and discussion are welcome. Before a large pull request, open an issue
to agree on the approach. Report a security problem privately, as
[SECURITY.md](SECURITY.md) describes.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
