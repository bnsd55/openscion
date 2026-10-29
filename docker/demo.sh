#!/bin/sh
# Builds the image and runs the whole demo: a server and a cache with TLS,
# a repo seeded with one commit, and a checkout through the cache.
#
#   sh docker/demo.sh
#
# The server keeps its metadata in Postgres; compose.yml carries the
# database service.
#
# Every secret here is made by this script and stays in docker/certs:
# nothing is committed, and nothing is baked into the image.
set -eu
cd "$(dirname "$0")"

compose() {
    docker compose "$@"
}

# A failure after the services started is diagnosed from their logs, and
# nothing is left running: the trap says what each container saw, then
# takes the project down with its volumes, as the success path does. The
# down runs through compose(), so it is scoped to this project's files.
trap 'compose logs --tail 200; compose down --volumes' EXIT

if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is needed to make the demo certificates" >&2
    exit 1
fi

# --- the demo's secrets, made fresh each run ---------------------------
# One token for the clients, one password for the demo database. Both are
# exported for compose to interpolate: nothing about them is committed or
# baked into the image, and a second run of the demo carries different
# secrets than the first.
SCION_DEMO_TOKEN="$(openssl rand -hex 24)"
SCION_DEMO_DB_PASSWORD="$(openssl rand -hex 16)"
export SCION_DEMO_TOKEN SCION_DEMO_DB_PASSWORD

# --- certificates: one CA, one certificate per service -----------------
rm -rf certs work
mkdir -p certs work

openssl req -x509 -newkey ed25519 -nodes -days 30 \
    -keyout certs/ca.key -out certs/ca.pem \
    -subj "/CN=scion demo ca" >/dev/null 2>&1

# The server reaches the database over the network, so #178's rule
# applies: the URL says sslmode=require, and --postgres-ca names the
# demo CA that signed the database's certificate.
for name in postgres; do
    openssl req -newkey ed25519 -nodes \
        -keyout "certs/$name.key" -out "certs/$name.csr" \
        -subj "/CN=$name" >/dev/null 2>&1
    printf "subjectAltName=DNS:%s\n" "$name" > "certs/$name.ext"
    openssl x509 -req -days 30 \
        -in "certs/$name.csr" -CA certs/ca.pem -CAkey certs/ca.key \
        -CAcreateserial -extfile "certs/$name.ext" \
        -out "certs/$name.crt" >/dev/null 2>&1
    rm "certs/$name.csr" "certs/$name.ext"
done

for name in server cache; do
    openssl req -newkey ed25519 -nodes \
        -keyout "certs/$name.key" -out "certs/$name.csr" \
        -subj "/CN=$name" >/dev/null 2>&1
    printf "subjectAltName=DNS:%s\n" "$name" > "certs/$name.ext"
    openssl x509 -req -days 30 \
        -in "certs/$name.csr" -CA certs/ca.pem -CAkey certs/ca.key \
        -CAcreateserial -extfile "certs/$name.ext" \
        -out "certs/$name.crt" >/dev/null 2>&1
    rm "certs/$name.csr" "certs/$name.ext"
done
chmod 600 certs/*.key
chmod 644 certs/*.pem certs/*.crt

# --- the image, then the server's ticket key ---------------------------
# CI builds the image first with the gha layer cache and tags it
# open-scion-demo; compose builds only when the image is missing.
docker image inspect open-scion-demo >/dev/null 2>&1 || compose build

# The containers read the certificates from a volume, so the keys inside
# are 0600 and owned by their user, whatever owns the host's copies. The
# database's key lands in its own volume before the database starts, or
# Postgres refuses to start with a key it cannot read.
compose run --rm certs-init '
    cp /host-certs/* /certs/
    chmod 600 /certs/*.key
    chmod 644 /certs/*.crt /certs/*.pem
    chown -R 10001:10001 /certs
'
compose run --rm pg-certs-init

# The server flags that name the database. The Postgres host is the
# network's `postgres`, which is not local, so the CA rides along, and
# the password is the demo run's own.
PG_ARGS="--postgres-url postgres://scion:${SCION_DEMO_DB_PASSWORD}@postgres:5432/scion?sslmode=require --postgres-ca /certs/ca.pem"
compose up -d postgres
for _ in $(seq 1 30); do
    if compose run --rm --entrypoint pg_isready postgres -h postgres -U scion >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
compose run --rm --entrypoint pg_isready postgres -h postgres -U scion \
    || { compose logs --tail 20 postgres; echo "postgres did not come up" >&2; exit 1; }

# The subcommand prints the public key on its last line; the cache
# verifies tickets against it. The key file itself lands in the server's
# data volume, written by the server's own user.
PUB=$(compose run --rm --entrypoint scion-server server \
    --data /data $PG_ARGS ticket-key --out /data/ticket.key | tail -n 1)
if [ -z "$PUB" ]; then
    echo "scion-server ticket-key printed no public key" >&2
    exit 1
fi
export SCION_TICKET_PUB="$PUB"

# The containers read the certificates from a volume, so the keys inside
# are 0600 and owned by their user, whatever owns the host's copies.
compose run --rm certs-init '
    cp /host-certs/* /certs/
    chmod 600 /certs/*.key
    chmod 644 /certs/*.crt /certs/*.pem
    chown -R 10001:10001 /certs
'
echo "starting the server and the cache"
compose up -d server cache

# The one-shot containers resolve the services through the network's DNS,
# which needs a moment on a fresh network. On a timeout, the service logs
# say why.
wait_for() {
    for _ in $(seq 1 30); do
        if compose run --rm scn "getent hosts $1 >/dev/null"; then
            return 0
        fi
        sleep 1
    done
    compose logs --tail 20
    echo "$1 did not come up" >&2
    exit 1
}
wait_for server
wait_for cache

# --- a repo with one commit, then a checkout through the cache ---------
# A repo's first changeset comes from a git import: the protocol asks for
# exactly one parent per client-made changeset, and only the import writes
# a root. The git repo lives in the host's work dir and rides in read-only.
echo "seeding the demo repo"
mkdir -p work/seed
printf "hello from git\n" > work/seed/README.md
git -C work/seed init -q
git -C work/seed add -A
git -C work/seed -c user.name=demo -c user.email=demo@localhost commit -qm "the seed commit"
compose run --rm --entrypoint scion-server server \
    --data /data $PG_ARGS import --repo demo /host-work/seed

ID=$(compose run --rm scn '
    set -eu
    mkdir -p /work/src
    printf "hello from scion\n" > /work/src/README.md
    scn commit --json -m "the demo commit" --put README.md=/work/src/README.md
' | sed -n "s/.*\"changeset\": *\"\([0-9a-f]*\)\".*/\1/p")
compose run --rm scn "scn land $ID --onto main"

echo "checking out main through the cache"
compose run --rm scn '
    set -eu
    scn checkout main --into /work/out
    diff -r /work/src /work/out
    echo DEMO-OK
'

trap - EXIT
compose down --volumes
