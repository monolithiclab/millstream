# Deploying Millstream

One binary is the engine, the CLI and a node, with the web UI embedded. It runs the same packages everywhere; the
environment file, `millstream.yaml`, says where (spec section 14). This guide covers the three targets: a laptop, a
VPS with Docker Compose, and a VPS with systemd.

> **M4 status.** `millstream serve`, `apply`, `rollback` and `disable`, every key of the environment file, the
> standby's takeover, remote nodes with the node token over TLS, the S3 artifact store, token auth, `/metrics`,
> retention and `millstream purge` are in (Tasks 2, 3, 4, 7 and 8 of the [M4 plan](plans/20261016.m4-server.md); see
> Standby and failover, and Retention and purge).

## The artefacts

| Artefact | Where | Contents |
| --- | --- | --- |
| Container image | `make image` (tag `millstream:dev`); releases at `ghcr.io/monolithiclab/millstream` | Distroless static, uid 65532, `ENTRYPOINT ["/millstream"]` |
| Release archives | `make release-snapshot` writes `dist/` | `linux`/`darwin` × `amd64`/`arm64`, an SPDX SBOM each, `SHA256SUMS` |
| Compose file | [`deploy/compose/`](../deploy/compose/) | Postgres 17, an active and a standby engine, two nodes |
| systemd units | [`deploy/systemd/`](../deploy/systemd/) | `millstream.service` (engine), `millstream-node.service` (node) |

The image is built from source by the [`Dockerfile`](../Dockerfile): the UI in the node image `make ui` uses, the
binary with the UI embedded, then the distroless runtime. Every base image is pinned by digest. Releases are not
published yet: GoReleaser ([`.goreleaser.yaml`](../.goreleaser.yaml)) builds snapshots only. When releases start,
`SHA256SUMS` carries a cosign keyless signature (`SHA256SUMS.sigstore.json`) and the image is signed by digest:

```sh
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/monolithiclab/millstream/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
```

## Laptop

`millstream run` in a package directory runs the engine and an embedded node on SQLite (`.millstream/` beside the
package) and serves the API and the UI on `127.0.0.1:7070`. No environment file is needed. In the image:

```sh
docker run --rm -v "$PWD:/pkg:ro" millstream:dev run --once /pkg
```

The image has no shell and no Docker CLI: its embedded node runs builtin steps and exec actions that are static
Linux binaries. Interpreted actions, container actions and sandboxed agents need the binary on the host.

## VPS with Docker Compose

```sh
cd deploy/compose
cp .env.example .env        # set every value: openssl rand -hex 32
$EDITOR millstream.yaml     # ui.baseUrl, registries, secrets, retention
docker compose up -d
```

- **Services.** `postgres` (not published), `engine` and `engine-standby` (the API on the host's `127.0.0.1:7070`
  and `127.0.0.1:7071`), `node-1` and `node-2`. Both engines run the same command: the one holding the store's
  advisory lock is active, the other is the hot standby and takes over when it fails.
- **Containers.** Read-only root, every capability dropped, `no-new-privileges`. The engines share a volume for their
  working directory; each node has its own for its package cache.
- **The environment file**, [`deploy/compose/millstream.yaml`](../deploy/compose/millstream.yaml), is mounted
  read-only; its keys are spec section 14's. It names secrets by variable (`{ env: MILLSTREAM_TOKEN }`) and
  `compose.yaml` passes those from `.env`, which is git-ignored. A pipeline's secrets need both: an entry under
  `secrets` and the variable added to the engines' `environment`.
- **Nodes** join both engines (`--join https://engine:7070,https://engine-standby:7070`) with the node token, over
  TLS with the certificate of `./tls` (below), and follow the active one across a failover. They run exec actions as
  static binaries only (see Laptop). For container actions, run nodes on hosts of their own with systemd.
- **TLS.** Before the first `up`, put the engines' certificate in `deploy/compose/tls/` (git-ignored): `cert.pem` and
  `key.pem`, naming `engine`, `engine-standby`, `localhost` and `127.0.0.1`, and `ca.pem`, the authority that signed
  it, which the nodes trust (see Remote nodes). The API on the host's loopback is HTTPS too; point the reverse proxy at
  it with that CA.

## Remote nodes

A node is `millstream node --join <url>[,<url>…]` on any host, with the deployment's node token in
`MILLSTREAM_NODE_TOKEN` (`node.token` on the engines, which serve no node without it). It registers its id (kept in
`--state-dir`, `.millstream/node` by default, with its package cache), its labels (`--labels gpu=true,zone=eu`) and
what it can run: exec actions only with `--allow-exec` (trusted code, run with the node's privileges), container
actions when Docker or Podman is installed (`runtime` in an environment file beside it), agents when `agents` there
gives it a sandbox or allows them unsandboxed. Steps go only to the nodes matching their `placement.labels` and the
runtime their action needs; a step no node can take is listed as unschedulable on the Nodes screen and in
`/api/v1/nodes`, and raises an `unschedulable` insight.

- **Node TLS.** A node reaches an engine on another host over HTTPS only (HTTP/2), checking its certificate against
  the system's authorities and `--ca <pem>`; plain HTTP is accepted to a loopback engine. The engines serve HTTPS with
  `tls: { cert, key }`. A private authority for a deployment:

  ```sh
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 -subj /CN=millstream-ca \
    -keyout ca.key -out ca.pem
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=engine -keyout key.pem -out engine.csr
  openssl x509 -req -in engine.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -out cert.pem \
    -extfile <(printf 'subjectAltName=DNS:engine,DNS:engine-standby,DNS:localhost,IP:127.0.0.1')
  ```

- **Failover.** List every engine in `--join`: a node tries them in turn, and follows the address a standby names
  (an engine listening on `0.0.0.0` names none, so the list matters). It registers again with the new active engine
  and resends the results it was holding; fencing refuses those whose lease ended meanwhile.
- **Data.** Packages are fetched by hash into the node's cache; artifacts move through URLs the engine issues for
  the deliveries the node holds: presigned ones with `artifacts.s3`, the engine's own proxy with a local directory.
  Secrets reach the node inside its deliveries and stay in memory and in each delivery's working directory, created
  on a tmpfs when the host has one (`$XDG_RUNTIME_DIR`, `/dev/shm`; the Compose nodes' `/tmp`), else 0700 in the
  temporary directory, and removed when the process ends.
- **Operations.** `millstream nodes` lists the nodes the store records with their health; `millstream node drain`
  (or SIGTERM) on the node's host drains it before an upgrade; the Nodes screen drains or revokes a node, as
  `POST /api/v1/nodes/{id}/drain` and `/revoke` do. `millstream nodes revoke <id>` refuses an id for good and ends
  its leases at once. To rotate the node token, restart the engines with the new one: every node is refused and
  exits until it is restarted with it.

## Signing in and monitoring

With `auth.token` set, every API request carries the token: the CLI sends `MILLSTREAM_TOKEN` as a bearer token, and
the web UI asks for it once on a sign-in screen, then holds a session cookie (12 hours, `HttpOnly`,
`SameSite=Strict`, `Secure` behind HTTPS) until **Sign out** in its menu. Sessions live in the engine process's
memory: a restart, and so a token change, or a failover to the other engine signs the UI out; the standby asks for
the token too. Every decision made through the API is recorded under `auth.user` (`operator` by default): the
open-source edition is single-user.

Prometheus scrapes `/metrics` on the same listener, with the token as a bearer token (`authorization: { credentials:
… }` in its scrape configuration), or without it when `metrics: { public: true }`. It serves the metric rollups and
the engine's own gauges: leases held per node, the writer's batch sizes, failed review notifications and each eval
case's latest score (spec section 10.2).

## Applying packages

A server runs the packages applied to its store, never a directory. From a checkout of the packages, on the engine
host or through an SSH tunnel to its loopback port:

```sh
export MILLSTREAM_SERVER=http://127.0.0.1:7070 MILLSTREAM_TOKEN=…   # the token from .env or millstream.env
millstream apply ./property-sourcing ./deal-flow --message "lower the score threshold"
millstream rollback property-sourcing          # the revision before the active one, or name one
millstream disable deal-flow                   # until: millstream resume deal-flow
```

`apply` checks each package and every other pipeline of the deployment before it uploads anything; the server keeps
each package by content hash and writes it under `packages/`, beside the local artifact store, when a delivery first
needs it. On Postgres, a command that changes state goes through `--server` (the active engine is the only writer)
unless `--direct` takes the leadership lock itself while no engine runs (with one running, it fails at once, naming
the active engine); `status`, `tail`, `inbox list`, `metrics`
and the other commands that only read open the database read-only, beside the running engine, with the same
environment file.

## Webhook triggers

A pipeline's `webhook` trigger is served at `POST /hooks/<pipeline>/<trigger>` on the same listener as the API (spec
section 4.6). `apply` prints each new trigger's token once, with its path; the store keeps only a hash, so copy it
into the sender's configuration then, or replace it later:

```sh
millstream apply ./property-sourcing
# property-sourcing revision 3 4f1c…
# property-sourcing webhook listing: POST /hooks/property-sourcing/listing with Authorization: Bearer mswh_… (shown once: keep it now)
millstream trigger token property-sourcing listing --rotate      # the old token stops working at once
```

The pipeline screen of the UI shows each webhook trigger's URL, with **Rotate token**. The sender posts JSON (at
most 1 MiB, matching the trigger's `inputSchema`) with the token as a bearer token, or as `?token=` when it cannot
set a header, and may name its delivery in `Idempotency-Key` so that a retry fires nothing new. Each trigger takes
at most its `webhook: { rate: … }` (60 a minute by default); beyond it the engine answers `429` with
`Retry-After`.

Behind the reverse proxy, route `/hooks/` to the engines like the rest: the endpoint is outside the API's Host and
Origin checks and needs neither `auth.token` nor a session, since the trigger's token is its authentication. To expose
only the webhooks to the internet while the UI and the API stay on a private network, give the proxy a public site
that forwards `/hooks/` alone, for example with Caddy:

```text
hooks.example.com {
    handle /hooks/* {
        reverse_proxy 127.0.0.1:7070
    }
    respond 404
}
```

Keep the proxy's access log free of query strings if senders use `?token=`. With a standby, a webhook reaching it is
answered `503` with the active engine's address, like any action: route `/hooks/` as the rest of the API (see
Standby and failover).

## VPS with systemd

On the engine host, Postgres 17 running locally or reachable:

```sh
install -m 0755 millstream /usr/local/bin/millstream
install -d -m 0755 /etc/millstream
install -m 0644 millstream.yaml /etc/millstream/millstream.yaml
install -m 0600 deploy/systemd/millstream.env.example /etc/millstream/millstream.env   # then set the values
install -m 0644 deploy/systemd/millstream.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now millstream
```

A standby is a second host with the same files and the same DSN. A node host installs the binary,
`deploy/systemd/node.env.example` as `/etc/millstream/node.env` and `millstream-node.service`.

- **Hardening.** Both units run under a transient user (`DynamicUser`), write only to their state directory
  (`/var/lib/millstream`, `/var/lib/millstream-node`), have no capabilities, and filter system calls to
  `@system-service`. `make test-deploy` runs `systemd-analyze verify` on them and holds their
  `systemd-analyze security` exposure at 2.0 or under (1.5 and 1.7 today).
- **Exec actions** run with the node's privileges, as its children: trusted code only (spec section 15.3). The node
  unit passes `--allow-exec`; remove it on a node that should only run containers.
- **Docker access** is opt-in, because the socket is root by another name: `systemctl edit millstream-node` and add
  `SupplementaryGroups=docker` under `[Service]`. The engine unit's embedded node needs the same for container
  actions and sandboxed agents.

## Standby and failover

A second `millstream serve` on the same Postgres database, with the same environment file, is the hot standby
(spec section 5.9). Both engines run the same command; whichever holds the database's leadership lock is active.

- **While it waits**, the standby logs `waiting as the standby` and serves the UI and the read endpoints of the API
  from a read-only connection. Every action it answers `503 unavailable`, with the active engine's id and API address
  in `X-Millstream-Active-Engine` and `X-Millstream-Active-Address`. The Nodes screen and `/api/v1/nodes` show both
  engines, the active one with its leadership epoch.
- **Taking over.** When the active engine's process dies, its lock is released at once; when its host or network
  dies, within `engine.lockCheck` + 2 × `engine.keepalive` (15 seconds with the default 5 s each). The standby then
  logs `leading the deployment` with the next epoch, rebuilds its state from the store and dispatches, the dead
  engine's leases being retried once they expire (`--lease-ttl`, 60 s by default). Nothing is applied twice.
- **Stepping down.** An engine that loses its write connection (a database failover, a network cut, a session ended
  by an administrator) logs `stepping down`, stops writing at once, abandons its embedded node's deliveries, and
  stands by again, retrying the database every `engine.lockCheck` while it is unreachable. Only SIGTERM or SIGINT
  stops it; a stopped standby exits 0.
- **Tuning.** `engine: { lockCheck: 5s, keepalive: 5s }` in the environment file. Shorter values fail over sooner and
  ping the database more often. A connection pooler between the engines and Postgres must not be in transaction mode:
  the lock lives on a session.
- **Clients.** Point the reverse proxy at both engines and fail over on `503`, or follow the header; the UI reloads
  its reads through whichever engine answers.

## Security

- **API token.** The API listens on loopback unless `listen` says otherwise, and refuses a non-loopback address
  without `auth.token`. Expose it through a reverse proxy that terminates TLS (Caddy, nginx), with `ui.baseUrl` set
  to its https address: the API answers requests naming that host or a loopback one, takes state-changing requests
  from that origin, and marks the session cookie `Secure`. Or let the engine serve HTTPS itself with
  `tls: { cert, key }` (PEM files, relative to the environment file). Never expose it as plain HTTP. Tokens are
  compared in constant time and never logged. `auth.token` and `node.token` hold at least 32 characters: the
  engines and nodes refuse to start with a shorter one, and `millstream check` reports it. Generate each with
  `openssl rand -hex 32`.
- **Webhook tokens.** Each webhook trigger has its own token, stored as a hash and compared in constant time; it
  admits that trigger's requests only, never the API. Rotate it with `millstream trigger token --rotate` or the UI
  when it may have leaked.
- **Node token and TLS.** Nodes present the deployment's node token, over TLS except on loopback (spec section 15.7).
  A node can fetch packages and artifacts only for the deliveries it holds. `millstream nodes revoke <id>` refuses
  a node; rotating the token disconnects every node until it has the new one (see Remote nodes).
- **Secrets** live in `.env` or `/etc/millstream/*.env`, mode 0600, never in `millstream.yaml`, an image or Git.
- **Supply chain.** Base images are pinned by digest (`make lint-pins` checks every `FROM`), the release build runs
  `go mod verify`, each archive has an SBOM, and checksums and images are signed with cosign (spec section 15.5).

## Retention and purge

Nothing expires unless told to (spec section 9.3). Set a stream's retention in its pipeline
(`streams: { listings.raw: { retention: 90d } }`, and on an imported stream's declaration for its copies), and the
deployment's in `millstream.yaml`:

```yaml
retention: { events: 90d, transcripts: 30d, rollups: 400d, every: 1h }
```

The active engine's retention job runs every `every` (an hour by default; the standby runs none): expired payloads
become tombstones (the UI shows the item as expired, its lineage intact), old events (once the metric rollup has read
them), transcripts and rollups are deleted in batches of 500, the artifact files nothing references any more are
deleted, in the local directory or the S3 bucket alike (a file stored again within the last day waits for the next
pass), and the package cache (`packages/` beside the artifacts) loses the revisions no pipeline runs any more. An item
that unfinished work needs (a delivery in flight or waiting, on the embedded node or a remote one, a parked review, an
open dead letter, an open review sample) keeps its payload until that work is
resolved: replay or discard dead letters to let retention take them. The engine logs what each pass removed
(`retention` lines).

Erase a person's data (a GDPR request) by the key their items carry, or by an item:

```sh
millstream purge --key 'https://www.example.com/listing/123' --request-id gdpr-2026-17
millstream purge --item it_7f3a… --pipeline property-sourcing
```

Through `MILLSTREAM_SERVER` the served engine runs it; on SQLite it runs from the command's process (on Postgres
with `--direct` only while no engine runs). The purge fences the items, their descendants and their copies in
importing pipelines (no new work on them from then on), erases payloads, keys, transcripts and the artifact files
nothing else references, and prints how many items it erased. It survives a crash: the engine finishes an unfinished
purge when it starts and at every retention pass, and repeating the command with the same `--request-id` finds it.
`GET /api/v1/purges/purge_<request id>` shows its progress. The purge records neither the key nor the item: keep the
request id with the erasure request instead. A purge does not reach backups taken before it, nor the logs the host
keeps (spec section 15.6): expire those on the same schedule.

## Backups

Everything durable is in Postgres, plus the artifact store. Back up with `pg_dump` in custom format:

```sh
docker compose exec -T postgres pg_dump -U millstream -Fc millstream > millstream-$(date +%F).dump
```

Restore into an empty database with the engines stopped:

```sh
docker compose stop engine engine-standby node-1 node-2
docker compose exec -T postgres pg_restore -U millstream -d millstream --clean --if-exists < millstream-2026-10-16.dump
docker compose start engine engine-standby node-1 node-2
```

On systemd, run the same `pg_dump` against the DSN from a timer. Back up an S3 artifact store with the provider's
versioning or replication; a local artifact directory is the engines' volume (Compose) or `/var/lib/millstream`.

## Upgrades

Before the first public release the store's schema changes freely between versions, with no migration (no
compatibility shims before launch): take a dump, then start the new version on a fresh database when the release
notes say so. From then on:

1. Dump the database.
2. Upgrade the standby first (`MILLSTREAM_IMAGE` in `.env` and `docker compose up -d engine-standby`, or the binary
   and `systemctl restart millstream` on its host), then the active engine: the standby takes over while it
   restarts.
3. Upgrade the nodes; each drains on SIGTERM and rejoins.

Pin images by digest in `.env` (`ghcr.io/monolithiclab/millstream@sha256:…`), so an upgrade is a reviewed change.
