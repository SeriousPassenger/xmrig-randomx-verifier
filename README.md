# xmrig-randomx-verifier

A local, fast/full-memory RandomX verification sidecar for the
`improvised-daemon-mining` branch of xmrig-proxy. It computes the hash of the
exact nonce-filled Monero hashing blob and compares the raw 32 bytes with the
miner's claimed result. It deliberately does **not** calculate share
difficulty or decide whether a share meets a target; xmrig-proxy owns those
decisions using the computed hash.

The native engine is pinned to
`nssy/node-randomx-hashing@e5bc20530e9aaac30f524fd0dd28ff4072ba745d`.
Only its asynchronous raw `hashAsync` path is used. Its `verifyShare` and
difficulty implementation are never called.

## Safety properties

- Unix `SOCK_STREAM` only; there is no TCP listener.
- Socket mode is exactly `0660`, intended for a private shared group.
- At most two simultaneous clients.
- 4-byte big-endian frame length followed by UTF-8 JSON.
- Maximum JSON body is 16,384 bytes.
- Strict per-operation fields and exact hexadecimal validation.
- Fast/full-memory mode only, with no silent light-mode fallback.
- A seed must complete `prepare_seed` before `verify`; a missing seed is never
  lazily initialized by a verification request.
- Three explicit seed slots, intended for previous/current/next seed hashes.
  A fourth seed is rejected until one is released, avoiding the native pool's
  implicit LRU behavior and temporary fourth-dataset allocation.
- Different seed preparations are serialized. Hashing can continue on a
  ready seed while the next seed is prepared asynchronously.
- A global FIFO queue holds at most 256 waiting verifications. Active native
  hashes never exceed `VM_POOL_SIZE`.
- `release_seed` retires the seed immediately, rejects new work for it, waits
  for already-accepted work, and only then frees native state.
- Bounded input/output buffering, read pausing on backpressure, write-stall
  timeout, handshake timeout, incomplete-frame timeout, duplicate request-ID
  rejection, and graceful SIGTERM/SIGINT draining.
- Existing symlinks and non-socket filesystem entries are never replaced.
  Stale sockets are removed only after an ownership, liveness, and inode
  recheck. The server binds a private name, atomically hard-links its socket
  inode to the public path, and removes the private name; therefore Node/libuv
  never owns the public pathname and cannot blindly unlink a replacement on
  close. Shutdown unlinks only the public inode created by this process.

Filesystem permissions are the authentication boundary. Put the socket in a
dedicated directory and add only the verifier and xmrig-proxy service users to
its group.

Native RandomX work already executing inside the addon cannot be cancelled
safely. `SHUTDOWN_TIMEOUT_MS` bounds socket-output draining, while systemd's
`TimeoutStopSec=60s` is the final bound for a genuinely hung native operation.
Normal queued hashes and seed preparation are drained before native state is
released.

## Build and test

Use Node.js 22 or newer. Ubuntu's distribution package can be older, so check
the installed major version instead of assuming `apt install nodejs` is
sufficient. Building the pinned native dependency requires a C++
toolchain, CMake, Python, Git, and approximately 2.5 GiB of free RAM per live
fast-mode seed dataset.

```bash
sudo apt update
sudo apt install -y build-essential cmake git python3

node --version
npm --version
node -e 'const major=Number(process.versions.node.split(".")[0]); if (major < 22) { throw new Error("Node.js 22 or newer is required"); }'

cd xmrig-randomx-verifier
npm ci
npm test
```

Unit tests use a mock engine and therefore do not allocate a RandomX dataset.
The Unix-socket integration tests run automatically on a normal Linux host.
They are reported as skipped only in sandboxes that explicitly deny
`AF_UNIX` bind.

On the bare-metal deployment host, `npm test` should report all 16 tests
passing and zero skipped. Any `AF_UNIX listen is blocked` skip there indicates
an unexpected host sandbox or mandatory-access-control restriction that must
be resolved before integration.

Run the optional real-engine known-answer test after installation:

```bash
UV_THREADPOOL_SIZE=4 VM_POOL_SIZE=4 INIT_THREADS=4 npm run smoke
```

This builds one fast dataset, so expect roughly 2.4 GiB RSS and several
seconds of initialization on the E3-1240 v5.

## Configuration

Environment variables are read only at startup:

| Variable | Default | Meaning |
|---|---:|---|
| `VERIFIER_SOCKET_PATH` | `/run/xmrig-randomx-verifier/verifier.sock` | Absolute Unix-socket path |
| `UV_THREADPOOL_SIZE` | Node default (`4`) | libuv native worker count; set before Node starts |
| `VM_POOL_SIZE` | `4` | Native RandomX VMs and maximum active hashes |
| `INIT_THREADS` | `4` | Dataset initialization threads |
| `ENABLE_HUGE_PAGES` | `false` | Request explicit huge pages; not needed for correctness |
| `VERIFY_QUEUE_LIMIT` | `256` | Maximum waiting hashes, excluding active hashes |
| `MAX_PENDING_PER_CLIENT` | queue + VMs + 16 | Input/read backpressure threshold |
| `MAX_BLOB_BYTES` | `4096` | Maximum decoded hashing-blob size |
| `HANDSHAKE_TIMEOUT_MS` | `5000` | Initial hello and partial-frame deadline |
| `WRITE_STALL_TIMEOUT_MS` | `10000` | Disconnect a client that does not read responses |
| `SHUTDOWN_TIMEOUT_MS` | `30000` | Socket-output drain deadline on shutdown |
| `READY_TIMEOUT_MS` | `15000` | systemd post-start hello deadline |

The tested E3-1240 v5 result with `VM_POOL_SIZE=4`, `INIT_THREADS=4`, and
`UV_THREADPOOL_SIZE=4` was approximately 1,140 hashes/second at p99 below
5 ms. All values remain configurable for other hosts.

## Protocol

Every request has protocol version `1`, a unique in-flight request ID, and an
operation. IDs may be nonnegative safe integers or 1-128 character tokens.
Responses can arrive out of order.

Frame layout:

```text
uint32_be json_length
json_length bytes of UTF-8 JSON
```

The first request on each connection must be a valid `hello`:

```json
{"v":1,"id":1,"op":"hello","client":"xmrig-proxy"}
```

```json
{"v":1,"id":1,"ok":true,"service":"xmrig-randomx-verifier","mode":"fast","allow_light_fallback":false,"max_frame":16384,"capabilities":["prepare_seed","release_seed","verify","ping","stats"],"vm_pool_size":4}
```

Prepare a seed. Repeating an already-ready seed is idempotent. `mode` and
`allow_light_fallback` are optional, but when present must have these exact
values:

```json
{"v":1,"id":2,"op":"prepare_seed","seed_hash":"46d6338d15886ba5000000000000000000000000000000000000000000000000","mode":"fast","allow_light_fallback":false}
```

```json
{"v":1,"id":2,"ok":true,"seed_hash":"46d6338d15886ba5000000000000000000000000000000000000000000000000","state":"ready","already_ready":false,"prepare_ms":6981.332}
```

Verify an exact nonce-filled hashing blob:

```json
{"v":1,"id":3,"op":"verify","seed_hash":"46d6338d15886ba5000000000000000000000000000000000000000000000000","blob":"1010abcdef","claimed_hash":"0000000000000000000000000000000000000000000000000000000000000000","job_id":"job-42","nonce":"01020304","share_id":"share-99"}
```

```json
{"v":1,"id":3,"ok":true,"hash":"d0402d6834e26fb94a9ce38c6424d27d2069896a9b8b1ce685d79936bca6e0a8","match":false,"queue_ms":0.031,"hash_ms":3.447,"total_ms":3.502}
```

`match:false` is a successful cryptographic verification, not a transport
error. The proxy must reject the claimed share and must calculate target and
difficulty from `hash`, never from `claimed_hash`.

Release a seed after all proxy jobs using it have expired:

```json
{"v":1,"id":4,"op":"release_seed","seed_hash":"46d6338d15886ba5000000000000000000000000000000000000000000000000"}
```

Health operations:

```json
{"v":1,"id":5,"op":"ping"}
{"v":1,"id":6,"op":"stats"}
```

Errors retain the request ID and are machine-readable:

```json
{"v":1,"id":7,"ok":false,"error":"seed has not been prepared","code":"SEED_NOT_READY","retryable":false}
```

Hexadecimal input is case-insensitive and is normalized to lowercase. Hashes
are compared as raw bytes without byte-order conversion.

## Seed lifecycle

For Monero templates, xmrig-proxy should:

1. `prepare_seed` for the current `seed_hash` before issuing work.
2. Prewarm a nonzero `next_seed_hash` as soon as monerod advertises it.
3. Keep the previous seed while jobs using it remain valid.
4. At an epoch transition, use the already-ready next seed immediately.
5. Call `release_seed` only after the old job TTL and outstanding submissions
   have drained.

If an unexpected seed is not ready, pause/fail closed while preparing it. Do
not fall back to light mode.

## systemd

The `deploy` directory contains a hardened unit and environment example. The
example assumes:

- code at `/opt/xmrig-randomx-verifier`;
- a dedicated unprivileged user named `xmrig-verifier`;
- a private group named `xmrig` shared with the xmrig-proxy service user.

Create the account and group first. Set `PROXY_SERVICE_USER` to the actual
`User=` running xmrig-proxy; `monero` below is only an example:

```bash
PROXY_SERVICE_USER=monero

getent group xmrig >/dev/null || sudo groupadd --system xmrig
id xmrig-verifier >/dev/null 2>&1 || sudo useradd \
  --system \
  --gid xmrig \
  --home-dir /var/lib/xmrig-randomx-verifier \
  --create-home \
  --shell /usr/sbin/nologin \
  xmrig-verifier
sudo usermod -aG xmrig "$PROXY_SERVICE_USER"
```

Install the checkout and compile the pinned native dependency as the service
user. These commands assume they are run from this repository root:

```bash
sudo install -d -o xmrig-verifier -g xmrig -m 0750 /opt/xmrig-randomx-verifier
sudo cp -a package.json package-lock.json README.md LICENSE src examples /opt/xmrig-randomx-verifier/
sudo chown -R xmrig-verifier:xmrig /opt/xmrig-randomx-verifier
sudo -u xmrig-verifier npm --prefix /opt/xmrig-randomx-verifier ci --omit=dev
sudo -u xmrig-verifier npm --prefix /opt/xmrig-randomx-verifier run smoke
sudo chown -R root:xmrig /opt/xmrig-randomx-verifier
sudo chmod -R go-w /opt/xmrig-randomx-verifier
```

The final ownership change makes the installed service code read-only to the
runtime user while retaining group-readable/traversable modes.

Confirm `command -v node` is `/usr/bin/node`, or update `ExecStart` in the unit
to the absolute Node.js 22+ path installed on this host.

```bash
sudo install -m 0644 deploy/xmrig-randomx-verifier.service /etc/systemd/system/
sudo install -m 0640 deploy/xmrig-randomx-verifier.env.example /etc/default/xmrig-randomx-verifier
sudo systemctl daemon-reload
sudo systemctl enable --now xmrig-randomx-verifier.service
sudo systemctl status xmrig-randomx-verifier.service
```

Restart xmrig-proxy after changing its supplementary groups so it receives
membership in `xmrig`.

The unit intentionally sets `MemoryDenyWriteExecute=no`: RandomX JIT needs
executable mappings. It otherwise has no capabilities, no network namespace,
read-only system paths, and access only to its systemd runtime directory.
Its `ExecStartPost` performs a framed `hello` exchange, so dependent services
are not started merely because the Node process exists; the socket protocol
must actually be ready.

This ordering covers initial startup only. The xmrig-proxy integration must
still fail closed, reconnect with bounded backoff after a later verifier
restart, repeat `hello`, and re-prepare every seed still referenced by live
jobs before accepting additional miner submissions.

Inspect the local socket and live health after startup:

```bash
stat -c '%F %U:%G %a %n' /run/xmrig-randomx-verifier/verifier.sock
journalctl -u xmrig-randomx-verifier.service -f
```
