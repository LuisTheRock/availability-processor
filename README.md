# Size-Level Availability Processor

A Go service that turns seller stock changes into availability updates:
validate → version check → product context → `PUT` availability → publish `SizeAvailabilityUpdated`.
Single events (`POST /_dev/messages`) and CSV rows (`POST /v1/availability/imports`) go through the **same** `availability.Processor.Process`.

## Run

```sh
docker compose up -d                 # from the repo root: WireMock :8080 + Redis :6379 (both bound to 127.0.0.1)
cd root && go run .                  # service on :5050 (see .env)
```

```sh
curl -X POST localhost:5050/_dev/messages -H 'Content-Type: application/json' \
  --data-binary @wiremock/__files/sample-event.json                     # from the repo root

curl -X POST localhost:5050/v1/availability/imports -H 'Content-Type: text/csv' \
  --data-binary @wiremock/__files/bulk-stock.csv                        # -> 202 {"jobId": "..."}
curl localhost:5050/v1/availability/imports/<jobId>?errorsLimit=100
```

Inspect what the service sent at `http://localhost:8080/__admin/requests`.
API docs (swagger UI): `http://localhost:5050/swagger/index.html`. Regenerate after changing handler annotations with `cd root && go run github.com/swaggo/swag/cmd/swag@v1.16.4 init -g main.go --parseInternal -o docs`.

Tests: `cd root && go vet ./... && go test -race ./...`.
The Redis-backed tests (CAS, lock, job store) are skipped unless Redis is reachable: `REDIS_ADDR=localhost:6379 go test -race ./...`.

## Configuration (`.env`)

`.env` is gitignored and optional: every variable has a default, so `go run .` works without it. To override, create `root/.env` (read from the directory you run `go run .` in) or set real environment variables, which take precedence over the file.

```sh
# root/.env  (values shown are the defaults)
SERVER_PORT=':5050'                          # listen address; change it if 5050 is taken
DEPENDENCIES_BASE_URL='http://localhost:8080' # WireMock: Product Context, Availability API, event sink
REDIS_ADDR='localhost:6379'                  # version store, locks, job status
```

If you change `SERVER_PORT`, use that port in the `curl` examples above.

## Layout

```
internal/upstream      HTTP client for Product Context, Availability API, event sink (+ retries)
internal/availability  the shared flow, version store (Redis), POST /_dev/messages
internal/imports       CSV parsing, job lifecycle, async worker, job status (memory + Redis)
```

## Assumptions and decisions

**Product context rules** (nothing is written or published in any of these):

| Situation | Event endpoint | Bulk row |
|---|---|---|
| Unknown product/size/country (404; absent from the snapshot) | 422 | failed `UNKNOWN_PRODUCT` |
| `productStatus != ACTIVE` | 422 | failed `INACTIVE_PRODUCT` |
| `countryEnabled == false` | 422 | failed `COUNTRY_NOT_ENABLED` |

Rationale: these are data problems the seller or catalog must fix, so they are reported, not retried and not silently dropped.

**Validation:** `sellerId/productId/sizeId` required, `countryCode` = two uppercase letters, `availableQuantity >= 0`, `sourceVersion >= 0`. The event endpoint also requires `eventType=SellerInventoryChanged`, `schemaVersion=1`. Invalid → 400 (event) or a failed row `INVALID`. On the event endpoint `availableQuantity` and `sourceVersion` must be present (a missing field is a 400, never a silent 0 that would mark the size sold out) and unknown fields are rejected, so a typo like `availableQuantty` is caught.

**Event endpoint results:** `200 {"status":"applied"}` or `200 {"status":"skipped","reason":"STALE|DUPLICATE"}`; 400 invalid; 422 context rules; 503 key busy (lock timeout); 503 + `Retry-After` a dependency is throttling (`RATE_LIMITED`); 502 a dependency still failing after retries or a failed publish.

## Versions, idempotency, stale rows

The stream is `(sellerId, productId, sizeId, countryCode)`; the last applied `sourceVersion` per stream is stored in Redis (`stockver:seller|product|size|country`), shared by events and CSV rows.

- `version < applied` → **skipped `STALE`**; `version == applied` → **skipped `DUPLICATE`** (a replay with a new `eventId` is the same business change). Neither calls the Availability API nor publishes.
- `version > applied` (or never seen) → apply.
- Each stream is processed under a **Redis lock** (`SET NX PX`, 60s lease, released by compare-and-delete). Check → PUT → record → publish runs inside it, so a bulk row and a live event for the same key do not interleave, even on different pods. This is what makes the stale rule hold while a bulk job is running. One row is cut off 5s before the lease would end (`context.WithTimeout`), so the lock cannot expire underneath a running row.
- The version is recorded with a Lua **compare-and-set** that refuses to go backwards. If a lease ever expires anyway (a long GC/pause), the recorded version is still monotonic, and a row whose `Record` is refused returns skipped `STALE` and does **not** publish. Its PUT may already have reached the gateway: that window is the remaining gap, closed in production by the outbox / one consumer per key.
- The output event reuses the inbound `eventId` (so a delivery can be traced end to end, and the provided sink's `fault-429` / `fault-503` inputs work). CSV rows have none, so they get `out-` + a hash of `key|version`.

## Retries and failures

`upstream.Client` retries **429 / 503 / network errors** up to 3 attempts. It waits `Retry-After` (capped at 10s) or a jittered 200–400ms, and stops when the context is cancelled. 404 and other 4xx are permanent (no retry). Timeouts are per attempt and cover reading the whole body: 5s for single calls, 60s for a country snapshot (50,000 objects), so a slow network does not fail a whole import. A timed-out attempt is retried like a network error.

**Exception: the event endpoint does not sleep through a 429.** Waiting out `Retry-After` there would hold the HTTP request (about 10s for two waits) and the per-stock lock. Instead a 429 from a dependency returns immediately as **`503` with a `Retry-After` header**, so the caller (or the broker's retry topic) decides when to retry. Bulk rows keep the in-process retry, since a background job can afford to wait. A 503 from a dependency is still retried in-process (about 0.5s total). **Publishing is never fail-fast:** once the write is recorded, a caller retry would be skipped as a duplicate and the event lost, so a 429 from the sink is retried in-process (honouring `Retry-After`) like in bulk.

**PUT succeeded, publish failed:** the stock really changed, so the version **is recorded** before publishing. The caller gets `PUBLISH_FAILED` (502 / failed row), and a replay of that version is a `DUPLICATE` no-op: the write is never applied twice, but the event is not re-sent either. This is a known gap: the production fix is a **transactional outbox** (write the pending event next to the version, a relay publishes and acknowledges it). Not built here.

**Redis down:** changes fail (502 / failed row) rather than being applied without a version check; startup fails fast on a failed ping.

## Bulk imports

`POST /v1/availability/imports` takes a raw `text/csv` body or a multipart `file`. A missing/wrong header, an empty file or an unreadable body is a synchronous **400**; a body over 10 MB (raw or multipart) is **413**. Otherwise **202 `{jobId}`** is returned as soon as the job is saved to Redis, and rows are processed in the background.

Per row, results are:

| Outcome | Counted as | Listed in `errors`? |
|---|---|---|
| Applied | `succeeded` | no |
| Stale or duplicate version | **`skipped`** (not a failure) | no, only counted |
| Malformed row, unknown/inactive/disabled product, dependency failure, publish failure | `failed` | yes: `{row, code, message}` |

`total = processed = succeeded + skipped + failed` once finished; `failed` is the true count, while the listed `errors` are a sample capped at the first 1000 per job (`?errorsLimit`, default 100, max 1000; `0` returns none). The cap also bounds what is stored in Redis. A stale row never fails the upload. States: `PENDING → RUNNING → COMPLETED | COMPLETED_WITH_ERRORS`, or `INTERRUPTED`.

**Status storage — memory + Redis.** The running job lives in memory (no Redis call per row). Its counters and *new* row errors are flushed to Redis every second and once at the end (then it leaves memory). `GET` reads memory first, then Redis, so any pod can answer. If a process dies mid-job, Redis keeps `RUNNING` with a stale `updatedAt`; after 30s a read reports `INTERRUPTED` with the last counters. **Recovery = resubmit the same CSV**: rows already applied come back `DUPLICATE`. Automatic resume is not built (it would require storing the uploaded file). Job keys expire after 7 days.

**After a hard crash (`kill -9`), wait ~60s before resubmitting.** The rows that were in flight (up to 8 per pod) still hold their per-stock Redis lock until the 60s lease expires, so resubmitting sooner makes those rows fail with `LOCK_TIMEOUT` after waiting 10s. They are not lost: resubmit again once the lease has expired. A graceful stop (SIGTERM) releases its locks and finishes the rows already started, so it needs no wait.

**Matching complexity.** One `GET /v1/product-contexts/snapshot` per distinct country (never one request per row). The `S` snapshot objects are indexed into a hash map keyed by `(productId, sizeId, countryCode)`: O(S) time and O(S) space. Each of the `R` rows then does an O(1) lookup: **O(S + R) time, O(S + R) space** overall (rows are held in memory too), versus O(S·R) for scanning the snapshot per row. If both grow: time stays linear; memory is dominated by the snapshot. At much larger scale, stream-decode the snapshot and keep only the keys that appear in the upload (O(R) space), or shard by country. 50,000 contexts and 10,000 rows is trivial for this approach.

## Concurrency and protecting dependencies

- Bulk rows share **one limit of 8 rows in flight per pod across all jobs** (a channel used as a semaphore), so several uploads at once do not multiply the load on the dependencies; retries are capped at 3 and honour `Retry-After`, so a throttling dependency slows the job instead of being hammered.
- Request bodies are limited (1 MB event, 10 MB CSV, for raw and multipart alike); HTTP server timeouts are set.
- With N pods the ceiling is N × 8 concurrent rows (the limit is per pod, not global); the next step is a shared rate limiter (Redis token bucket) sized to the gateway's published limits, and a circuit breaker per dependency.
- A burst of live events is bounded by the HTTP server and the per-key lock; in production the broker's consumer concurrency is the control.

## Production broker: Kafka

Not implemented (the exercise doesn't require a broker); the code is shaped so Kafka is just another adapter that calls `Processor.Process`, exactly like `/_dev/messages`.

```
seller systems -> topic seller-inventory-changed (key = seller|product|size|country)
   consumer group "availability-processor", pods share partitions
   -> Kafka adapter -> Processor.Process -> Product Context / Availability API
   -> topic size-availability-updated (same key, idempotent producer)
```

- **Key = stock stream** → all changes for a stream land on one partition, owned by one pod at a time: per-key ordering comes from Kafka. The Redis lock stays as the safety net for the bulk HTTP path and for rebalances.
- **Offsets** are committed after `Process` returns applied, skipped, or a permanent failure. Transient failures (429/503/lock timeout) are retried in-process, then sent to a delayed `.retry` topic and finally `.dlq`. Delivery is at-least-once; versions in Redis make redelivery a `DUPLICATE` no-op.
- **Output:** idempotent producer (`acks=all`), keyed by stream, the same `eventId`; the outbox above replaces the "record then report PUBLISH_FAILED" behaviour.
- **Bulk in production:** the pod that receives the CSV still builds the snapshot index (one fetch per country), then produces each enriched row onto the input topic (header `jobId`) so imports and live events are ordered by the same partitions; consumers `HINCRBY` the job counters in Redis. Trade-off: a large import can delay live events on shared partitions; a separate low-priority topic avoids that but then relies on the Redis lock for cross-topic ordering.
- **Scaling** = partitions (fix the count up front, e.g. 24–48; changing it remaps keys). Client: `franz-go`.

## Known limits

- Output event `occurredAt` is the processing time; `eventId`, not `occurredAt`, is the idempotency handle.
- Jobs resume only by resubmitting the CSV; flush granularity is 1s, so a crash can lose up to ~1s of counter progress (never stock updates: versions are recorded before the next row).
- `/health/readiness` checks Redis (503 when it is unreachable) but does not probe the Product Context, Availability or event-sink endpoints; those failures show up per request as 502/503.
