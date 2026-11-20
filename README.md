# Courier Queue

Courier is a local job queue with a SQLite database shared across worker processes. It supports idempotent enqueue, atomic claim, expiring worker leases, retry backoff and dead-letter states. A sample worker computes word count, byte length and SHA-256 for text payloads.

## Run locally

Use Node.js 24 or newer.

```sh
node cli.mjs enqueue batch-01 "the quick brown fox"
node cli.mjs work --once
node cli.mjs status batch-01
node cli.mjs metrics
node --test queue.test.mjs
```

Run `node cli.mjs work` in multiple terminals for long-running workers. Set `COURIER_DB` to share a database file other than `courier.sqlite`. Running the same enqueue command returns the original job ID. Reusing its key for changed inputs fails.

A worker receives a unique lease token when it claims a job. Once the lease expires, another worker can reclaim it, and the earlier token can no longer write a result. Explicit failures retry with exponential backoff and stop at the attempt limit. Delivery is at least once, so handlers with external side effects need their own idempotency.

## Docker

```sh
docker build -t courier-queue .
docker volume create courier-data
docker run --rm -v courier-data:/data courier-queue enqueue sample "container payload"
docker run --rm -v courier-data:/data courier-queue work --once
docker run --rm -v courier-data:/data courier-queue status sample
```

SQLite coordinates workers sharing one local filesystem. Queue state counts are available through the `metrics` command.
