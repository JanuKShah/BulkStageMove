# Benchmarks

Measured on this machine, not estimated. Reproduce the 50,000-record run with
`npm run bench`.

No test asserts any number here. A latency assertion fails on a loaded CI box and
passes on a laptop. What *is* asserted is that 50,000 records move correctly.

## Environment

| | |
|---|---|
| Host | Windows 11, 12 logical cores, 15.7 GB |
| Node | v24.21.0 |
| PostgreSQL | 16.15 |
| RabbitMQ | 4.3.6 |
| Fixture | 50,000 opportunities, one workspace |
| Topology | one Docker Compose project, all services sharing one Postgres |

Single machine, single Postgres, no replicas, no separate broker. Not a portable
SLA, and it says nothing about concurrent load.

`docker compose down -v` then `up --build --wait` from nothing — build, migrations
on an empty volume, all health checks — takes about 24 s.

## The three numbers

A bulk job has three phases, and they are worth separating because only two of them
are a background job.

| phase | what is happening | who is waiting |
|---|---|---|
| **submission** | `POST /bulk-moves` resolves the filter and writes the batches | **the caller, synchronously** |
| **drain** | batches travel through RabbitMQ and the worker applies them | nobody |
| **end-to-end** | submission + drain | the caller |

Submission is the only part a user perceives as latency. Everything after the 201
is the feature working as intended.

## 50,000 opportunities, one job

The current run, 4 consumers, on a volume created by `docker compose down -v`:

| | |
|---|---|
| Outcome | 50,000 moved, 0 failed |
| Batches | 50 × 1,000 |
| **Submission** (returns job id) | **2.30 s** |
| **Drain** (submit → settled) | **5.18 s** |
| **End-to-end** | **7.47 s** |
| Drain rate | 9,656/sec |
| End-to-end rate | 6,693/sec |

Per-batch completion, submit → that batch's 1,000 records all moved. 50 samples,
one per batch:

| min | p50 | p95 | p99 | max |
|---|---|---|---|---|
| 2.78 s | 4.56 s | 6.44 s | 6.82 s | 6.82 s |

The worker claim uses the partial index from migration 0005, at 0.10 ms:

```
Index Scan using bulk_job_outbox_claimable_idx on bulk_job_outbox
  Index Cond: (job_id = … AND batch_no = 0)
  Filter: (status = ANY ('{pending,running}') AND workspace_id = …)
Execution Time: 0.101 ms
```

## Before and after removing the item table

The 50,000-record job used to write one row per record into `bulk_job_item` — 50,000
rows, four indexes and three composite foreign keys each. It now writes 50 batch
rows holding the same ids as arrays.

| | before | after | |
|---|---|---|---|
| Submission | 8.40 s | **0.58 – 2.30 s** | 4–14× |
| Drain | 0.52 s | 3.88 – 5.73 s | **7–11× slower** |
| End-to-end | 8.92 s | **5.49 – 7.94 s** | ~1.1–1.6× |

Read the ranges, not the endpoints. Six runs of the new code gave submissions of
0.58, 1.59, 1.75, 2.16, 2.21 and 2.30 s, and drains of 3.90, 3.88, 4.95, 5.73 and
5.18 s. The drain looked monotonic for four runs and then fell, so it is variance on
a shared machine rather than degradation. The "before" column is a **single run**,
so that comparison is indicative rather than rigorous — it is the weakest number
here, and the end-to-end gain in particular is the least certain thing on this page.

## The drain got slower, and I do not know why

This is stated plainly because the alternative is presenting a 7× regression as if
it were part of a win.

The worker used to update 1,000 `bulk_job_item` rows per batch and now updates
none, so removing that work should have made the drain faster. It did not.

Two candidate causes, neither measured:

1. **Three extra round trips per batch.** Reading the batch's ids, reading the
   target stage and the filter's stage list, and writing the batch's settled state
   are each a separate statement — 150 extra round trips across 50 batches.
2. **Contention on the `bulk_job` row.** Every batch updates it, and those
   statements serialise on it.

A third was ruled out by measurement: `UNIQUE (job_id, opportunity_id)` on
`opportunity_transition`, with `ON CONFLICT DO NOTHING`, cost one index probe per
row — 50,000 per job, confirmed by `idx_scan` on that index. Removing it did not
recover the time, so it was not the cause. It was removed anyway, because it cannot
fire: a batch is one transaction, so a retry re-inserts into a table with no trace
of the failed attempt, and the worker separately skips records already at the
target. See the note in `migrations/0001_schema.sql`.

## There is no per-record latency distribution

An earlier version of this file reported p50/p95/p99 over 50,000 per-opportunity
samples. **Those were not 50,000 samples.** A batch applies in one transaction, so
all 1,000 of its transitions share a single `created_at`. The benchmark now counts
the distinct timestamps directly, and for a 50,000-record job:

```
distinct timestamps                   50
```

Fifty distinct values across fifty thousand rows, each repeated a thousand times.
Percentiles over that set are the batch distribution above, restated — which is why
the two sections used to print identical numbers.

The version before that reported `0ms` for every intra-batch percentile, because it
measured max-minus-min *within* a batch, and that is zero by construction. Both
were artefacts of the measurement, not of the system.

## Where submission time went

Measured inside a chunk, on the query shape the old code used. Reproduce with
`npm run bench`, which still builds and submits the same 50,000 records — the
harnesses that produced this table were deleted with the design they measured, so
the numbers below are a record rather than a rerunnable script.

| part | median | total | share |
|---|---|---|---|
| `SELECT` 1,000 (keyset page) | 2.2 ms | 125 ms | 2% |
| `INSERT` 1,000 items | 98.5 ms | 5,046 ms | **81%** |
| outbox row + counter | 1.5 ms | 87 ms | 1% |
| `COMMIT` (fsync) | 19.0 ms | 999 ms | 16% |

That is why the read was not worth caching: the whole read was 2% of the cost, and
Redis would have had to be populated first. The 81% was the item insert, which is
what the batch-row change removed.

## Chunk size, measured

One statement per chunk, sweeping 50,000 records:

| chunk | transactions | total | speedup |
|---|---|---|---|
| 1,000 | 50 | 4,191 ms | 1.00× |
| 2,000 | 25 | 4,033 ms | 1.04× |
| 5,000 | 10 | 3,625 ms | 1.16× |
| 10,000 | 5 | 3,473 ms | 1.21× |
| 25,000 | 2 | 3,337 ms | 1.26× |
| 50,000 | 1 | 3,013 ms | 1.39× |

**This is what the old shape cost, and the table is the only reason to keep the
chunking.** 1.39× for an unbounded transaction is a poor trade, because a
transaction sized by the match count is how one filter over a large workspace
becomes one enormous transaction.

An earlier version of this file claimed "widening the chunk from 1,000 to 2,000 is
1.21×". **That is wrong** — 2,000 is 1.04×, and 1.21× belongs to 10,000. The false
figure is also in the message of commit `8f2052b`, which is pushed.

## Test suite timings

| suite | tests | time | status |
|---|---|---|---|
| `safety` | 61 | ~14 s | passing |
| `endpoints` | 86 | ~14 s | passing |
| `happyflow` | 10 | 11 s | passing |
| **all three** | **157** | ~25 s | passing |

`happyflow` runs a real 50,000-record job through the broker and four consumers.
It was 589 s before this change and is 11 s now, because it no longer builds
50,000 item rows to assert against.

No test asserts a number in this file. Correctness at scale is asserted in
`test/happyflow/bulk-happy-flow.spec.ts`.

## Known limits

- **The drain regressed 7×** and the cause is not identified. See above.
- **These are single runs on one machine.** Only the chunk-size table and the
  submission split were repeated; the headline figures were not.
- **A record that leaves the filtered stage mid-job is skipped, not overwritten.**
  The job moves what matches when the worker reaches it. `total_matched` means
  "matched at submission" and `processed_count` can land below it; the worker
  reports the difference as `skipped` so it does not read as lost work.
- **Submission is still synchronous** and grows with the match count. It is now
  0.6–2.2 s rather than 8.4 s, but it is the only part the caller waits for.
- **Two harnesses were deleted, not repaired.** `chunk-size.ts` and
  `submit-cost.ts` inserted into the item table this change removed. Fixing them
  would have produced numbers about a design that no longer exists. Their findings
  are recorded above; the scripts are not coming back.
