# Benchmarks

Measured on this machine, not estimated. Reproduce the 50,000-record run with
`npm run bench`.

No test asserts any number here. A latency assertion fails on a loaded CI box and
passes on a laptop. Correctness at scale is asserted in
`test/happyflow/bulk-happy-flow.spec.ts`.

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
SLA and says nothing about concurrent load.

`docker compose down -v` then `up --build --wait` from nothing — build, migrations
on an empty volume, all health checks — takes **27.9 s**.

## 50,000 opportunities, one job

One run, 4 consumers, on a volume created fresh by `docker compose down -v`.

**Submission** is the HTTP request: the client stays blocked while the server resolves
the filter and writes the snapshot. **Drain** is everything after the response, until
the records are actually moved. **End-to-end** is the sum of the two, and is what a
user actually waits for.

```text
  click
    │
    │◄── submission: 8.40s ────────────►│◄── drain: 0.52s ─────────────►│
    │                                   │  50 batches travel through    │
    │  server resolves the filter,      │                               │
    │  writes 50,000 snapshot rows      │  RabbitMQ, 4 consumers        │
    │                                   │  move the records             │
    ▼                                   ▼                               ▼
  201 { jobId }                       all 50,000          job "completed"
                                         moved

    │◄── end-to-end: 8.92s ────────────────────────────────────────────►│
```

The split is at the HTTP response. Before it, the work is synchronous and the caller
waits; after it, the work is asynchronous and the caller could close the tab. Today
94% of the wait is on the wrong side of that line.

| | |
|---|---|
| Outcome | 50,000 moved, 0 failed |
| Batches | 50 × 1,000 |
| `POST /bulk-moves` | 8.40 s |
| Drain, submit → settled | **0.52 s** |
| **End-to-end** | **8.92 s** |
| Drain rate | 95,602/sec |
| End-to-end rate | 5,605/sec |

Per-opportunity latency, submit → moved (50,000 samples):

| min | p50 | p95 | p99 | mean |
|---|---|---|---|---|
| 0.49 s | 4.38 s | 8.14 s | 8.66 s | 4.51 s |

**Two rate figures, deliberately.** Drain rate counts only the work after the POST
returns, so it ignores the 8.40 s the caller actually spent waiting and flatters the
result by roughly 16×. End-to-end rate includes submission. Only the second is a
user-facing number.

**This is a floor, not a typical result.** It was measured on an empty
`bulk_job_item` with a provably empty queue, so all 4 consumers belonged to this job
alone. The same code against a warm database carrying ~107,000 rows left by earlier
suites drained in 4.43 s, for 10.01 s end-to-end. Nothing about that run was slower
code — only a bigger table and a busier queue.

**Why 4 consumers.** With one consumer and prefetch 1, each batch waits for the one
before it, so completion timestamps measure queue depth rather than work. An earlier
run showed batches 0–6 finishing ~8 s apart and then 43 batches landing in 7.4 s,
which looked like a broker fault and was not one. Four separate channels remove the
serialisation; amqplib dispatches one channel's messages a callback at a time, so
four consumers on a single channel would still have been serial.

Per-batch *duration* is always 0 ms: all 1,000 items in a batch share one
`completed_at` because they commit together, so there is no intra-batch timing to
measure. Percentiles over 50 batch samples are not a tail — treat p99 as the worst
batch seen.

Worker claim plan, 1 batch, `EXPLAIN (ANALYZE)`:

```
Index Scan using bulk_job_item_claimable_idx  (actual time=0.023..0.023 rows=0)
  Index Cond: (job_id = … AND batch_no = 0)
  Filter: (status = ANY ('{pending,running}') AND workspace_id = …)
Execution Time: 0.094 ms
```

## Where submission time goes

`POST /bulk-moves` resolves the filter and writes the snapshot. It never holds more
than one chunk of rows in memory, and the worker already consumes 1,000 per
message, so the shape is right. The cost is the number of transactions.

**All figures below were measured with the 4-consumer worker running**, so they
include contention from a live worker, and the run that produced them died on a
cleanup deadlock partway through — only the first two chunk sizes completed.

| shape | transactions | total | worst chunk |
|---|---|---|---|
| **A.** `OFFSET` paging, 1,000 per chunk | 50 | 7,547 ms | — |
| **B.** keyset paging, 1,000 per chunk — **what production does** | 51 | **4,535 ms** | 151 ms |
| **C.** keyset paging, 2,000 per chunk | 26 | **3,748 ms** | 182 ms |

51 and 26 rather than 50 and 25: each run ends with one extra probe that returns no
rows, which is how the loop knows it has reached the end.

**A previous version of this file claimed collapsing the loop into one statement was
1.7× faster. That was wrong, and it was wrong in a way that pointed the wrong
way.** Shape A was measured with `OFFSET` paging, which production does not use —
at offset 49,000 Postgres reads and discards 49,000 rows to return 1,000, so A
measures deep-offset scanning rather than the production path. The real production
shape is B at 4,535 ms, and the single-statement variant measured 4,432 ms. That is
**1.02×, not 1.7×** — there is essentially nothing to win from removing the commits
once the paging is keyset rather than offset. The 1.7× was an artifact of the
strawman, not headroom in the real code.

**What does help is a larger bounded chunk: C at 2,000 rows is 1.21× over
production**, keeps every transaction bounded, and keeps `batch_no = (rn - 1) / 1000`
identical because 2,000 is a whole number of batches. Larger chunk sizes were not
measured — the run stopped at 2,000 — so the knee is unknown, not absent.

The remaining ~3.7 s is Postgres writing 50,000 rows. That floor is not removable
by restructuring the application, because those rows have to exist for the snapshot
to be a snapshot. The only way below it is to stop doing the work in the request —
return the job id first and resolve the filter in the worker — which gives up
atomicity between the job row and its items and needs a resume path. Deliberately
not done.

Reproduce with `npx tsx test/happyflow/submit-cost.ts` and
`npx tsx test/happyflow/chunk-size.ts`.

## Test suite timings

| suite | tests | runs | median | min | max | status |
|---|---|---|---|---|---|---|
| `safety` | 50 | 3 | **6.1 s** | 6.1 s | 6.3 s | passing |
| `endpoints` | 87 | 3 | **7.2 s** | 7.1 s | 7.5 s | passing |
| `happyflow` | 9 | 1 | 685 s | — | — | **5 failing** |

`npm test` (all three) is ~11.5 min, essentially all of it happyflow. The two
suites a developer waits on in the inner loop are ~13 s combined.

Within happyflow, the shared-timestamp regression alone is **24 s**; the 50,000
suite is the remaining ~11 min. 20 runs of happyflow is not feasible at that cost,
which is why the timing harness defaults to 3.

## Known limits

- **Submission is synchronous** and scales with match count. It is 5.6–8.4 s of an
  8.9 s job, so ~94% of what a caller waits for. Everything else is now fast.
- **The snapshot is only approximately frozen.** The filter is re-evaluated once
  per chunk across the length of the sweep, so an opportunity created mid-sweep can
  be included or missed depending on where the cursor is. The doc comment claims
  the set is fixed at submission; the code approximates that. A `created_at <=
  $snapshot` watermark bound once from the application would make it true.
- **The 1,000-row snapshot transaction is a bound, not a choice.** Collapsing to one
  unbounded statement is faster but sizes the transaction by match count, so a
  filter over a large workspace becomes one enormous transaction. The chunked shape
  keeps the bound.
- **The drain numbers assume an idle database.** Concurrent submissions contend on
  `bulk_job_item` and on the `bulk_job` counters, which every batch updates.

## Change log

| change | end-to-end | drain | submission | note |
|---|---|---|---|---|
| 4 consumer channels | 8.92–10.01 s | 0.52–4.43 s | 5.58–8.40 s | range spans fresh and warm volumes |
| Redis | *pending* | | | not started |
