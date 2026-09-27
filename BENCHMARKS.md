# Benchmarks

50,000 opportunities, one job, `npm run bench`.

Measured on Windows 11, 12 logical cores, 15.7 GB, Node v24.21.0, PostgreSQL
16.15, RabbitMQ 4.3.6, one Compose project, all services sharing one Postgres.

## 50k bulk job

Two background timers run every 125 ms:

- the builder sweep, which writes a job's batches
- the outbox relay, which publishes finished batches

Either firing is "a tick".

Submitting calls the sweep directly and does not wait for it, so batching starts
about 10 ms later and a new job never waits for that timer.

The sweep's timer is the fallback. It finishes a job whose walk stopped partway,
resuming from its stored cursor.

The relay is never called directly. A written batch waits for its next tick to be
published, the only point in a job where progress waits on a timer.

The worker has no timer. It consumes messages as they arrive.

```
  POST /bulk-moves ──▶  one job row written as 'preparing', 201 + jobId (~13 ms)
                              │
                              │  submit kicks the sweep itself, no timer, 10 ms
                              ▼
                        batches written, 1,000 ids each
                              │
                              │  wait up to one RELAY tick, 208 ms mean
                              ▼
                        published to RabbitMQ
                              │
                              │  no timer, consumed as it arrives
                              ▼
                        12 consumers apply batches
                              │
                              ▼
  GET /bulk-moves/:id ──▶  progress, batches, failures, dead letters
```

One of the three phases is poll-driven, and it is the only place a fresh job waits.
The drain is not.

| 50,000 records | item table, 4 consumers | batches, 4 consumers | batches, 12 consumers | **+ scheduler, 250 ms ticks** | **+ scheduler, 125 ms ticks** |
|---|---|---|---|---|---|
| **Whole job, submit to settled** | **8.92 s** | **7.47 s** | **2.39 s** | **1.54 s** | **1.40 s** |
| **Submission (returns job id)** | 8.40 s | 0.56 – 2.30 s | 0.56 – 2.30 s | **15 ms** | **13 ms** |
| Snapshot batching (before any batch exists) | inline in submit | inline in submit | inline in submit | **942 ms** | **886 ms** |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** | **27,248/sec** | **29,878/sec** |
| | | | | | |
| Batches | 50,000 rows | 50 | **50** | **50** | **50** |
| Work per batch, mean | n/a | n/a | n/a | **422 – 565 ms** | **392 ms** |
| Relay lag, mean | n/a | n/a | n/a | **259 ms** | **208 ms** |
| Queue wait, mean | n/a | n/a | n/a | **195 ms** | **177 ms** |
| Batches per consumer slot | n/a | n/a | n/a | 4–5 each, all 12 busy | 4–5 each, all 12 busy |

- **Halving the tick made the job 9% faster and the queue slightly better**, relay lag fell 20%, throughput rose 10%, and queue wait fell rather than rose, contradicting the earlier 3-run reading that said it doubled.
- **Submission is 250× faster and the whole job is faster still:** the walk still costs 0.9-1.0 s, it just no longer happens while somebody waits on a response.
- **Batching and the drain cannot be timed separately under the scheduler,** because batches are consumed while the batching is still running; the drain is the longer of the two phases, and overlapping them is why the whole job is 1.40-1.54 s rather than their 2.15-2.29 s sum.

## Where the time goes

Mean of five runs, fresh volume each time, from the four timestamps on each batch row plus the worker's own in-process timing.

| phase | mean of 5 |
|---|---|
| **Whole job, submit to settled** | **1.40 s** |
| Sweep wait, submit to first page | 10 ms |
| Batching, per page, mean | 18 ms |
| Batching total | 886 ms |
| Work, per consumer slot | 1.27 s |
| Batching / work-per-slot | 0.70x |
| Relay lag, written to published | 208 ms |
| Queue wait, in RabbitMQ | 177 ms |
| Claim gap, claim txn to work txn | 9 ms |
| Batching + work, if sequential | 2.15 s |
| Earned by overlapping drain | 750 ms |

## Filter selectivity

`npx tsx test/happyflow/filter-bench.ts` - one freshly seeded 50,000 record workspace per case, one **full job** per filter, each case run twice with only the warm pass reported, run at the 125 ms tick. **Per transition** is total wall clock over records moved, the only column comparable across filter sizes, since raw totals are not.

| filter | matched | pages | per transition | projected at 50,000 |
|---|---|---|---|---|
| no filter | 50,000 | 50 | 24.4-31.0 µs | 1.22-1.55 s |
| outcome | 12,500 | 13 | 37.4-43.4 µs | 1.87-2.17 s |
| value range | 5,050 | 6 | 66.3-67.5 µs | 3.32-3.38 s |
| date range | 5,065 | 6 | 54.3-66.3 µs | 2.72-3.32 s |

**Projected at 50,000** is the per-transition figure multiplied out to 50,000 records, so the four filters are comparable at one size. It is arithmetic on the measured column, not a second measurement, the whole point of the column is that cost per record rises as the job gets smaller, because one sweep tick, one relay tick and one builder pass do not amortise. A selective filter therefore costs *more* per record than an unfiltered one at equal size, and a caller who filters pays that premium on every record they move.

## Beyond the brief

At 500,000 the job still completes cleanly, 500,000 moved, 0 failed, 58.71 s
end-to-end.

## Interactive reads while a bulk job runs

`npm run bench:interactive`, `GET /opportunities?limit=20`, sampled every 200 ms
against two workspaces, so the cost of a bulk job can be separated from the cost of
being the tenant running it. 15 s quiet baseline, then a 500,000-record job, sampled
until it settled in **52.68 s**.

| 500,000-record job running | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| same workspace, quiet | 70 | 7.1 | 11.3 | 15.6 | 15.6 |
| **same workspace**, during | 246 | 5.7 | 11.8 | **30.9** | 56.5 |
| different workspace, quiet | 70 | 7.1 | 10.7 | 19.0 | 19.0 |
| **different workspace**, during | 246 | 5.5 | 10.6 | **13.4** | 16.0 |

Run at 500,000 rather than 50,000 deliberately: a 50k job settles in ~1.5 s, which at
one sample per 200 ms is about seven samples, and a p99 over seven samples is not a
percentile. The harness warns when it collects fewer than 30.

## Kill the builder mid-walk

`npm run bench:killresume`, `SIGKILL` to `transition-service` once the walk has
written 10 of its 50 batches, so the cursor is genuinely mid-flight and a graceful
shutdown gets no chance to tidy up. The container is then started again, which is
what an orchestrator would do. This is the measurement behind the `DESIGN.md` section 1
claim that a half-built job is finished rather than restarted.

| 50,000 records, killed after batch 10 | |
|---|---|
| **Kill to settled** | **5.03 s** |
| kill to service answering again | 3.47 s |
| service answering to settled | 1.56 s |
| Processed / failed | **50,000 / 0** |

The fourth row is the one that matters. If the cursor were not committed in the same
transaction as the batch it follows, the resumed walk would re-emit batches 0-9 and
`count(DISTINCT batch_no)` would come back under 50. It is exactly 50, contiguous
from 0, so the walk resumed from the cursor and re-walked nothing. The third row is
the same claim from the other side: 50,000 transitions across 50,000 *distinct*
opportunities, so nothing was applied twice.

**The 3.47 s is Docker, not the application.** It is container start plus health
check, and it dominates the 5.03 s total, the actual resumed work took 1.56 s.
