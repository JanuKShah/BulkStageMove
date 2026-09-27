# Benchmarks

50,000 opportunities, one job, `npm run bench`.

Measured on Windows 11, 12 logical cores, 15.7 GB, Node v24.21.0, PostgreSQL
16.15, RabbitMQ 4.3.6 — one Compose project, all services sharing one Postgres.

## 50k bulk job

**A tick is one firing of the two background timers** — the builder sweep and the
outbox relay, both on this interval. Only one of them gates a fresh job: submit
kicks the sweep itself, fire-and-forget, so batching starts in **10 ms** rather than
waiting out a tick. The sweep timer is the fallback that finishes a job whose walk
did not complete. The relay is never kicked, so written batches wait up to one tick
to be published — **208 ms mean**, and the only poll wait in a job. The worker has no
timer; it consumes messages as they arrive.

```
  POST /bulk-moves ──▶  one job row written as 'preparing', 201 + jobId (~13 ms)
                              │
                              │  submit kicks the sweep itself — no timer, 10 ms
                              ▼
                        batches written, 1,000 ids each
                              │
                              │  wait up to one RELAY tick — 208 ms mean
                              ▼
                        published to RabbitMQ
                              │
                              │  no timer — consumed as it arrives
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
| **Whole job — submit to settled** | **8.92 s** | **7.47 s** | **2.39 s** | **1.54 s** | **1.40 s** |
| **Submission (returns job id)** | 8.40 s | 0.56 – 2.30 s | 0.56 – 2.30 s | **15 ms** | **13 ms** |
| Snapshot batching (before any batch exists) | inline in submit | inline in submit | inline in submit | **942 ms** | **886 ms** |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** | **27,248/sec** | **29,878/sec** |
| | | | | | |
| Batches | 50,000 rows | 50 | **50** | **50** | **50** |
| Work per batch, mean | — | — | — | **422 – 565 ms** | **392 ms** |
| Relay lag, mean | — | — | — | **259 ms** | **208 ms** |
| Queue wait, mean | — | — | — | **195 ms** | **177 ms** |
| Batches per consumer slot | — | — | — | 4–5 each, all 12 busy | 4–5 each, all 12 busy |

- **Halving the tick made the job 9% faster and the queue slightly better** — relay lag fell 20%, throughput rose 10%, and queue wait fell rather than rose, contradicting the earlier 3-run reading that said it doubled.
- **Submission is 250× faster and the whole job is faster still:** the walk still costs 0.9-1.0 s, it just no longer happens while somebody waits on a response.
- **Batching and the drain cannot be timed separately under the scheduler,** because batches are consumed while the batching is still running; the drain is the longer of the two phases, and overlapping them is why the whole job is 1.40-1.54 s rather than their 2.15-2.29 s sum.

## Where the time goes

Mean of five runs, fresh volume each time, from the four timestamps on each batch row plus the worker's own in-process timing.

| phase | mean of 5 |
|---|---|
| **Whole job — submit to settled** | **1.40 s** |
| Sweep wait, submit to first page | 10 ms |
| Batching, per page, mean | 18 ms |
| Batching total | 886 ms |
| Work, per consumer slot | 1.27 s |
| Batching / work-per-slot | 0.70x |
| Relay lag, written to published | 208 ms |
| Queue wait, in RabbitMQ | 177 ms |
| Claim gap, claim txn to work txn | 9 ms |
| Batching + work, if sequential | 2.15 s |
| Earned by overlapping | 750 ms |

- **The drain is the constraint, not batching** — 0.70x of the work per slot, so a third of the batching is headroom the job was never waiting on.
- **Batching's per-page cost is contention, not its own work** — 18 ms measured against ~7 ms for a page on a quiet table, because twelve workers share the same buffers and WAL.
- **Queue wait is the largest single cost in the pipeline** at 177 ms mean, and halving the tick barely moved it (195 ms before) — the streams of 85-321 ms across five runs overlap, so the tick is not what sets it.
- **Relay lag averages 208 ms** and the sweep adds 10 ms, so a small job spends roughly 220 ms waiting to be noticed.
- **Work cannot be read from the database.** `now()` is the transaction *start* time, so `completed_at - started_at` is the gap between two `BEGIN`s — a few ms against a real 174-598 ms apply.
- **It exists only in the worker's log,** which is why the `work=` field on each batch line is load-bearing and why `benchmark.ts` shells out to read it back.
- **Pages cannot be parallelised within a job,** because page N+1's keyset cursor is page N's last row; different jobs can be, which is what the advisory claim is for.
- **Queue wait can read negative,** because a worker claimed a batch before the relay's `markPublished` committed — `published_at` records that publication happened, not an ordering guarantee.

## Filter selectivity

`npx tsx test/happyflow/filter-bench.ts` - one freshly seeded 50,000 record workspace per case, one **full job** per filter, each case run twice with only the warm pass reported, run at the 125 ms tick. **Per transition** is total wall clock over records moved — the only column comparable across filter sizes, since raw totals are not.

| filter | matched | pages | per transition | projected at 50,000 |
|---|---|---|---|---|
| no filter | 50,000 | 50 | 24.4-31.0 µs | 1.22-1.55 s |
| outcome | 12,500 | 13 | 37.4-43.4 µs | 1.87-2.17 s |
| value range | 5,050 | 6 | 66.3-67.5 µs | 3.32-3.38 s |
| date range | 5,065 | 6 | 54.3-66.3 µs | 2.72-3.32 s |

**Projected at 50,000** is the per-transition figure multiplied out to 50,000 records, so the four filters are comparable at one size. It is arithmetic on the measured column, not a second measurement — the whole point of the column is that cost per record rises as the job gets smaller, because one sweep tick, one relay tick and one builder pass do not amortise. A selective filter therefore costs *more* per record than an unfiltered one at equal size, and a caller who filters pays that premium on every record they move.

## Beyond the brief

At 500,000 the job still completes cleanly — 500,000 moved, 0 failed, 58.71 s
end-to-end.

## A single 50,000-record job

`npm run bench`, fresh volume, one run. The 50k table above is a mean of five; this
is the single run that accompanied the two measurements below, so the conditions
match each other and not the table.

| 50,000 records | |
|---|---|
| **Processed / failed** | **50,000 / 0** |
| **End-to-end from submit** | **2.12 s** |
| **Sustained throughput** | **23,878 records/sec** |
| Submission, returns job id | 23 ms |
| Batches | 50 |
| Per-batch completion p50 / p95 / p99 | 906 ms / 1.61 s / 1.71 s |
| Per-batch min / max | 244 ms / 1.71 s |

## Interactive reads while a bulk job runs

`npm run bench:interactive` — `GET /opportunities?limit=20`, sampled every 200 ms
against two workspaces, so the cost of a bulk job can be separated from the cost of
being the tenant running it. 15 s quiet baseline, then a 500,000-record job, sampled
until it settled in **52.68 s**.

| 500,000-record job running | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| same workspace — quiet | 70 | 7.1 | 11.3 | 15.6 | 15.6 |
| **same workspace** — during | 246 | 5.7 | 11.8 | **30.9** | 56.5 |
| different workspace — quiet | 70 | 7.1 | 10.7 | 19.0 | 19.0 |
| **different workspace** — during | 246 | 5.5 | 10.6 | **13.4** | 16.0 |

- **p95 barely moves: 1.04x in the job's own workspace, 0.98x in a different one.**
  A half-million-record job does not make ordinary reads slower.
- **The cost is all in the p99 tail, and only in the tenant running the job** —
  15.6 → 30.9 ms, while another tenant's p99 went 19.0 → 13.4. That is the
  measurable form of the isolation claim in `DESIGN.md` § 5, which otherwise only
  argues it structurally.
- **p50 got *faster* during the run** (7.1 → 5.7 ms), which is not the job helping:
  the 15 s baseline pays cold-start cost. It makes the quiet row a pessimistic
  reference, so the p99 result is if anything conservative.
- **The quiet p99 is not a real p99.** Over 70 samples, nearest-rank p99 is
  effectively the maximum, so that row is really "max of 70" against a 246-sample
  p99. The p50 and p95 comparisons are the sounder ones; treat the 2x tail as
  indicative. A longer baseline (`BASELINE_MS`) fixes it.

Run at 500,000 rather than 50,000 deliberately: a 50k job settles in ~1.5 s, which at
one sample per 200 ms is about seven samples, and a p99 over seven samples is not a
percentile. The harness warns when it collects fewer than 30.

## Kill the builder mid-walk

`npm run bench:killresume` — `SIGKILL` to `transition-service` once the walk has
written 10 of its 50 batches, so the cursor is genuinely mid-flight and a graceful
shutdown gets no chance to tidy up. The container is then started again, which is
what an orchestrator would do. This is the measurement behind the `DESIGN.md` § 1
claim that a half-built job is finished rather than restarted.

| 50,000 records, killed after batch 10 | |
|---|---|
| **Kill to settled** | **5.03 s** |
| kill to service answering again | 3.47 s |
| service answering to settled | 1.56 s |
| Processed / failed | **50,000 / 0** |

**Is the result correct?** All six checks pass, and the harness exits non-zero if any
fails — a benchmark that reports a wrong result and exits 0 is worse than none.

| check | result |
|---|---|
| every record in the target stage | 50,000 of 50,000 |
| exactly one transition per record | 50,000 transitions |
| no record moved twice | 50,000 distinct opportunities |
| **no batch re-walked after the kill** | 50 rows, 50 distinct `batch_no`, 0..49 |
| the filter margin was left alone | 50,000 moved + 2,000 excluded |
| no failed records | 0 |

The fourth row is the one that matters. If the cursor were not committed in the same
transaction as the batch it follows, the resumed walk would re-emit batches 0-9 and
`count(DISTINCT batch_no)` would come back under 50. It is exactly 50, contiguous
from 0, so the walk resumed from the cursor and re-walked nothing. The third row is
the same claim from the other side: 50,000 transitions across 50,000 *distinct*
opportunities, so nothing was applied twice.

**The 3.47 s is Docker, not the application.** It is container start plus health
check, and it dominates the 5.03 s total — the actual resumed work took 1.56 s.
