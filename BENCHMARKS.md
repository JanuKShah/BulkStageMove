# Benchmarks

50,000 opportunities, one job, `npm run bench`. No test asserts these numbers;
correctness at scale is asserted in `test/happyflow/bulk-happy-flow.spec.ts`.

Measured on Windows 11, 12 logical cores, 15.7 GB, Node v24.21.0, PostgreSQL
16.15, RabbitMQ 4.3.6 — one Compose project, all services sharing one Postgres.
Not a portable SLA.

## The job

The last column is the shipped design: the same twelve consumers, but the walk that
turns a filter into batches runs in `SnapshotBuilder` after the response is sent.
It is a separate column rather than a replacement for the third because **the two
do the same work under different conditions**, and putting them in one column
invites a comparison the numbers cannot support. In the third, submission blocks
until all 50 batches exist and the workers are idle until then. In the fourth the
relay publishes each page the moment the builder writes it, so the drain overlaps
the build and the first batch runs about a second before the walk finishes.

| 50,000 records | item table, 4 consumers | batches, 4 consumers | batches, 12 consumers | **+ scheduler, 12 consumers** |
|---|---|---|---|---|
| **Submission (returns job id)** | 8.40 s | 0.56 – 2.30 s | 0.56 – 2.30 s | **9.2 ms** |
| Snapshot build (before any batch exists) | inline in submit | inline in submit | inline in submit | **2,366 ms** |
| Drain — snapshot built to settled | 0.52 s | 5.18 s | **1.83 s** | overlaps the build |
| **Whole job — submit to settled** | **8.92 s** | **7.47 s** | **2.39 s** | **2.62 s** |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** | **19,083/sec** |
| | | | | |
| Batches | 50,000 rows | 50 | **50** | **50** |
| Work per batch, mean | — | — | — | **529 ms** |
| Work, all 50 batches | — | — | — | **26,460 ms** |
| Pool wait per batch, mean | — | — | — | **7.3 ms** |
| Relay lag, mean | — | — | — | **195 ms** |
| Queue wait, mean | — | — | — | **65 ms** |
| Batches per consumer slot | — | — | — | 5,4,4,4,5,4,5,4,3,4,4,4 |

**The whole job is 10% slower and submission is 250× faster.** That is the trade the
scheduler makes, stated plainly rather than netted out: the walk still costs 2,366
ms, it just no longer happens while somebody is waiting on an HTTP response.

**"Drain — snapshot built to settled" has no value in the last column** because it
cannot be isolated: batches are consumed while the walk is still running. The
drain's own length is the 26,460 ms of work divided across twelve slots, 2,205 ms,
which is within 7% of the build's 2,366 ms — the two are the same length, and
overlapping them is why the whole job is 2.62 s rather than their sum.

A dash means the instrumentation that measures it did not exist for that
configuration, not that the figure is zero. The phase rows come from
`bulk_job_outbox`'s four timestamps plus the worker's own in-process timing;
`bulk_job_item` is deleted, so the item-table column's per-batch cost can never be
recovered.

The 4× spread in the submission cells is this host, not the code: six runs at 4
consumers gave 0.58, 1.59, 1.75, 2.16, 2.21 and 2.30 s.

## Where the time goes

The scheduler column above, in full. One run, from the four timestamps on each
batch row plus the worker's own in-process timing.

| phase | min | mean | max | total |
|---|---|---|---|
| **Whole job — submit to settled** | | | | **2,615 ms** |
| Sweep wait → first page | | | | 50 ms |
| Build, per page (1 thread) | 23.6 ms | 47.3 ms | 133.0 ms | **2,366 ms** |
| Relay lag — written to published | 68 ms | 195 ms | 322 ms | 9,738 ms |
| Queue wait — in RabbitMQ | −18 ms | 65 ms | 213 ms | 3,264 ms |
| Work — claim to settled | 293.8 ms | 529.2 ms | 1,000.1 ms | **26,460 ms** |
| Pool wait | 0.1 ms | 7.3 ms | 57.8 ms | 363 ms |
| Settle tail | | | | 0 ms |

The build is the critical path: 2,366 ms of a 2,615 ms job, on one thread, while
the drain runs twelve-wide. Build and drain are within 7% of each other (2,366 ms
against 2,205 ms per slot), so build time maps nearly 1:1 onto total time. The
drain is not what makes the job slow, and neither is submission.

Pages within a job cannot be parallelised — page N+1's keyset cursor is page N's
last row. Different jobs can be, which is what the build's advisory claim is for.

**Work cannot be read from the database.** `now()` in Postgres is the transaction
*start* time, so `completed_at − started_at` is the gap between two `BEGIN`s —
17 ms against a real 529 ms apply. The apply happens inside the work transaction,
after `now()` is captured. It appears only in the worker's log, which is why the
`work=` field on each batch line is load-bearing.

**One run.** Every figure above is a single sample, on the same host whose
submission figures spread 4× across six runs. The 2,366 ms build could plausibly
be 1,400 or 3,500. Treat it as a shape, not a result.

**The per-page mean is inflated by contention.** Measured against a quiet table a
page costs ~7 ms (1.1–3.6 ms read, 3.3 ms write). The 47.3 ms above is what it
costs while twelve workers share the same buffers and WAL. The two have not been
separated.

**Queue wait can read negative** — a worker claimed a batch before the relay's
`markPublished` committed. Correct, and harmless, but `published_at` records that
publication happened, not an ordering guarantee.

## Why 12 consumers

Consumer count was measured, not guessed. The per-batch time is the evidence: 0.41 s
at 4 consumers, 0.44 s at 12. Flat, while throughput rises 2.8×. Linear scaling —
the batches do not get slower, more run at once. Each does 1,000 opportunity
updates and 1,000 transition inserts, so it is compute-bound, not queue-bound, and
matching consumers to cores is the lever.

Corroborated by the run above: pool wait is 7.3 ms mean against 529 ms of work,
`queued` is 0 on every batch, and all twelve slots were busy with a near-even
spread (5,4,4,4,5,4,5,4,3,4,4,4 batches per slot for 50 batches). Twelve
consumers are not waiting on connections or on each other.

One queue, not one per worker: consumers on a single queue already share work
round-robin, so extra queues only add a second thing to keep balanced.

`RABBITMQ_CONSUMER_CONCURRENCY` is a tuning value, not a constant, and would need
re-measuring on other hardware.

## What the job writes

The item table wrote 50,000 rows per job, four indexes and three composite foreign
keys each. Measured inside a chunk, that insert was **81%** of the time to the
`201`. It now writes 50 batch rows holding the same ids as arrays — 16 kB per
1,000-record batch, 16 bytes per uuid with no per-row overhead.

There is **no per-record latency distribution** to report, and there never was: a
batch applies in one transaction, so its 1,000 transitions share one `created_at`.
The benchmark counts the distinct timestamps directly — 50 for 50,000 records.
Any percentile over the expanded set is the batch distribution restated.

## The drain is still 3.5× slower than before

Removing 50,000 row inserts should have made the drain faster. At 4 consumers it
went 0.52 s → 5.18 s. At the shipped 12 it is 1.83 s, so most of the gap is
concurrency — but that residue appeared at *constant* consumer count, so it is not
a parallelism problem.

**One candidate is now eliminated.** Connection-pool starvation was the strongest
hypothesis, since consumers and `PG_POOL_MAX` are both 12. Measured: 7.3 ms mean
pool wait, 39.9 ms worst, `waitingCount` 0 throughout — 1.4% of work time. The
pool is not the constraint.

The residue is therefore inside the 529 ms of Postgres work per batch. Three extra
round trips per batch and contention on the shared `bulk_job` row remain
candidates; neither is measured. **Still unidentified**, but no longer a
saturation problem.

## Beyond the brief

At 500,000 the job still completes cleanly — 500,000 moved, 0 failed, 57.34 s
end-to-end.

**Submission used to break here, and no longer does.** It was 0.56 s at 50,000
and 32.06 s at 500,000 — sixteen times the time for ten times the data — because
`submit()` accumulated every batch's ids in memory before writing any, roughly
50–100 MB of uuid strings at 500,000, and it would not hold at 5,000,000. The walk
now runs in `SnapshotBuilder` and streams: each page is written with the cursor
that follows it, so the response returns in single-digit milliseconds at any size
and memory is bounded by one page.

That fix is unmeasured at 500,000. The 57.34 s above predates it.

## Tests

| suite | files | tests | time |
|---|---|---|---|
| `safety` | 8 | 88 | 6.0 s |
| `endpoints` | 7 | 87 | 4.2 s |
| `happyflow` | 1 | 10 | 36.1 s |
| **all three** | **16** | **185** | passing |

Run individually, so the times are not three projects sharing one process. The
suite is 185 because the async snapshot needed tests the old shape had no use for:
the scheduler, resume from a partial cursor, and the per-job build claim.

## Known limits

- The 3.5× drain residue is unexplained, though pool starvation is ruled out.
- The build is single-threaded per job and is 90% of the job's wall clock. Pages
  cannot be parallelised within a job because each page's keyset cursor is the
  previous page's last row.
- Concurrency is tuned to this host's 12 cores.
- Every figure in "Where the time goes" is one run, and the build's per-page cost
  has not been separated from contention.
- An earlier version of this file claimed a chunk-size win of 1.21× that was really
  1.04×. The false figure is in the message of pushed commit `8f2052b`.
- The two harnesses that measured the old shape were deleted with it rather than
  repaired; they would have produced numbers about a design that no longer exists.
