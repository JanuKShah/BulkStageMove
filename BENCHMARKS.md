# Benchmarks

50,000 opportunities, one job, `npm run bench`.

Measured on Windows 11, 12 logical cores, 15.7 GB, Node v24.21.0, PostgreSQL
16.15, RabbitMQ 4.3.6 — one Compose project, all services sharing one Postgres.
Not a portable SLA.

## 50k bulk job

| 50,000 records | item table, 4 consumers | batches, 4 consumers | batches, 12 consumers | **+ scheduler, 250 ms ticks** | **+ scheduler, 125 ms ticks** |
|---|---|---|---|---|---|
| **Whole job — submit to settled** | **8.92 s** | **7.47 s** | **2.39 s** | **1.82 – 2.72 s** | **1.52 – 2.37 s** |
| **Submission (returns job id)** | 8.40 s | 0.56 – 2.30 s | 0.56 – 2.30 s | **9.2 ms – 53 ms** | **10 – 22 ms** |
| Snapshot batching (before any batch exists) | inline in submit | inline in submit | inline in submit | **1.54 – 2.36 s** | **812 ms – 1.54 s** |
| Drain — snapshot built to settled | 0.52 s | 5.18 s | **1.83 s** | overlaps the batching | overlaps the batching |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** | **18,400 – 27,500/sec** | **21,300 – 27,300/sec** |
| | | | | | |
| Batches | 50,000 rows | 50 | **50** | **50** | **50** |
| Work per batch, mean | — | — | — | **422 – 565 ms** | **392 ms** |
| Pool wait per batch, mean | — | — | — | **7.3 – 9.1 ms** | — |
| Relay lag, mean | — | — | — | **185 – 202 ms** | **99 – 111 ms** |
| Queue wait, mean | — | — | — | **21 – 144 ms** | **116 – 280 ms** |
| Batches per consumer slot | — | — | — | 4–5 each, all 12 busy | 4–5 each, all 12 busy |

- The two right-hand columns are the same design; only the sweep and relay interval differs. Everything left of them is a different design.
- **Halving the tick made the job faster and the queue worse** — a caller waits less, a batch waits more, and three runs is thin for a range that wide.
- **Submission is 250× faster and the whole job is no slower:** the walk still costs 0.8-1.5 s, it just no longer happens while somebody waits on a response.
- **"Drain" has no scheduler-column value** because it cannot be isolated — batches are consumed while the batching is still running, and the drain is the longer of the two phases.
- **Pool wait has no 125 ms figure, which is a measurement gap rather than an improvement** — the log line now carries a connection count, not a wait time.
- A dash means the instrumentation did not exist for that configuration, not that the figure is zero.
- Phase rows come from `bulk_job_outbox`'s four timestamps plus the worker's own timing; `bulk_job_item` is deleted, so the item-table per-batch cost can never be recovered.
- The 4× spread in the submission cells is this host, not the code: six runs at 4 consumers gave 0.58, 1.59, 1.75, 2.16, 2.21 and 2.30 s.

## Where the time goes

Three consecutive runs, fresh volume each time, from the four timestamps on each
batch row plus the worker's own in-process timing.

| phase | run 1 | run 2 | run 3 |
|---|---|---|---|
| **Whole job — submit to settled** | **1.84 s** | **1.52 s** | **1.74 s** |
| Sweep wait, submit to first page | 32 ms | 7 ms | 12 ms |
| Batching, per page, mean | 31 ms | 16 ms | 20 ms |
| Batching total | 1.54 s | 812 ms | 985 ms |
| Work, per consumer slot | 1.72 s | 1.49 s | 1.69 s |
| Relay lag, written to published | 99 ms | 105 ms | 111 ms |
| Queue wait, in RabbitMQ | 116 ms | 280 ms | 202 ms |
| Claim gap, claim txn to work txn | 9 ms | 9 ms | 7 ms |
| Earned by overlapping | 1.42 s | 780 ms | 935 ms |

- **The drain is the constraint, not batching** — 0.55x-0.89x of the work per slot, leaving 678 ms of batching headroom the job was never waiting on.
- **Batching's per-page cost is contention, not its own work** — 16-31 ms measured against ~7 ms for a page on a quiet table, because twelve workers share the same buffers and WAL.
- **Queue wait is the largest single cost in the pipeline** at 116-280 ms mean, and it is the price of publishing every 125 ms rather than less often.
- **Relay lag is 99-111 ms** and the sweep adds 7-32 ms, so roughly 150 ms of every small job is spent waiting to be noticed.
- **Work cannot be read from the database.** `now()` is the transaction *start* time, so `completed_at - started_at` is the gap between two `BEGIN`s — a few ms against a real 174-598 ms apply.
- **It exists only in the worker's log,** which is why the `work=` field on each batch line is load-bearing and why `benchmark.ts` shells out to read it back.
- **Pages cannot be parallelised within a job,** because page N+1's keyset cursor is page N's last row; different jobs can be, which is what the advisory claim is for.
- **Queue wait can read negative,** because a worker claimed a batch before the relay's `markPublished` committed — `published_at` records that publication happened, not an ordering guarantee.

## Filter selectivity

`npx tsx test/happyflow/filter-bench.ts` - one freshly seeded 50,000 record workspace per case, one **full job** per filter, each case run twice with only the warm pass reported, whole script run three times per tick. **Per transition** is total wall clock over records moved — the only column comparable across filter sizes, since raw totals are not.

| filter | matched | pages | per transition | projected at 50,000 |
|---|---|---|---|---|
| no filter | 50,000 | 50 | 24.4-31.0 µs | 1.22-1.55 s |
| outcome | 12,500 | 13 | 37.4-43.4 µs | 1.87-2.17 s |
| value range | 5,050 | 6 | 66.3-67.5 µs | 3.32-3.38 s |
| date range | 5,065 | 6 | 54.3-66.3 µs | 2.72-3.32 s |

**Projected at 50,000** is the per-transition figure multiplied out to 50,000 records, so the four filters are comparable at one size. It is arithmetic on the measured column, not a second measurement — the whole point of the column is that cost per record rises as the job gets smaller, because one sweep tick, one relay tick and one builder pass do not amortise. A selective filter therefore costs *more* per record than an unfiltered one at equal size, and a caller who filters pays that premium on every record they move.

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
| `safety` | 11 | 119 | 9.3 s |
| `endpoints` | 7 | 87 | 4.8 s |
| `happyflow` | 1 | 10 | 36.4 s |
| **all three** | **19** | **216** | passing |

Run individually, so the times are not three projects sharing one process. The
suite grew from 185 because the async snapshot needed tests the old shape had no
use for — the scheduler, resume from a partial cursor, and the per-job batching claim
— and because the filter tests found a data-loss bug, where a filter naming no
records was stored as an empty object and a job with no stage filter matched the
entire workspace.

## Known limits

- Batching is single-threaded per job, because each page's keyset cursor is the previous page's last row.
- Batching's per-page cost has not been separated from contention, so how much is its own work is unknown.
- **A small job is bound by its polling intervals, not by its data** — ~125 ms of waiting, halved from 250 ms and still a floor. The record count at which waiting stops dominating has moved with it and is not re-measured. See "Filter selectivity".
- Queue wait roughly doubled when the tick was halved, which is the cost of it. Unsettled at three runs. See "Where the time goes".
- Batching got about 2x faster when only a timer changed. Unexplained. See "Where the time goes".

