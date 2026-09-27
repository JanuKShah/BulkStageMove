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
| **Submission (returns job id)** | 8.40 s | 0.56 – 2.30 s | 0.56 – 2.30 s | **9.2 ms – 53 ms** |
| Snapshot build (before any batch exists) | inline in submit | inline in submit | inline in submit | **1.54 – 2.36 s** |
| Drain — snapshot built to settled | 0.52 s | 5.18 s | **1.83 s** | overlaps the build |
| **Whole job — submit to settled** | **8.92 s** | **7.47 s** | **2.39 s** | **1.82 – 2.72 s** |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** | **18,400 – 27,500/sec** |
| | | | | |
| Batches | 50,000 rows | 50 | **50** | **50** |
| Work per batch, mean | — | — | — | **422 – 565 ms** |
| Pool wait per batch, mean | — | — | — | **7.3 – 9.1 ms** |
| Relay lag, mean | — | — | — | **185 – 202 ms** |
| Queue wait, mean | — | — | — | **21 – 144 ms** |
| Batches per consumer slot | — | — | — | 4–5 each, all 12 busy |

Ranges are **three consecutive runs** on a fresh volume each time, not a single
sample. A single run of this job varies by 34% on work per batch and 7× on queue
wait with no configuration change, so a lone figure here would be misleading in a
way the earlier version of this file did not acknowledge.

**The whole job is 10% slower and submission is 250× faster.** That is the trade the
scheduler makes, stated plainly rather than netted out: the walk still costs 1.5-2.4 s,
it just no longer happens while somebody is waiting on an HTTP response.

**"Drain — snapshot built to settled" has no value in the last column** because it
cannot be isolated: batches are consumed while the walk is still running. The
drain's own length is the work total divided across twelve slots, and the two are within
a few percent of each other - the same length, and overlapping them is why the whole
job is 1.85-2.72 s rather than their sum.

A dash means the instrumentation that measures it did not exist for that
configuration, not that the figure is zero. The phase rows come from
`bulk_job_outbox`'s four timestamps plus the worker's own in-process timing;
`bulk_job_item` is deleted, so the item-table column's per-batch cost can never be
recovered.

The 4× spread in the submission cells is this host, not the code: six runs at 4
consumers gave 0.58, 1.59, 1.75, 2.16, 2.21 and 2.30 s.

## Where the time goes

Three consecutive runs, fresh volume each time, from the four timestamps on each
batch row plus the worker's own in-process timing.

| phase | run 1 | run 2 | run 3 |
|---|---|---|---|
| **Whole job -- submit to settled** | **1.85 s** | **2.70 s** | **2.72 s** |
| Sweep wait to first page | 62 ms | -- | -- |
| Build, per page (1 thread), min/mean/max | 15/41/75 ms | 23/47/98 ms | 20/45/80 ms |
| Build total | 2.04 s | 2.36 s | 2.25 s |
| Work, per consumer slot | 1,757 ms | 2,353 ms | 2,273 ms |
| **Build / work-per-slot** | **1.16x** | **1.00x** | **1.01x** |
| Relay lag, written to published | 185 ms | 202 ms | 194 ms |
| Queue wait, in RabbitMQ | 21 ms | 108 ms | 144 ms |
| Pool wait | 9.1 ms | 8.7 ms | 8.1 ms |

**The build and the drain are the same length** -- 1.00x and 1.01x in two of the
three runs. That is the finding, and it replaces the earlier claim that the build
was 90% of wall clock on its own.

Because the two overlap, the job costs roughly the longer of them rather than
their sum. In run 1 that is 1.85 s measured against 2.75 s sequential, so
**overlapping earns about 900 ms, a third of the job.** The corollary is the part
worth keeping: shortening *either* phase alone moves the total very little,
because the other becomes the constraint immediately. A 30% win needs both.

**Stable across runs:** pool wait at 7-9 ms (~1.5% of work) and relay lag at
185-202 ms, which is the 250 ms tick. **Variable:** work per batch spans 34% and
queue wait spans 7x, with no configuration change. Neither is a fixed cost, and
quoting either as one would be wrong.

Pages within a job cannot be parallelised -- page N+1's keyset cursor is page N's
last row. Different jobs can be, which is what the build's advisory claim is for.

**Work cannot be read from the database.** `now()` in Postgres is the transaction
*start* time, so `completed_at - started_at` is the gap between two `BEGIN`s --
14 ms against a real 422-565 ms apply. The apply happens inside the work
transaction, after `now()` is captured. It appears only in the worker's log,
which is why the `work=` field on each batch line is load-bearing, and why
`benchmark.ts` shells out to read it back.

**The per-page mean is inflated by contention.** Measured against a quiet table a
page costs ~7 ms (1.1-3.6 ms read, 3.3 ms write). The 41-47 ms above is what it
costs while twelve workers share the same buffers and WAL. The two have not been
separated, so it is not yet known how much of the build is its own work.

**Queue wait can read negative** — a worker claimed a batch before the relay's
`markPublished` committed. Correct, and harmless, but `published_at` records that
publication happened, not an ordering guarantee.

## Filter selectivity

`npx tsx test/happyflow/filter-bench.ts` - one freshly seeded 50,000 record
workspace per case, one **full job** per filter, each case run twice with only the
second (warm) pass reported, whole script run three times. Two rates, because they
measure different things: *build* is records per second of **finding** them,
*total* includes the drain and is what a submitter actually waits on.

| filter | matched | pages | build | build/s | per page | moved | total | total/s |
|---|---|---|---|---|---|---|---|---|
| no filter | 50,000 | 50 | 427–958 ms | 52–117k | 8–17 ms | 50,000 | 1.00–1.37 s | 36.5–49.8k |
| outcome | 12,500 | 13 | 127–436 ms | 29–98k | 7–27 ms | 12,500 | 528–762 ms | 16.4–23.7k |
| value range | 5,050 | 6 | 136–206 ms | 25–37k | 20–23 ms | 5,050 | 390–470 ms | 10.7–12.9k |
| date range | 5,065 | 6 | 60–79 ms | 64–84k | 6–7 ms | 5,065 | 386–407 ms | 12.4–13.1k |

`matched` and `pages` are exact and identical every run. Everything else is a
range, because the per-batch work varies 34% run to run and queue wait 7× — the
same instability documented under "The job". The wide build/s band on the first
two rows is that, not a property of the filter.

**Per page does not show the cost of selectivity**, which is the column this was
written to find. Across four filters and three runs it lands in a 6–27 ms band
with no ordering — the value-range rows are the *slowest* per page, and they
match the fewest records. At this scale a page costs about what a page costs
whatever matched it. The walk's per-page work is dominated by the batch write — a
1,000-element uuid array and a commit — not by how many rows the stage filter
discarded on the way.

An earlier reading of 25 ms against 13 ms looked like a selectivity effect and was
not: the cases ran in order, and the first was always the coldest thing in shared
buffers after 50,000 rows had just been inserted. Each case now runs twice and
only the warm pass is reported, which is why the numbers moved.

**End-to-end rate falls by roughly 3.5× from the unfiltered case to the smallest
filter** — 36.5–49.8k/s against 10.7–13.1k/s — on a job moving five thousand
records instead of fifty thousand. The data is not the reason. Both small cases
settle in **386–470 ms whatever they match**, in all three runs, and roughly half
of that is the job waiting to be noticed: a 250 ms sweep tick before the first
batch exists, then a 250 ms relay tick before any of it is published. **Below
roughly 15,000 records that fixed cost is the job.** A caller filtering for five
thousand records waits about as long as one filtering for five thousand *and
sixty-five*.

Neither rate alone is the honest figure, which is the point of reporting both.
Counting only the build flatters a selective filter — it did less work; counting
only the total punishes it for being small. The build/s band being 2× wider on
selective rows is itself the warning: at 6 pages the measurement is dominated by
the two ticks, not by the walk.

This does not rule the per-page effect out at 500,000, where a selective filter
walks proportionally further past rows it discards. It says the question is not
answerable here, so the `stage_id`-in-the-index decision should not be argued
from these numbers.

## Why 12 consumers

Consumer count was measured, not guessed. The per-batch time is the evidence: 0.41 s
at 4 consumers, 0.44 s at 12. Flat, while throughput rises 2.8×. Linear scaling —
the batches do not get slower, more run at once. Each does 1,000 opportunity
updates and 1,000 transition inserts, so it is compute-bound, not queue-bound, and
matching consumers to cores is the lever.

Corroborated by the runs above: pool wait is 7-9 ms mean against 422-565 ms of work,
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

The residue is therefore inside the 422-565 ms of Postgres work per batch. Three extra
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
| `safety` | 11 | 118 | 9.3 s |
| `endpoints` | 7 | 87 | 4.8 s |
| `happyflow` | 1 | 10 | 36.4 s |
| **all three** | **19** | **215** | passing |

Run individually, so the times are not three projects sharing one process. The
suite grew from 185 because the async snapshot needed tests the old shape had no
use for — the scheduler, resume from a partial cursor, and the per-job build claim
— and because the filter tests found a data-loss bug (see below).

## Known limits

- The 3.5× drain residue is unexplained, though pool starvation is ruled out.
- The build is single-threaded per job. Pages cannot be parallelised within a job
  because each page's keyset cursor is the previous page's last row.
- The build and the drain are the same length, so neither alone is the lever:
  shortening one leaves the other as the constraint.
- The build's per-page cost has not been separated from contention, so how much of
  the build is its own work is still unknown.
- **Below ~15,000 records a job is bound by its polling intervals, not by its
  data.** A 250 ms sweep tick before the first batch exists, then a 250 ms relay
  tick before any of it is published, and both small filter cases settle in
  ~405 ms whatever they match. The floor is roughly 500 ms of waiting and nothing
  in the design removes it. See "Filter selectivity" above.
- Filter selectivity does not measurably change the cost of a page at this scale,
  so whether `stage_id` belongs in the walk's index is not answered here. See
  "Filter selectivity" above.
- The filter benchmark carries its own fixture because the happy-flow one cannot
  be filtered: two stages both with outcome `open`, no created_at spread, and a
  value range too narrow to bound, so three of the four filters would match
  nothing and the fourth everything.
- An earlier version of this file claimed a chunk-size win of 1.21× that was really
  1.04×. The false figure is in the message of pushed commit `8f2052b`.
- Work per batch varies 34% and queue wait 7x between identical runs, so neither is a
  fixed cost and neither should be quoted as one.
- The two harnesses that measured the old shape were deleted with it rather than
  repaired; they would have produced numbers about a design that no longer exists.
