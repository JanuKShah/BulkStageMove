# Benchmarks

50,000 opportunities, one job, `npm run bench`.

Measured on Windows 11, 12 logical cores, 15.7 GB, Node v24.21.0, PostgreSQL
16.15, RabbitMQ 4.3.6 — one Compose project, all services sharing one Postgres.
Not a portable SLA.

## The job

| 50,000 records | item table, 4 consumers | batches, 4 consumers | batches, 12 consumers | **+ scheduler, 12 consumers** |
|---|---|---|---|---|
| **Submission (returns job id)** | 8.40 s | 0.56 – 2.30 s | 0.56 – 2.30 s | **9.2 ms – 53 ms** |
| Snapshot batching (before any batch exists) | inline in submit | inline in submit | inline in submit | **1.54 – 2.36 s** |
| Drain — snapshot built to settled | 0.52 s | 5.18 s | **1.83 s** | overlaps the batching |
| **Whole job — submit to settled** | **8.92 s** | **7.47 s** | **2.39 s** | **1.82 – 2.72 s** |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** | **18,400 – 27,500/sec** |
| | | | | |
| Batches | 50,000 rows | 50 | **50** | **50** |
| Work per batch, mean | — | — | — | **422 – 565 ms** |
| Pool wait per batch, mean | — | — | — | **7.3 – 9.1 ms** |
| Relay lag, mean (at the old 250 ms tick) | — | — | — | **185 – 202 ms** |
| Queue wait, mean | — | — | — | **21 – 144 ms** |
| Batches per consumer slot | — | — | — | 4–5 each, all 12 busy |

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
| Batching, per page (1 thread), min/mean/max | 15/41/75 ms | 23/47/98 ms | 20/45/80 ms |
| Batching total | 2.04 s | 2.36 s | 2.25 s |
| Work, per consumer slot | 1,757 ms | 2,353 ms | 2,273 ms |
| **Batching / work-per-slot** | **1.16x** | **1.00x** | **1.01x** |
| Relay lag, written to published | 185 ms | 202 ms | 194 ms |
| Queue wait, in RabbitMQ | 21 ms | 108 ms | 144 ms |
| Pool wait | 9.1 ms | 8.7 ms | 8.1 ms |

**Batching and the drain are the same length** -- 1.00x and 1.01x in two of the
three runs. That is the finding, and it replaces the earlier claim that the batching
was 90% of wall clock on its own.

Because the two overlap, the job costs roughly the longer of them rather than
their sum. In run 1 that is 1.85 s measured against 2.75 s sequential, so
**overlapping earns about 900 ms, a third of the job.** The corollary is the part
worth keeping: shortening *either* phase alone moves the total very little,
because the other becomes the constraint immediately. A 30% win needs both.

**Stable across runs:** pool wait at 7-9 ms (~1.5% of work) and relay lag at
**Stable across runs:** pool wait at 7-9 ms (~1.5% of work) and relay lag at
185-202 ms, which *was* the 250 ms tick. Both rows above predate the halving to
125 ms and have not been re-measured. **Variable:** work per batch spans 34%
and queue wait spans 7x, with no configuration change. Neither is a fixed cost,
and quoting either as one would be wrong.
Pages within a job cannot be parallelised -- page N+1's keyset cursor is page N's
last row. Different jobs can be, which is what batching's advisory claim is for.

**Work cannot be read from the database.** `now()` in Postgres is the transaction
*start* time, so `completed_at - started_at` is the gap between two `BEGIN`s --
14 ms against a real 422-565 ms apply. The apply happens inside the work
transaction, after `now()` is captured. It appears only in the worker's log,
which is why the `work=` field on each batch line is load-bearing, and why
`benchmark.ts` shells out to read it back.

**The per-page mean is inflated by contention.** Measured against a quiet table a
page costs ~7 ms (1.1-3.6 ms read, 3.3 ms write). The 41-47 ms above is what it
costs while twelve workers share the same buffers and WAL. The two have not been
separated, so it is not yet known how much of the batching is its own work.

**Queue wait can read negative** — a worker claimed a batch before the relay's
`markPublished` committed. Correct, and harmless, but `published_at` records that
publication happened, not an ordering guarantee.

## Filter selectivity

`npx tsx test/happyflow/filter-bench.ts` - one freshly seeded 50,000 record
workspace per case, one **full job** per filter, each case run twice with only the
second (warm) pass reported, whole script run three times. Two rates, because they
measure different things: *batching* is records per second of **finding** them,
*total* includes the drain and is what a submitter actually waits on.

| filter | matched | pages | batching | batching/s | per page | moved | total | total/s |
|---|---|---|---|---|---|---|---|---|
| no filter | 50,000 | 50 | 857-951 ms | 52-58k | 16-18 ms | 50,000 | 1.22-1.55 s | 32.3-41.0k |
| outcome | 12,500 | 13 | 198-216 ms | 58-63k | 10-14 ms | 12,500 | 468-543 ms | 23.0-26.7k |
| value range | 5,050 | 6 | 65-145 ms | 35-78k | 8-18 ms | 5,050 | 335-341 ms | 14.8-15.1k |
| date range | 5,065 | 6 | 62-81 ms | 63-82k | 6-8 ms | 5,065 | 275-336 ms | 15.1-18.4k |

`matched` and `pages` are exact and identical every run. Everything else is a
range, because the per-batch work varies 34% run to run and queue wait 7× — the
same instability documented under "The job". The wide batching/s band on the first
two rows is that, not a property of the filter.

**Halving both ticks from 250 ms to 125 ms took ~100 ms off every small job**, and
the effect is consistent across three runs:

| | 250 ms | 125 ms |
|---|---|---|
| value range, 5,050 records | 390-470 ms | **335-341 ms** |
| date range, 5,065 records | 386-407 ms | **275-336 ms** |
| outcome, 12,500 records | 514-762 ms | **468-543 ms** |

~100 ms rather than the ~125 ms the arithmetic suggests, because a uniform tick's
*expected* wait is half its interval: 62.5 ms per tick instead of 125, and there
are two ticks on the path. The 50,000-record row is unchanged within noise, which
is the point — the ticks were never that job's problem.

**This mitigates rather than fixes, and the distinction is the finding.** The floor
is now ~125 ms of expectation instead of ~250 ms, and it is still a floor. Polling
imposes a wait; halving the interval halves it. Removing it needs a kick rather
than a shorter tick: `POST /bulk-moves` waking the builder, and the builder waking
the relay after each page commits, with the timers left as the recovery path for a
crashed replica. **Not built.**

The tighter tick is affordable because both polls are index scans that usually
return nothing — the sweep reads `bulk_job_preparing_idx`, and the relay reads
`bulk_job_outbox_unpublished_idx`, itself a partial index over
`WHERE published_at IS NULL`, so an idle tick touches one index entry. Doubling
the poll rate costs two empty index lookups every 125 ms. Going much below this
stops being free, which is where the event-driven version becomes the right answer
rather than merely the better one.

**Per page does not show the cost of selectivity**, which is the column this was
written to find. Across four filters and three runs it lands in a 6–18 ms band
with no ordering — the value-range rows are the *slowest* per page, and they
match the fewest records. At this scale a page costs about what a page costs
whatever matched it. The walk's per-page work is dominated by the batch write — a
1,000-element uuid array and a commit — not by how many rows the stage filter
discarded on the way.

An earlier reading of 25 ms against 13 ms looked like a selectivity effect and was
not: the cases ran in order, and the first was always the coldest thing in shared
buffers after 50,000 rows had just been inserted. Each case now runs twice and
only the warm pass is reported, which is why the numbers moved.

**End-to-end rate falls by roughly 2.5× from the unfiltered case to the smallest
filter** — 32.3–41.0k/s against 14.8–15.1k/s — on a job moving five thousand
records instead of fifty thousand. The data is not the reason: the two small cases
land within a few tens of milliseconds of each other, **335–341 ms and 275–336 ms**,
whatever they match, across all three runs. What is left of that is still mostly
waiting to be noticed — one tick before the first batch exists, one before any of
it is published. **Below roughly 15,000 records that fixed cost is the job.** A
caller filtering for five thousand records waits about as long as one filtering for
five thousand *and sixty-five*.

Neither rate alone is the honest figure, which is the point of reporting both.
Counting only the batching flatters a selective filter — it did less work; counting
only the total punishes it for being small. The batching/s band being 2× wider on
selective rows is itself the warning: at 6 pages the measurement is dominated by
the two ticks, not by the walk.

This does not rule the per-page effect out at 500,000, where a selective filter
walks proportionally further past rows it discards. It says the question is not
answerable here, so the `stage_id`-in-the-index decision should not be argued
from these numbers.

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

- Batching is single-threaded per job. Pages cannot be parallelised within a job
  because each page's keyset cursor is the previous page's last row.
- Batching's per-page cost has not been separated from contention, so how much of
  the batching is its own work is still unknown.
- **Below ~15,000 records a job is bound by its polling intervals, not by its
  data.** A sweep tick before the first batch exists, then a relay tick before any
  of it is published. Halved from 250 ms to 125 ms, which took ~100 ms off every
  small job: 5,050 records settled in 335–341 ms against 390–470 ms, and 5,065 in
  275–336 ms against 386–407 ms. The floor is now ~125 ms of expectation rather
  than ~250 ms, and is still a floor — removing it needs a kick rather than a
  shorter tick. See "Filter selectivity" above.
