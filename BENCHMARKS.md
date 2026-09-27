# Benchmarks

50,000 opportunities, one job, `npm run bench`.

Measured on Windows 11, 12 logical cores, 15.7 GB, Node v24.21.0, PostgreSQL
16.15, RabbitMQ 4.3.6 — one Compose project, all services sharing one Postgres.
Not a portable SLA.

## 50k bulk job

| 50,000 records | item table, 4 consumers | batches, 4 consumers | batches, 12 consumers | **+ scheduler, 250 ms ticks** | **+ scheduler, 125 ms ticks** |
|---|---|---|---|---|---|
| **Submission (returns job id)** | 8.40 s | 0.56 – 2.30 s | 0.56 – 2.30 s | **9.2 ms – 53 ms** | **10 – 22 ms** |
| Snapshot batching (before any batch exists) | inline in submit | inline in submit | inline in submit | **1.54 – 2.36 s** | **812 ms – 1.54 s** |
| Drain — snapshot built to settled | 0.52 s | 5.18 s | **1.83 s** | overlaps the batching | overlaps the batching |
| **Whole job — submit to settled** | **8.92 s** | **7.47 s** | **2.39 s** | **1.82 – 2.72 s** | **1.52 – 2.37 s** |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** | **18,400 – 27,500/sec** | **21,300 – 27,300/sec** |
| | | | | | |
| Batches | 50,000 rows | 50 | **50** | **50** | **50** |
| Work per batch, mean | — | — | — | **422 – 565 ms** | **392 ms** |
| Pool wait per batch, mean | — | — | — | **7.3 – 9.1 ms** | — |
| Relay lag, mean | — | — | — | **185 – 202 ms** | **99 – 111 ms** |
| Queue wait, mean | — | — | — | **21 – 144 ms** | **116 – 280 ms** |
| Batches per consumer slot | — | — | — | 4–5 each, all 12 busy | 4–5 each, all 12 busy |

The two right-hand columns are the same design, and the only difference between
them is the sweep and relay interval: 250 ms, then 125 ms. Everything left of those
is a different design — batching the ids in memory, then moving the walk off the
request path — and the row that matters across all of them is the one where
submission collapses from seconds to milliseconds.

**Halving the tick made the job faster and the queue worse.** Whole job 1.82-2.72 s
against 1.52-2.37 s, and relay lag's mean roughly halved from 185-202 ms to
99-111 ms. But **queue wait more than doubled, 21-144 ms to 116-280 ms**, because
publishing twice as often delivers a heavier stream to twelve consumers and batches
sit in the broker longer. A caller waits less; a batch waits more. Three runs is
thin for a range that wide, so the size of the regression is not settled.

**The pool wait row has no 125 ms figure, and that is a measurement gap rather than
an improvement.** The worker's per-batch line reports `pool=4conn/0queued` — a
connection count and a queue depth, not a wait time — so the 7.3-9.1 ms in the
250 ms column was read from something the current log line no longer carries. It
was dropped rather than restated as a dash implying zero.

**Submission is 250× faster, and the whole job is no slower.** That is the trade
the scheduler makes: the walk still costs 0.8-1.5 s, it just no longer happens while
somebody is waiting on an HTTP response. It is a trade rather than a free win
because the walk competes with the workers for the same Postgres — but at the
125 ms tick the job is faster end to end than it was at 250 ms, so the cost has
been more than paid back.

**"Drain — snapshot built to settled" has no value in either scheduler column**
because it cannot be isolated: batches are consumed while the batching is still
running. The drain's own length is the work total divided across twelve slots, and
in both scheduler columns it is the longer of the two — overlapping them is why the
whole job is 1.82-2.72 s and 1.52-2.37 s rather than their sums.

A dash means the instrumentation that measures it did not exist for that
configuration, not that the figure is zero. The phase rows come from
`bulk_job_outbox`'s four timestamps plus the worker's own in-process timing;
`bulk_job_item` is deleted, so the item-table column's per-batch cost can never be
recovered.

The 4× spread in the submission cells is this host, not the code: six runs at 4
consumers gave 0.58, 1.59, 1.75, 2.16, 2.21 and 2.30 s.

## Where the time goes

Three consecutive runs at the 125 ms tick, fresh volume each time, from the four
timestamps on each batch row plus the worker's own in-process timing.

- **The drain is the constraint now, not batching** — 0.55x-0.89x, reversing the
  1.00x this section used to report. At 812 ms of batching against 1.49 s of work
  per slot there is 678 ms of headroom the job was never waiting on. Batching
  nonetheless got about 2x faster (per-page mean 41-47 ms to 16-31 ms) from a timer
  that is not on the page-write path. That is unexplained, and it is why the
  per-page figure is not yet trusted: a quiet table pages in ~7 ms, so most of the
  16-31 ms is contention with the twelve workers rather than the walk's own work.
- **The tighter tick was not free.** Queue wait mean 116-280 ms against 21-144 ms,
  summing to 14.00 s across the 50 batches against 5.78 s — publishing twice as
  often delivers a heavier stream to twelve consumers, so batches sit in the broker
  longer. The job a caller waits for got faster, 1.52-2.37 s against 1.82-2.72 s,
  while batches queued roughly twice as long. Three runs is thin for a range that
  wide, so the size of the regression is unsettled. Against it: relay lag halved
  (mean 99-111 ms, floor 43-55 ms, against a previously flat 185 ms that was almost
  entirely tick) and sweep wait fell to 7-32 ms from 62 ms.
- **Work cannot be read from the database.** `now()` is the transaction *start*
  time, so `completed_at - started_at` is the gap between two `BEGIN`s — a few ms
  against a real 174-598 ms apply. It exists only in the worker's log, which is why
  the `work=` field on each batch line is load-bearing and why `benchmark.ts` shells
  out to read it back.
- **Two things that look like bugs and are not.** Pages cannot be parallelised
  within a job, because page N+1's keyset cursor is page N's last row; different
  jobs can be, which is what the advisory claim is for. And queue wait can read
  negative, because a worker claimed a batch before the relay's `markPublished`
  committed — `published_at` records that publication happened, not an ordering
  guarantee.

## Filter selectivity

`npx tsx test/happyflow/filter-bench.ts` - one freshly seeded 50,000 record
workspace per case, one **full job** per filter, each case run twice with only the
second (warm) pass reported, whole script run three times per tick. **Per
transition** is total wall clock divided by records moved, which is the only column
here that is directly comparable across filters of different sizes: the raw totals
are not, because a 5,000-record job pays the same fixed cost as a 50,000-record one.

| filter | matched | pages | per transition, 250 ms | per transition, 125 ms | moved | verdict |
|---|---|---|---|---|---|---|
| no filter | 50,000 | 50 | 20.0-27.4 µs | 24.4-31.0 µs | 50,000 | **unchanged** — ranges overlap |
| outcome | 12,500 | 13 | 41.1-61.0 µs | 37.4-43.4 µs | 12,500 | **-21%** |
| value range | 5,050 | 6 | 77.2-93.1 µs | 66.3-67.5 µs | 5,050 | **-21%** |
| date range | 5,065 | 6 | 76.2-80.4 µs | 54.3-66.3 µs | 5,065 | **-23%** |

**No regression anywhere.** Every selective filter got about 21-23% cheaper per
transition, and the unfiltered job is unchanged within run-to-run noise — its two
ranges overlap, and `benchmark.ts` puts the same 50,000-record job the other way
(1.82-2.72 s at 250 ms against 1.52-2.37 s at 125 ms), which is what noise at this
scale looks like. The tick change was never going to help a job that spends its
time on per-record work rather than waiting.

**The finding is the 3.5x spread down the per-transition column, not the tick
change.** 23.7 µs at 50,000 records against 85.1 µs at 5,050: the smaller the job,
the worse it pays per record, because the fixed costs — one sweep tick, one relay
tick, one builder pass — do not amortise over fewer records. That is the honest
version of "small jobs are slow", and total/s alone hides it by reporting a rate.

**Per page does not show the cost of selectivity**, which is the column this was
originally written to find. Across four filters and three runs it lands in a
6-18 ms band with no ordering — the value-range rows are the *slowest* per page and
they match the fewest records. The walk's per-page work is dominated by the batch
write, a 1,000-element uuid array and a commit, not by how many rows the stage
filter discarded on the way. So whether `stage_id` belongs in the walk's index is
**not answered here** and should not be argued from these numbers. An earlier
reading of 25 ms against 13 ms looked like an effect and was a warm-up curve: the
cases ran in order and the first was always the coldest thing in shared buffers
after 50,000 rows had just been inserted.

**Halving the ticks mitigated the floor rather than removing it.** A job waits one
sweep tick before its first batch exists and one relay tick before any of it is
published, so the expected wait is half the interval per tick: ~125 ms instead of
~250 ms, which is where the ~21-23% above comes from on a 5,000-record job. It is
still a floor. Removing it needs a kick rather than a shorter tick — `POST
/bulk-moves` waking the builder, and the builder waking the relay after each page
commits, with the timers left as the recovery path for a crashed replica. **Not
built.**

The tighter tick is affordable because both polls are index scans that usually
return nothing — the sweep reads `bulk_job_preparing_idx`, the relay reads
`bulk_job_outbox_unpublished_idx`, itself partial over `WHERE published_at IS
NULL` — so an idle tick touches one index entry. Below this it stops being free,
which is where the event-driven version becomes the right answer rather than merely
the better one.

**The record count at which waiting stops dominating has moved and is not
re-measured.** The smallest filter here is 5,050 records, which is not small enough
to locate it. **No threshold should be quoted until this bench runs cases below
1,000.**

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
- **A small job is bound by its polling intervals, not by its data.** A sweep tick
  before the first batch exists, then a relay tick before any of it is published.
  Halved from 250 ms to 125 ms, which took ~100 ms off every small job: 5,050
  records settled in 335–341 ms against 390–470 ms, and 5,065 in 275–336 ms against
  386–407 ms. The floor is now ~125 ms of expectation rather than ~250 ms, and is
  still a floor — removing it needs a kick rather than a shorter tick. The record
  count at which waiting stops dominating has moved with it and is not re-measured;
  see "Filter selectivity" above.
- Queue wait roughly doubled when the tick was halved, which is the cost of it.
  Unsettled at three runs. See "Where the time goes".
- Batching got about 2x faster when only a timer changed, and nothing in the tick
  is on the path of a page write. Unexplained. See "Where the time goes".
