# Design

How a bulk stage move is chunked, made idempotent, kept correct while people edit
the same records, and isolated. Numbers referenced here are in `BENCHMARKS.md`;
the tests that guard each claim are in `TESTSTRATEGY.md`.

## 1. How the job is chunked, and how the cursor survives a restart

**A job's match set is never materialised.** Submission writes one `bulk_job` row
and returns in single-digit milliseconds. The walk that turns a filter into batches
runs afterwards, in `SnapshotBuilder`, paging 1,000 ids at a time.

**The index that makes it cheap** is `opportunity_workspace_created_id_idx` on
`(workspace_id, created_at, id)`. Column order is the whole design: the filter's
workspace is an equality, `created_at <= snapshot_at` is a range, and the cursor is
`(created_at, id)` — so a page is a contiguous index range that starts where the
previous one stopped. Recorded cost for a 1,000-row page against a 50,000-row
table: **2.2 ms**.

**There is no `OFFSET` anywhere in the job path.** With `OFFSET`, the page at
offset 49,000 reads and discards 49,000 index entries to return the last thousand,
making the sweep quadratic in the number of batches. The worker does not page at
all — it reads ids off its batch row.

**The cursor is two columns on the job row**, `snapshot_cursor` and
`snapshot_cursor_id`, and it is committed **in the same transaction as the batch it
precedes**. That is what makes it trustworthy: the cursor can never claim progress
the data does not have. A crash between batches loses at most one page of work and
resumes from the last committed cursor.

The cursor is resolved by **subquery, never by binding a JavaScript `Date`**:

```sql
AND (created_at, id) > (
  SELECT created_at, id FROM opportunity WHERE id = $n AND workspace_id = $1)
```

`timestamptz` carries microseconds and a JS `Date` carries milliseconds, so binding
the cursor truncates it: every remaining row then compares greater than the
truncated value, the same page returns for ever, and every later page conflicts.
The same trap is why the worker reads its stage from a column-to-column comparison
in SQL rather than round-tripping a timestamp.

## 2. Idempotency

**The key is caller-supplied**, required non-empty, and stored on the job row.
Enforced by `CONSTRAINT bulk_job_workspace_idempotency_uniq UNIQUE
(workspace_id, idempotency_key)`.

A derived key — a hash of `(workspace, target, filter)` — was rejected on purpose.
It can only detect an *identical* request, and a deliberate re-run of the same
filter is identical, so it would be refused as a replay. A client key keeps "retry
this" distinguishable from "do it again", which is the distinction a client
actually needs.

**What it protects.** A client that retries `POST /bulk-moves` — because it timed
out, or because a proxy retried — gets the original `jobId` back and creates
nothing. The service returns `replay: true` so the caller can tell.

**The check is in the database, not the application.** The service does a
read-then-insert, which two concurrent callers can both pass; the unique
constraint is what actually holds. Verified by deleting the service check and
confirming the tests still pass, and separately by racing N callers and asserting
exactly one job exists.

**Where a retry could still slip through** — the honest list:

- **A different key for the same intent** creates a second job. Nothing can prevent
  this; a client that mints a fresh uuid per attempt is asking for a second move.
- **A retry after the job row is deleted** creates a new job, because the key dies
  with the row. There is no tombstone.
- **A retry with a different filter or target under the same key is rejected** (409),
  which is deliberate: it means the client changed its mind mid-retry, and silently
  reusing the old job would be worse.

## 3. Concurrency on a single opportunity

Two actors can want the same record: a person moving it, and a bulk job. The
worker applies six checks in a fixed order, and the order is load-bearing.

| # | check | outcome |
|---|---|---|
| 1 | the record still exists | fail |
| 2 | **is already in the target stage** | count as **moved** |
| 3 | `record.stage_decided_at <= job.snapshot_at` | **skip** |
| 4 | still in a stage the filter names | **skip** |
| 5 | a permitted transition to the target exists | fail |
| 6 | — | **move**, insert one transition |

**Check 2 must precede check 3.** A person who moves a record *to* the target has
stamped it with a time newer than the job's, so testing the clock first would
report a skip — under-counting a job that in fact achieved its intent. It also
makes retries idempotent: a record a half-finished batch already moved is counted
as moved, not as a failure.

**So: a manual move and the job hitting the same record → the person wins.** If
their edit lands before the job's watermark, the job treats the new stage as the
starting point. If it lands after, the record is skipped and counted as a
shortfall, which the job reports rather than hiding.

**`stage_decided_at` is a logical clock, not a timestamp of change.** A person
stamps the wall clock. A bulk job stamps **its own `snapshot_at`**. The rule is
`skip if record.stage_decided_at > job.snapshot_at`, which compares *when jobs were
submitted* rather than when rows were written — so the newest job wins no matter
which finishes first. Writing the wall clock would make it last-writer-wins, and an
older job still draining would beat a newer one.

**It needs no synchronised clocks**, because every timestamp in the comparison
comes from the one Postgres: the job's `snapshot_at` is `now()` at insert, the
trigger's `now()` is the same clock, and the worker reads its stamp from its own
row rather than its own system time.

The clock is maintained by a trigger with **two** `WHEN` clauses, both load-bearing:

```sql
WHEN (  OLD.stage_id IS DISTINCT FROM NEW.stage_id
    AND NEW.stage_decided_at IS NOT DISTINCT FROM OLD.stage_decided_at )
```

- The first stops a rename or an owner change from bumping the clock, which would
  silently drop the record from every in-flight job — a cosmetic edit cancelling
  real work.
- The second is the opt-out. The worker sets the column to its own `snapshot_at`,
  making `NEW` distinct from `OLD`, so the trigger stands down. Without it a bulk
  job would stamp 1,000 rows with the wall clock and defeat the entire design.

**No `version` column.** Two people editing the same deal is the gap: the second
save wins. See § 7.

## 4. Snapshot vs live filter set

**Neither, exactly — the predicate is frozen and the result set is not.** A
watermark on `bulk_job.snapshot_at` is bound at submission, and every page resolves
`created_at <= snapshot_at`. A record created after submission can never be swept
up; a record that existed at submission is found whenever the walk reaches it.

This is what replaced a materialised snapshot, and it is what lets submit write 50
rows instead of 50,000 — 50 batch rows of 1,000 uuids rather than 50,000 item rows,
which were **81% of the time to the `201`**.

**It is sound because `created_at` is immutable.** The same filter and the same
watermark resolve to the same set, every time, for every worker. The watermark is
bound once at insert rather than being `now()` per query, which would advance with
each transaction and re-open the set mid-walk.

**The consequence is that the match count is not known at submission.** It is
reported only once the walk has counted, so `totalMatched` is 0 while a job is
`preparing`. A client that wanted "how many would this match?" has to ask after
the fact — there is no cheap pre-count that stays correct, because records leave
the filter while the walk runs.

**The consequence that matters for correctness:** the *filter* is frozen but
*stage membership is not*. A record can be in the match set and no longer in a
stage the filter named, which is exactly what check 4 catches.

## 5. Isolation, and the hole it leaves

**Every table carries `workspace_id NOT NULL`, and every cross-table reference is a
composite foreign key on `(id, workspace_id)`.** So an opportunity *cannot* point
at another workspace's stage or owner — the insert fails whatever the service does.
This is enforced by the database, not by application checks that a future endpoint
might forget.

```
opportunity_stage_fk  FOREIGN KEY (stage_id, workspace_id)
                      REFERENCES stage (id, workspace_id)
```

`NOT NULL` on `email` is load-bearing too: NULLs are distinct in a unique index, so
a nullable `UNIQUE (workspace_id, email)` would let any number of nameless users
share a workspace unjudged by the constraint.

**Query scoping is separate and is the weaker half.** Every repository query filters
on `workspace_id` explicitly, because a `WHERE` clause is not implied by a foreign
key. That is a convention, not a guarantee — a forgotten predicate leaks rows.

**There is no endpoint that enumerates workspaces.** A caller cannot discover other
tenants by asking. The CLI reads the database directly instead, for exactly this
reason: it already holds the credentials, and adding a route would give the
capability to anyone who can reach the API.

**The hole this still leaves:** isolation is enforced at the *row* level, not the
*connection* level. Every service shares one Postgres and one credential set, so a
bug in any single query that omits `workspace_id` returns another tenant's rows
with no error. There is no row-level security, no separate database or role per
tenant, and no test that can prove the absence of a future mistake — only tests
that the queries written *so far* are correct. Row-level security policies would
close it at the cost of a predicate on every query.

## 6. What breaks at 10×

**At 500,000 records in one job it still completes cleanly** — 500,000 moved, 0
failed, 57.34 s. The first thing to break at that size was submission, and it is
already fixed: the old inline walk was super-linear, 0.56 s at 50,000 became
**32.06 s at 500,000**, because it accumulated every batch's ids in memory before
writing any — roughly 50–100 MB of uuid strings. It now streams, one page at a
time, so response time is flat and memory is bounded by one page.

**The first thing that will fail at 20M opportunities in a workspace is the
`opportunity_workspace_created_id_idx` depth, and the symptom is the batching
phase, not the drain.**

Why, in numbers: `shared_buffers` on this host is **128 MB**, and that index is
already **44 MB at 50,000 rows** — a third of the entire buffer cache, for one
workspace's worth of one column. At 20M rows the same index is roughly **17 GB**,
about 130× the cache, so every 1,000-row page becomes a real disk read. Measured
per-page cost is 18 ms at 50,000 with a warm cache against ~7 ms on a quiet table;
that gap is contention, and it widens with index size rather than staying flat.

Extrapolating from the 1.40 s whole job: batching scales linearly in rows while the
drain is fixed at ~1.27 s of work per slot, independent of workspace size. So
somewhere past a few million records batching dominates, and the job becomes bound
by sequential index scans over an index that does not fit in memory.

**What I would do, in order:**

1. **Partition `opportunity` by `created_at` range** (monthly), so the watermark
   prunes to one partition and the index per partition stays small. This is the
   single biggest win and it is also what makes retention possible.
2. **Parallelise the walk across pages.** It is serialised only because page N+1's
   cursor is page N's last row. A fixed number of parallel range scans over
   pre-computed partition bounds would break that dependency — but only if each
   worker writes batches for a disjoint id range, which needs the uniqueness
   guarantee that does not currently exist (§ 7).
3. **Raise `shared_buffers` and move to a covering index** including the columns the
   walk selects (`id, stage_id, created_at`), so a page is an index-only scan.
4. **Then** reconsider the batch size. 1,000 is a guess, not a measurement: the
   worker's per-batch time is flat from 4 to 12 consumers, which says the batch is
   not the unit of contention.

**What does not break, and why I am confident:** the drain. It is bounded by
per-batch work (392 ms mean) divided across 12 slots, and it is independent of
workspace size. The outbox is also fine — 50 batch rows per 50,000 records, so
20M opportunities is 20,000 rows, which is nothing.

**What I cannot claim:** the 500,000 figure **predates the streaming fix**, so it is
a lower bound on the current design's behaviour, not a measurement of it. It has
not been re-run since.

## 7. What I would do with another week, ranked

1. **Re-measure at 500,000 records.** The only number in this document I have not
   verified against the current code. Everything above is extrapolation until it is.
2. **Add `opportunity.version`.** The one real concurrency gap left. A
   `version integer` with `WHERE version = $expected`, rejecting the later save with
   a 409. The constraint that makes it safe: **a bulk job must never bump it**, or a
   job moving 1,000 records hands every affected user a 409 about a field they never
   edited. Not built because there is no UI to make it observable.
3. **Kick the pipeline instead of polling it.** `POST /bulk-moves` waking the
   builder, and the builder waking the relay after each page commits, leaving the
   timers as the recovery path. Removes the ~125 ms floor for small jobs entirely;
   the timers already exist and already re-entrancy-guard.
4. **Separate retry queues by workspace** or shard the relay, so one tenant's
   backlog cannot delay another's.
5. **Add a rate limit on submit.** A Postgres counter, no new dependency. Async
   submission made job creation cheap enough to spam.
6. **Row-level security**, closing the query-scoping gap in § 5 at the cost of a
   predicate per query.
7. **Retention.** Nothing prunes `bulk_job` or `opportunity_transition`; both grow
   without bound. Partitioning (item 1 above) makes this a drop-partition rather
   than a delete.

**Not on the list, deliberately:** Redis. Every piece of state here is either
durable in Postgres or per-process, so there is nothing a cache could hold that is
safe to hold. It would earn its place for leader election at high replica counts, or
for a read path `EXPLAIN` shows hammering Postgres. Neither is true yet.
