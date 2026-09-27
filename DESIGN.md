# Design

How a bulk stage move is chunked, made idempotent, kept correct while people edit the
same records, and isolated. Numbers are in `BENCHMARKS.md`; the tests guarding each
claim are in `TESTSTRATEGY.md`.

## 0. The shape of it

![Design diagram](docs/design.png)

After step 1 the job row is the only thing that exists; nothing is held in memory.

| # | step | what it does | the part that is not obvious |
|---|---|---|---|
| 1 | **Submit** | Resolves the filter to stage ids, writes **one** job row with `snapshot_at = now()`, returns `201` in ~13 ms | No batch exists yet, so the row defaults to `preparing` — the state a job with no batches is actually in |
| 2 | **Batching** | A 125 ms sweep claims the job with an advisory lock, walks the filter keyset-style, 1,000 ids per page | Each page commits its batch **and** the cursor after it, in one transaction |
| 3 | **Relay** | A 125 ms timer publishes unpublished batch rows | Marked published only *after* the broker confirms, so a crash re-publishes rather than loses |
| 4 | **Drain** | 12 consumers take a per-batch lock, claim `pending → running`, apply in one transaction | Those two guards make at-least-once effectively-once — see 3 |
| 5 | **Status** | `GET /bulk-moves/:id` — progress, batches, failures, dead letters | A dead-lettered message does not stop the rest of the job |

## 1. Chunking, and surviving a restart

- **The match set is never materialised.** Submit writes one row; the walk pages
  1,000 ids at a time afterwards.
- **The index:** `opportunity_workspace_created_id_idx` on
  `(workspace_id, created_at, id)`. Equality, range, cursor — so a page is a
  contiguous index range starting where the last stopped. Recorded 2.2 ms for
  1,000 rows against a 50,000-row table.
- **No `OFFSET` anywhere in the job path.** At offset 49,000 it would read and
  discard 49,000 entries to return the last thousand, making the sweep quadratic.
  The worker does not page at all — it reads ids off its batch row.
- **The cursor** is `snapshot_cursor` + `snapshot_cursor_id` on the job row,
  committed **in the same transaction as the batch it follows** — so it can never
  claim progress the data does not have. A crash loses at most one page.
- **Resolved by subquery, never a bound `Date`:** `timestamptz` has microseconds,
  a JS `Date` has milliseconds, so binding truncates and the same page returns for
  ever. Same trap made the worker compare column-to-column in SQL rather than
  round-tripping a timestamp.

## 2. Idempotency

- **Key:** caller-supplied, required, stored on the job row, enforced by
  `UNIQUE (workspace_id, idempotency_key)`.
- **Not derived from the request.** A hash of `(workspace, target, filter)` can only
  detect an *identical* request, and a deliberate re-run is identical — it would be
  refused as a replay. A client key keeps "retry this" distinct from "do it again".
- **Protects:** a client retrying `POST` gets the original `jobId` and creates
  nothing; the body reports `replay: true`.
- **Held by the database, not the application** — the service does read-then-insert,
  which two callers can both pass.
- **Where a retry still slips through:** a different key for the same intent (a
  client minting a fresh uuid is asking for a second move); a retry after the job
  row is deleted (no tombstone); a same-key retry with a changed filter or target is
  rejected 409, deliberately.

## 3. Concurrency on one opportunity

Six checks, fixed order. The order is load-bearing.

| # | check | outcome |
|---|---|---|
| 1 | the record still exists | fail |
| 2 | **is already in the target stage** | count as **moved** |
| 3 | `record.stage_decided_at <= job.snapshot_at` | **skip** |
| 4 | still in a stage the filter names | **skip** |
| 5 | a permitted transition to the target exists | fail |
| 6 | — | **move**, insert one transition |

- **Check 2 must precede 3.** A person who moves a record *to* the target stamped it
  newer than the job, so testing the clock first reports a skip and under-counts a
  job that achieved its intent. It also makes retries idempotent.
- **A manual move and the job on one record → the person wins.** Before the
  watermark, the job takes the new stage as its starting point; after it, the record
  is skipped and the shortfall reported.
- **`stage_decided_at` is a logical clock.** A person stamps the wall clock, a job
  stamps its own `snapshot_at`, so `skip if record.stage_decided_at > job.snapshot_at`
  compares *submission* order — newest job wins however long the older takes.
- **No synchronised clocks needed:** every timestamp in that comparison comes from
  the one Postgres.
- **The trigger has two `WHEN` clauses, both load-bearing:**
  `OLD.stage_id IS DISTINCT FROM NEW.stage_id` (so a rename or owner change cannot
  drop a record from an in-flight job) **and**
  `NEW.stage_decided_at IS NOT DISTINCT FROM OLD.stage_decided_at` (the opt-out —
  the worker sets the column, so the trigger stands down; without it a bulk job
  would stamp 1,000 rows with the wall clock and defeat the design).
- **No `version` column.** Two people editing one deal: the second save wins. See 7.

## 4. Snapshot vs live

- **Neither — the predicate is frozen, the result set is not.** A watermark on
  `bulk_job.snapshot_at`, bound at submission, plus `created_at <= snapshot_at`.
- **Sound because `created_at` is immutable:** same filter plus same watermark
  resolves to the same set, for every worker, every time. Bound once at insert, not
  `now()` per query, which would advance mid-walk and re-open the set.
- **This is what replaced a materialised snapshot** — 50 batch rows of 1,000 uuids
  instead of 50,000 item rows, which were 81% of the time to the `201`.
- **Consequence:** the count is not knowable at submit. `totalMatched` is 0 while
  `preparing`. There is no cheap pre-count that stays correct, because records leave
  the filter while the walk runs.
- **Consequence for correctness:** filter membership is frozen, *stage* membership
  is not — a record can be in the match set and no longer in a named stage, which
  is what check 4 catches.

## 5. Isolation, and the hole

- **Composite foreign keys.** Every table carries `workspace_id NOT NULL`; every
  cross-table reference is `(id, workspace_id)`. An opportunity *cannot* point at
  another workspace's stage — the insert fails whatever the service does.
- **`email` is `NOT NULL`** because NULLs are distinct in a unique index, so a
  nullable `UNIQUE (workspace_id, email)` would let nameless users share a
  workspace unjudged by the constraint.
- **Query scoping is the weaker half.** Every repository query filters on
  `workspace_id` explicitly, because a `WHERE` clause is not implied by a foreign
  key. That is convention, not guarantee.
- **No endpoint enumerates workspaces** — a caller cannot discover tenants by
  asking. The CLI reads the database directly for this reason.
- **The hole:** isolation is enforced per *row*, not per *connection*. One shared
  database and one credential set means one forgotten predicate leaks another
  tenant's rows with no error. No row-level security, and no test can prove the
  absence of a future mistake.

## 6. What breaks at 10×

- **500,000 in one job still completes** — 500,000 moved, 0 failed, 57.34 s.
- **What already broke and is fixed:** submission was super-linear, 0.56 s at 50,000
  became **32.06 s at 500,000**, accumulating every id in memory (50–100 MB). It now
  streams one page at a time.
- **What breaks next: the index depth, and the symptom is batching, not the drain.**
  `shared_buffers` is **128MB**; `opportunity_workspace_created_id_idx` is already
  **44MB at 50,000 rows**. At 20M rows it is ~17GB, ~130× the cache, so every page
  is a disk read. Batching scales linearly in rows; the drain is fixed at ~1.27 s of
  work per slot.
- **Fix, in order:** (1) partition `opportunity` by `created_at` range so the
  watermark prunes to one partition — biggest win, and it enables retention;
  (2) parallel range scans over pre-computed partition bounds, which needs the
  uniqueness guarantee item 1 of 7 does not have; (3) covering index including
  `id, stage_id, created_at` for index-only scans; (4) then revisit the 1,000 batch
  size, which is a guess rather than a measurement.
- **What does not break:** the drain — bounded by per-batch work (392 ms) across 12
  slots, independent of workspace size. The outbox is 20,000 rows at 20M
  opportunities, which is nothing.
- **What I cannot claim:** the 500,000 figure **predates the streaming fix**, so it is
  a lower bound, not a measurement of the current design.

## 7. Another week, ranked

1. **Re-measure at 500,000.** The only number here not verified against current code.
2. **`opportunity.version`** — the one real concurrency gap. `WHERE version = $expected`,
   reject the later save 409. **A bulk job must never bump it**, or a job moving
   1,000 records hands every affected user a 409 about a field they never edited.
3. **Kick the pipeline instead of polling it** — removes the ~125 ms floor entirely.
   Polling imposes a wait, so halving the interval only halves it.
4. **Expose a records-moved count** — the endpoint reports batches settled and records
   failed, not records moved, so `job-watch` can only report batches.
5. **Shard the relay by workspace** so one tenant's backlog cannot delay another's.
6. **Rate limit on submit** — a Postgres counter, no new dependency.
7. **Row-level security**, closing the gap in 5.
8. **Retention** — nothing prunes `bulk_job` or `opportunity_transition`.

## 8. Deliberately not built

- **Redis.** Added, then removed: every piece of state is either durable in Postgres
  or per-process, so there is nothing a cache could hold that is safe to hold.
- **Six services, not one per noun.** Split by *write ownership*; the only
  cross-service traffic is a handful of `stage-service` lookups per request.
- **Websockets for progress.** The CLI polls every 2 s, which is the right trade
  while the poll is 2 s.
- **A gateway in front of every service** — only `transition-service` sits behind
  nginx, because it is the only one a client calls.
- **Zookeeper, Redis or etcd for coordination.** Every lock and every create today
  is a Postgres operation — `pg_try_advisory_lock` for the build claim and the
  per-batch claim, a unique constraint for the idempotency key. None of that needs
  a second system at this scale, and moving a lock out of Postgres would trade a
  transactional guarantee for a network round trip. It earns its place when
  bringing up workers and schedulers across zones needs a lease a database
  transaction cannot span.
- **Multi-region.** Scaling past one region needs quorum consensus, a coordinator
  with a real lease, and geo-aware placement of the worker pool. Nothing here is
  designed for it and nothing here prevents it.

## 9. Known gaps

Things that are true now and would bite at scale, stated plainly.

- **A dead-lettered batch cannot be retried.** Nothing consumes `bulk.move.dlq`, by
  design: a batch that has exhausted its 3 attempts should not be resurrected
  automatically, and the queue's own TTL (7 days) and length cap discard it
  eventually. The consequence is that those records are **in neither the per-record
  failures nor the job's counters — they are simply not moved**, and the only
  durable record is the batch row's `deadLettered` field. Re-submitting the job is
  the only recovery, and a new idempotency key is required.
- **Idempotency keys are never cleared.** A key lives as long as its `bulk_job` row,
  so the unique constraint grows without bound and a key cannot be reused even after
  its job is deleted. A retention window on completed jobs would fix both, and
  interacts with the point above: a job must outlive any retry a client might make.
- **Nothing prunes `bulk_job` or `opportunity_transition`.** Both grow without bound,
  and `opportunity_transition` is the audit trail, so it cannot simply be deleted.
- **Batching's per-page cost has not been separated from contention**, so how much of
  the 18 ms is the walk's own work is unknown. Measured against a quiet table a page
  costs ~7 ms.
- **Batching got about 2x faster when only a timer changed** — per-page mean 41-47 ms
  to 16-31 ms — and the tick is not on the page-write path. Unexplained; the likeliest
  cause is that per-page cost is contention-bound, but the direction is backwards from
  what the new overlap predicts.
- **Queue wait is the largest cost in the pipeline at 177 ms, and the tick does not
  set it** — halving the interval moved it only 195 ms to 177 ms. An earlier 3-run
  reading said it doubled; 5 runs per tick says otherwise, and the two sets of runs
  overlap, so it is not yet attributed.
- **The record count at which waiting stops dominating a small job is not
  re-measured.** The floor is ~125 ms now instead of ~250 ms, so the crossover moved,
  but the smallest filter measured is 5,050 records. No threshold should be quoted
  until the bench runs cases below 1,000.
- **Batching is single-threaded per job** and cannot be otherwise, because each
  page's keyset cursor is the previous page's last row. Only *different* jobs
  parallelise, which is what the per-job advisory claim is for.

## 10. Design decisions

One line each; the sections above are the reasoning.

- **Submission returns before the work starts** — the walk runs in `SnapshotBuilder`
  afterwards, resumed from a cursor.
- **A job row defaults to `preparing`, not `pending`** — a job exists before its
  batches do; the opposite default produced a silent hang.
- **Batches hold ids, not one row per record** — 50 rows of 1,000 uuids.
- **`stage_decided_at` is a logical clock** — newest job wins, no clock sync. See 3.
- **One batching pass per job, enforced by the database** — an advisory lock, so it
  is Postgres's rule rather than a property of there being one replica.
- **Dispatch is an outbox** — batch rows and the intent to send them commit together.
- **The scheduler is in-process, not a broker message** — so an abandoned job is
  picked up by whichever replica is alive.
- **A correlation id is minted at the edge**, validated against
  `^[A-Za-z0-9._:-]{1,64}$` — a newline in a caller-supplied id would let a client
  forge a log line.
- **Five queues: one work, one dead-letter, three retry**, one per backoff step
  (1 s, 5 s, 30 s), because a queue carries one TTL. The backoff waits in the
  broker, so a retrying batch holds no worker slot — which matters at `prefetch: 1`.
- **A dash in a benchmark table** means the instrumentation did not exist for that
  configuration, not that the figure is zero.
