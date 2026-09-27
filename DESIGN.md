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
- **No `OFFSET` in the job path** — it would re-scan and discard every skipped row,
  making the sweep quadratic in the number of batches. The worker does not page at
  all; it reads ids off its batch row.
- **The cursor** is `snapshot_cursor` + `snapshot_cursor_id` on the job row,
  committed **in the same transaction as the batch it follows** — so it can never
  claim progress the data does not have. A cursor ahead of the data would skip
  records on resume; a cursor behind it would duplicate them.
- **A half-built job is finished by the scheduler, not restarted.** If a builder
  dies after 20 of 50 batches, the job is still `preparing`, so the next sweep —
  on whichever replica is alive — claims it, reads `snapshot_cursor`, and walks
  only what is after it. Batches 21-50 are created fresh; batches 1-20 are left
  exactly as they were. Nothing is re-walked and nothing is duplicated.
- **Two details make that safe.** `batch_no` is not reused: on resume it is
  `max(batch_no) + 1` from the rows already written, not a counter held in memory,
  so a restarted builder cannot overwrite a batch. And the per-job advisory lock is
  *session*-scoped, so a killed builder's connection dies and Postgres drops the
  lock immediately — the replacement does not wait out a lease timeout, it just
  finds the key free.
- **The already-written batches are not blocked waiting for the builder.** The relay
  publishes them as soon as it ticks, so a job that dies mid-walk has its first 20
  batches draining normally while the remaining 30 are still being written.
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
- **Where a retry still slips through:** only a different key for the same intent —
  a client minting a fresh uuid per attempt is asking for a second move, and nothing
  can tell that apart from one it meant.
- **What is *not* a hole:** a same-key retry with a changed filter or target is
  rejected 409 rather than silently reusing the old job, and the key's protection
  outlives the job — it is a column on the row, and nothing prunes completed jobs,
  so a key stays reserved indefinitely. There is no DELETE route for a job, so the
  only way to free a key is to delete the workspace, which cascades the tenant away.

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

**Snapshot. The decision is taken once, at submission, and the job is scheduled
against that frozen copy — the request is never consulted again.**

Concretely, `submit()` does three things in order: resolve the filter
(`outcome` → stage ids), canonicalise it, and write the result plus a watermark
onto the `bulk_job` row. The walk then reads `job.filter` and `job.snapshot_at`
off that row. Nothing downstream re-reads the client's request or re-resolves the
outcome.

**Why the filter is snapshotted rather than kept as the request:** the stored form
is already resolved. An `outcome=won` filter is stored as the concrete stage ids it
matched at submit, so renaming a stage or changing its outcome afterwards cannot
change what an in-flight job does. It also means the job row is a complete audit
of what was asked — you never need the original request to explain a job.

**What is snapshotted is the decision, not the ids.** The match set is materialised
lazily, one 1,000-id page at a time, which is what keeps submit O(1) — 50 batch
rows, not 50,000 item rows, which were **81% of the time to the `201`**. A snapshot
of the *ids* at submit is the design that took 32.06 s at 500,000.

**Consequences:**

- **The job is scheduled immediately, and not by a timer.** The row defaults to
  `preparing` at insert, and `submit()` then calls `void this.builder.sweep()` before
  it returns — so the walk starts in the same request that queued the job, measured
  at 10 ms instead of a tick. The sweep timer is the fallback, not the mechanism: it
  exists to finish a job whose walk did not complete.
- **The count is not knowable at submit.** `totalMatched` stays 0 while `preparing`
  and is only right once the walk has counted. Ids are not known at t=0, only the
  filter is.
- **A record can still leave scope after the snapshot.** The filter is frozen but
  *stage membership* is not — a record matched at t=0 may have moved out of a named
  stage by the time its batch is written. Check 4 in § 3 skips it and reports it as
  left-scope rather than dragging it to the target.
- **A record created after submit can never be swept up** — the watermark is
  `created_at <= snapshot_at`, and it is bound once at insert rather than being
  `now()` per query, which would advance mid-walk and re-open the set.

## 5. Isolation, and the hole

- **Composite foreign keys.** Every table carries `workspace_id NOT NULL`; every
  cross-table reference is `(id, workspace_id)`. An opportunity *cannot* point at
  another workspace's stage — the insert fails whatever the service does.
- **Query scoping is the weaker half.** Every repository query filters on
  `workspace_id` explicitly, because a `WHERE` clause is not implied by a foreign
  key. That is convention, not guarantee.
- **The hole:** isolation is enforced per *row*, not per *connection*. One shared
  database and one credential set means one forgotten predicate leaks another
  tenant's rows with no error. No row-level security, and no test can prove the
  absence of a future mistake.

## 6. What breaks at 10×

- **500,000 in one job still completes** — 500,000 moved, 0 failed, 58.71 s.
- **What already broke and is fixed:** submission was super-linear, 0.56 s at 50,000
  became **32.06 s at 500,000**, accumulating every id in memory (50–100 MB). It now
  streams one page at a time. The job total barely moved — 57.34 s before, 58.71 s
  now. The fix took 32 s off the *response*, not off the job.
- **The constraint moves from the drain to batching.** At 50,000, batching is 0.70x
  the work per slot, so the drain is the longer phase. At 500,000 it is **2.11x** —
  55.91 s of batching against 26.49 s of work per slot, with per-page cost degrading
  18 ms → 112 ms. Batching scales in rows; the drain does not.
- **The cause is the working set, not the index.** `shared_buffers` is 128MB and
  `opportunity_workspace_created_id_idx` is 44MB at 50,000 rows, ~17GB at 20M — but a
  job reads one tenant's slice, so the figure that matters is per-tenant.
- **Noisy neighbour is structural, not hypothetical.** The sweep claims
  `ORDER BY created_at LIMIT 10` across all workspaces, so one tenant filling that
  window delays every other tenant, and a single 500,000-record job is 2.11x the
  whole drain by itself. There is no fairness and no `limit_req`.
- **Fix, in order:**
  1. **Fair queueing, ahead of any rate limit.** Claim per workspace round-robin, or
     cap in-flight jobs per workspace. No migration, and it is the only measure that
     touches the measured failure — a few *large* jobs from one tenant. A request-rate
     limit does not: one request can be 500,000 records.
  2. **Rate limit on submit** — a Postgres counter, no new dependency. Meter on row
     creation, not on the idempotency replay path, so a retrying client is not
     charged twice for one job. It cannot be a volume limit: § 4 leaves `totalMatched`
     at 0 until the walk finishes, so records-per-minute has to be metered in the
     builder, which can pause a tenant mid-walk.
  3. **Partition `opportunity` by `workspace_id`**, sub-partitioned by `created_at`.
     Every query in the job path already constrains `workspace_id` as an equality on
     the leading index column, so pruning is exact with no query change, and the
     per-tenant index becomes cache-resident. Hash for bounded partition count; list
     only if tenants churn enough to be worth a partition each for `DETACH`. The PK
     must widen to `(workspace_id, id)` — a partitioned unique constraint must
     contain the partition key — but `opportunity_id_workspace_uniq` already has that
     shape, so the transition FK survives untouched.
  4. **Row-level security in the same change as 3**, because partitioning raises the
     price of the missing predicate § 5 admits to: a forgotten `WHERE workspace_id`
     then returns another tenant's rows *and* scans every partition.
  5. **Replicas.** More builders is the only way to speed up one huge job, because
     page N+1's cursor depends on page N's last row, so a single job cannot be
     parallelised. More workers drain concurrent jobs and need no code change. Both
     are already safe at N: the builder claims per job, the worker per batch, each by
     advisory lock. **The relay is the exception** — no claim, so replicas
     double-publish; correctness survives on the worker's claim, but broker traffic
     and queue depth double with it.
  6. **The ceiling on all of it is connections, not cores.** `PG_POOL_MAX` is 10, and
     12 for the worker, which must stay ≥ its consumer count because `processBatch`
     holds one connection per batch. `max_connections` is 100 and six services already
     hold ~60, so the stack roughly doubles before Postgres refuses. PgBouncer is not
     the way out: session-scoped advisory locks are load-bearing in claims 3 and 5,
     and transaction pooling would hand two workers the same key on two backends.
     Past that the unlock is a lease table with expiry, trading an auto-released lock
     for a claim that can be stranded.
  7. **Then revisit the 1,000 batch size**, which is a guess, not a measurement.
- **What does not break:** the drain — bounded by per-batch work (392 ms) across 12
  slots, independent of workspace size. The outbox is 20,000 rows at 20M
  opportunities, which is nothing.

## 7. Another week, ranked

Ordered by dependency rather than by size: 3 enables 4, and 6 enables 7.

1. **`opportunity.version`** — the one real concurrency gap. `WHERE version = $expected`,
   reject the later save 409. **A bulk job must never bump it**, or a job moving
   1,000 records hands every affected user a 409 about a field they never edited.
2. **Retry a dead-lettered batch.** `bulk.move.dlq` has no consumer by design, so those
   records are in no counter and are **simply not moved** — the batch row's
   `deadLettered` field is the only durable trace. Re-driving the batch is safe: the
   worker's per-batch claim and compare-and-swap make a second attempt a no-op on
   records already moved, and it needs no new idempotency key because the job still
   exists. Must land **before** retention, since the row it re-drives is what 3 prunes.
3. **Retention at 60 days** on `bulk_job`, `bulk_job_outbox` and `bulk_job_failure` —
   all three grow without bound. **Not** `opportunity_transition`: that is the audit
   trail and has to outlive the job row it describes. Bounds the tables and makes 4
   possible; 60 days is far longer than any client retry window.
4. **Clear the idempotency key once a job is past retention**, closing the § 8 gap.
   Today a key is reserved for the life of its row and nothing ever frees it, so the
   unique index grows without bound and a key can never be reused.
5. **Redis, for two of the three things it is wanted for.** A token bucket for the
   submit rate limit, and replacing the session-scoped advisory locks so the design
   survives connection pooling — both are genuinely better off-box, and the second is
   what would make PgBouncer usable at all. **Not the idempotency key**:
   `UNIQUE (workspace_id, idempotency_key)` is transactional and durable where a Redis
   key is neither, and Redis cannot commit atomically with the insert — so the
   constraint has to stay regardless, and the lookup gets faster by roughly nothing
   against a 13–22 ms submit.
6. **ZooKeeper or etcd for worker and scheduler membership — one of them, not both.**
   Both give ephemeral leases and watches, which is exactly what "bring up a worker as
   required" needs and what a database transaction cannot span. **etcd** is the cheaper
   operational bet: a single static binary, gRPC, and the same library Kubernetes
   already ships, so the tooling exists. **ZooKeeper** is heavier — a JVM ensemble and
   ZAB — but has more years of very-large-cluster evidence behind it. Register on
   start, deregister on shutdown, and let builders and relays watch for peers instead
   of waking on a fixed interval.
7. **Multi-region, which needs 6 first.** Quorum consensus, the coordination service,
   and geo-aware placement of the worker pool. One region today and nothing here
   prevents more. It also opens a question this design does not answer: a bulk move
   that crosses a border is a data-residency problem, not a latency one.

## 8. Known gaps

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

## 9. Design decisions

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
