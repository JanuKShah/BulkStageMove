# Design

How a bulk stage move is chunked, made idempotent, kept correct while people edit the
same records, and isolated. Numbers are in `BENCHMARKS.md`; the tests guarding each
claim are in `TESTSTRATEGY.md`.

## 0. The shape of it

![Design diagram](docs/design.png)

After step 1 the job row is the only thing that exists; nothing is held in memory.

| # | step | what it does | the part that is not obvious |
|---|---|---|---|
| 1 | **Submit** | Resolves the filter to stage ids, writes **one** job row with `snapshot_at = now()`, returns `201` in ~13 ms | No batch exists yet, so the row defaults to `preparing`, the state a job with no batches is actually in |
| 2 | **Batching** | A 125 ms sweep claims the job with an advisory lock, walks the filter keyset-style, 1,000 ids per page | Each page commits its batch **and** the cursor after it, in one transaction |
| 3 | **Relay** | A 125 ms timer publishes unpublished batch rows | Marked published only *after* the broker confirms, so a crash re-publishes rather than loses |
| 4 | **Drain** | 12 consumers take a per-batch lock, claim `pending → running`, apply in one transaction | Those two guards make at-least-once effectively-once, see 3 |
| 5 | **Status** | `GET /bulk-moves/:id`, progress, batches, failures, dead letters | A dead-lettered message does not stop the rest of the job |

## 1. Chunking, and surviving a restart

- **Submit writes one row:** `now()`, status `preparing`.
- **Index** `(workspace_id, created_at, id)`, so a page is an index range, not a scan.
- **The sweep** every 125 ms takes `preparing` jobs, reads the last cursor, writes the next 1,000 ids as one batch, until nothing matches.
- **The cursor is stored after every batch, in the same transaction as the batch.** Ahead skips records, behind duplicates them.
- **A half-built job resumes, it does not restart.** Still `preparing`, so the next sweep carries on from the cursor. Written batches keep draining.
- **No `OFFSET` here.** It re-reads every row it skips. The worker does not page at all.

## 2. Idempotency

- **Key:** caller-supplied, on the job row. `UNIQUE (workspace_id, idempotency_key)`.
- **Submit looks the key up.** No match: insert. Match: insert nothing.
- **Then compare** the stored job's target stage and filter against the request.
- **Both match:** a retry. Return the same `jobId`, create nothing.
- **Either differs:** the key was reused for something else. **409**, change nothing.
- **The filter is compared resolved.** `outcome=won` becomes stage ids first, so two spellings of one request match.
- **Keys are never freed.** A column on the row, and there is no delete route.
- **What this does not catch:** the same intent sent under a different key. A client that generates a fresh uuid per attempt gets a new job each time, and nothing here can tell that apart from a deliberate re-run.

## 3. Concurrency on one opportunity

Six checks, fixed order. The order is load-bearing.

| # | check | outcome |
|---|---|---|
| 1 | the record still exists | fail |
| 2 | **is already in the target stage** | count as **moved** |
| 3 | `record.stage_decided_at <= job.snapshot_at` | **skip** |
| 4 | still in a stage the filter names | **skip** |
| 5 | a permitted transition to the target exists | fail |
| 6 | not a check, the action itself | **move**, insert one transition |

- **Check 2 must precede 3.** A person who moved a record to the target stamped it newer than the job, so testing the clock first under-counts a job that achieved its intent, and breaks retry idempotency.
- **A manual move and the job on one record, the person wins.** Before the watermark the job takes the new stage as its start; after it the record is skipped and the shortfall reported.
- **`stage_decided_at` is a logical clock.** A person stamps the wall clock, a job stamps its own `snapshot_at`, so the comparison is submission order. Newest job wins however long the older takes, and every timestamp comes from the one Postgres, so no clock sync.
- **Two `WHEN` clauses on the trigger, both load-bearing.** `OLD.stage_id IS DISTINCT FROM NEW.stage_id`, so a rename cannot drop a record from an in-flight job. `NEW.stage_decided_at IS NOT DISTINCT FROM OLD.stage_decided_at` is the opt-out: the worker sets the column, so the trigger stands down. Without it a job would stamp 1,000 rows with the wall clock and defeat the design.

## 4. Snapshot vs live

- **The snapshot is one timestamp,** `snapshot_at`, written when the job row is created. The filter is resolved and stored on the same row, so the request is never read again.
- **Batches are built immediately,** not on a timer. `submit()` calls the sweep before returning, and the 125 ms sweep is the fallback.
- **A record can fall out of the filter before its batch is built.** The filter is frozen but stage membership is not, so a record that matched at submit may have left by the time the walk reaches it. Check 4 skips it.
- **Once a batch exists its membership is fixed.** It can be processed much later and still run, as long as the record's own timestamp allows it: `stage_decided_at <= snapshot_at`. Newer than that means someone decided after we submitted, so we leave it alone.
- **No backpressure at this volume.** One tenant and a small table means nothing limits how much work is in flight. With multiple tenants, and throttling needed per job and per workspace, this is where it will bite.

## 5. Isolation, and the hole

- **Composite foreign keys.** Every table carries `workspace_id NOT NULL`; every
  cross-table reference is `(id, workspace_id)`. An opportunity *cannot* point at
  another workspace's stage, the insert fails whatever the service does.
- **Query scoping is the weaker half.** Every repository query filters on
  `workspace_id` explicitly, because a `WHERE` clause is not implied by a foreign
  key. Convention, not guarantee.
- **The hole:** isolation is per *row*, not per *connection*. One database and one
  credential set means one forgotten predicate leaks another tenant's rows with
  no error. No row-level security, and no test can prove the absence of a future
  mistake.

## 6. What breaks at 10x

- **500,000 in one job still completes**, 500,000 moved, 0 failed, 58.71 s.

**Already broke, now fixed:**

1. We used to store every job item individually. At 500,000 records that was 500,000 rows and 500,000 database round trips. We fixed it: we now only create batch rows holding the ids to be processed, so 50 rows instead of 500,000.
2. We also used to change opportunities one at a time, which was another 500,000 database calls at the worker. A batch of 1,000 records is now one commit, so 500 calls instead of 500,000.

**Open issues:**

1. **Batching does not scale.** The scheduler is linear and creates batches one after another, so with more data it takes a long time. Workers sit ready with nothing to pick up, because the batches do not exist yet.
2. **The cause is the working set, not the index.** The index is about 17GB at 20M rows, but a single job only reads one tenant's slice of it.
3. **Noisy neighbour is structural.** The sweep claims `ORDER BY created_at LIMIT 25` across all workspaces, so one tenant filling that window delays everyone, and one 500,000-record job is 2.11x the whole drain. No fairness, no limit.
4. **Adding servers breaks on connections, not cores.** `PG_POOL_MAX` is 10, and 12 for the worker, which must be at least its consumer count because `processBatch` holds a connection for the whole batch. `max_connections` is 100, the Postgres default, and six services already hold about 62. The session-scoped lock is what makes it linear: it cannot be taken through the pool, so a connection is pinned per consumer. A pooled design would add capacity per replica, this one adds connections.

**What to fix:**

1. **Fair queueing first.** Round-robin per workspace, or cap in-flight jobs per tenant. No migration needed, and it is the only thing that touches a few very large jobs from one tenant.
2. **Rate limit on submit,** metered in the builder on rows created, not on the request. A request is one row even when it moves 500,000 records.
3. **More builders.** The only way to speed up one huge job, because page N+1's cursor is page N's last row. More workers drain concurrent jobs, no code change.
4. **Partition `opportunity` by `workspace_id`,** sub-partitioned by `created_at`. Every job-path query already filters on `workspace_id`, which is the leading index column, so pruning is exact. Row-level security goes in with it, or the missing predicate becomes a full-partition scan.
5. **Raise `max_connections`,** or use a lease table with expiry. PgBouncer does not help, because transaction pooling can hand two workers the same key on two backends.
6. **Measure the 1,000 batch size.** It is a guess, not a measurement.

## 7. Another week, ranked

1. **`opportunity.version`,** to solve the race between two parallel UI changes on one opportunity. Reject the later save with 409. A bulk job must never bump it, or a job moving 1,000 records hands every affected user a 409 about a field they never edited.
2. **Retention at 60 days** on `bulk_job`, `bulk_job_outbox` and `bulk_job_failure`, and clear the idempotency key past that. A key is reserved for the life of its row and nothing ever frees it, so the unique index grows without bound and a key can never be reused.
3. **Redis,** for two of the three things it is wanted for: a token bucket for the rate limit, and replacing the session-scoped advisory locks so the design survives connection pooling, which is what would make PgBouncer usable. Not the idempotency key, the unique constraint is transactional and durable where a Redis key is neither.
4. **ZooKeeper or etcd** for worker and service membership, one and not both. Both give ephemeral leases and watches, which is what "bring up a worker as required" needs and what a database transaction cannot span. etcd is the cheaper bet, a static binary and the library Kubernetes already ships. Register on start, deregister on shutdown, watch for peers instead of waking on a timer.
5. **Sharding and partitioning** the database by `workspace_id`, so data can be scoped to one tenant and then distributed.
6. **A load balancer,** so the microservices can be managed behind it.
7. **Geo-located multi-region servers,** for low latency and to survive a data centre crash. Needs 4 first, because a region needs a membership service. A bulk move crossing a border is a data-residency problem, not a latency one.
8. **A retry mechanism and a DLQ for failed batches.** Today a given-up batch is only marked in the database, and it starts again when a caller asks through `POST /bulk-moves/:id/retry-failed`.
9. **Rate limits at workspace, user and job level,** not only per request.
10. **More RabbitMQ queues, partitioned by `workspace_id`.** Every worker subscribes to every queue but treats one as its priority, so a noisy tenant cannot delay another, and a worker with spare capacity can still pick up load from a single tenant.

## 9. Design decisions

One line each; the sections above are the reasoning.

- **Submission returns before the work starts**, the walk runs in
  `SnapshotBuilder` afterwards, resumed from a cursor.
- **A job row defaults to `preparing`, not `pending`**, a job exists before its
  batches do; the opposite default produced a silent hang.
- **Batches hold ids, not one row per record**, 50 rows of 1,000 uuids.
- **`stage_decided_at` is a logical clock**, newest job wins, no clock sync. See 3.
- **One batching pass per job, enforced by the database**, an advisory lock, so
  it is Postgres's rule, not a property of there being one replica.
- **Dispatch is an outbox**, batch rows and the intent to send them commit
  together.
- **The scheduler is in-process, not a broker message**, so an abandoned job is
  picked up by whichever replica is alive.
- **One exchange and one queue.** No dead letter queue and no retry queues: a
  batch that runs out of attempts is marked `failed` in `bulk_job_outbox` with its
  reason, plus a `bulk_job_failure` row per record, and the message is acked.
