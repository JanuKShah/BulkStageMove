# BulkStageMove

Bulk stage move for Opportunities — SDE-3 backend take-home.

## The problem

A user selects a filter — stage, owner, status, value range, date range — and
moves up to **50,000** matching opportunities to a different stage in one action.
The API returns immediately with a job handle. The work happens in the
background. The user polls for progress.

It sounds like a loop. It is not. At volume it is a background job that must be
**idempotent**, **resumable**, and polite to everything running alongside it —
and it has to stay correct while individual users are editing the same records
by hand.

## Domain model

A **workspace** is a tenant. All data is scoped to one workspace and never
crosses.

- A workspace has a **pipeline** with an ordered list of **stages**
  (e.g. New Lead → Contacted → Qualified → Proposal Sent → Negotiation →
  Closed Won).
- An **opportunity** is a deal. It sits in exactly one stage at a time and
  carries at least: a name, a monetary value, a status
  (open / won / lost / abandoned), an owner, and created/updated timestamps.
- Every stage change is recorded as a **transition**.
- `opportunity.stage_decided_at` is a **logical clock** — see *Design decisions*.

## Technology

| | |
|---|---|
| Node.js 22 | runtime |
| TypeScript 5.9 | `strict`, `noUncheckedIndexedAccess` |
| NestJS 11 + Express | HTTP framework |
| PostgreSQL 16 | the only datastore — no cache, no second store |
| `pg` 8 | driver, raw SQL, no ORM |
| RabbitMQ 4 + `amqplib` 2 | batch dispatch between services |
| nginx | the edge, mints the correlation id |
| Jest + ts-jest | 216 tests across 19 suites |
| tsx | runs the migrate, seed, cli and benchmark scripts |
| ESLint + Prettier | lint and format |
| Docker Compose | 10 containers |

Deliberately absent: **Redis**. See *Left for later*.

## Running it

```bash
npm run verify     # up + seed + dump + test, the one command
```

`npm run up` is `docker compose up -d --build --wait` — it starts everything, runs
migrations, and blocks until healthy. `npm run down` tears the stack down and
**deletes the database volume**.

| command | |
|---|---|
| `npm run up` / `npm run down` | start / stop, with a fresh volume |
| `npm run seed` | seed a workspace and print its id |
| `npm run seed:large` | the 500,000 record dataset |
| `npm run test` | 216 tests, in band |
| `npm run test:safety` / `:endpoints` / `:happyflow` | one project |
| `npm run bench` | the 50,000 record benchmark — see `BENCHMARKS.md` |
| `npx tsx test/happyflow/filter-bench.ts` | filter selectivity against batching |
| `npm run lint` / `npm run format` | |

## The services

**Ten containers, all started together. Six of them are application services, and
they are single-process each — the concurrency is inside them, not in how many of
them there are.**

| container | role | port | kept up at once |
|---|---|---|---|
| `workspace-service` | tenants | 3001 | 1 process, pool of 10 |
| `user-service` | owners | 3002 | 1 process, pool of 10 |
| `stage-service` | pipelines and permitted moves | 3003 | 1 process, pool of 10 |
| `opportunity-service` | deals, their stages, the list and filter | 3004 | 1 process, pool of 10 |
| `transition-service` | **bulk jobs**: submit, batching, status | 3005 | 1 process, pool of 10, **2 background loops** |
| `worker-service` | applies batches; **no port** — it only consumes | — | 1 process, **12 consumers**, pool of 12 |
| `nginx` | the edge; fronts transition-service only | 8080 | 1 |
| `postgres` | the datastore | 5432 | 1 |
| `rabbitmq` | batch dispatch | 5672, UI 15672 | 1 exchange, **5 queues** |
| `migrate` | applies migrations, then exits | — | runs once at startup, not kept up |

**The three loops that do the work**, all of them on 125 ms timers inside
`transition-service` and `worker-service`:

| loop | where | every | up to | parallelism |
|---|---|---|---|---|
| `SnapshotBuilder` | transition-service | 125 ms | 25 jobs claimed per tick | **one job at a time**, and always one walker per job |
| `OutboxRelay` | transition-service | 125 ms | 50 unpublished batch rows per tick | one at a time |
| `BatchWorker` | worker-service | message-driven | — | **12 slots**, one channel each |

Batching is the one that cannot be widened. A page's keyset cursor is the
previous page's last row, so page 2 cannot be asked for before page 1 returns —
one job, one walker, always. What *can* run in parallel is different jobs, which
is what the advisory lock is for: with three replicas, three jobs batch at once
and no two batching passes touch the same one. Twelve workers is the widest thing in the
system and it is deliberately matched to this host's 12 cores.

Every service has its **own** pool, per service rather than shared: five pools of
10 plus the worker's 12 is **62 connections at most**, against Postgres' 100
limit. That arithmetic is why the worker gets 12 and the rest get 10 — the other
five services are request/response and never need more than a handful at a time.

The five queues are one work queue, one dead-letter queue, and **three retry
queues, one per backoff step** (1 s, 5 s, 30 s). A queue carries exactly one TTL
and dead-letters on expiry, so three delays means three queues — and the message
waits out its backoff in the broker, not in a sleeping consumer, so a retrying
batch holds no worker slot. That is the difference between 12 slots and 12 slots
that are sometimes just waiting.

Only **transition-service** sits behind nginx. The other four keep direct ports,
because the brief asks for the service boundaries rather than a gateway. Tenant
scoping is a required `X-Workspace-Id` header.

## The main logic

Five steps, and after step 1 the job row is the only thing that exists — every
later step reads it or the rows it produces, and nothing is held in memory.

| # | step | what it does | the part that is not obvious |
|---|---|---|---|
| 1 | **Submit** | Resolves the filter to a list of stage ids, writes **one** job row with `snapshot_at = now()`, returns `201` + `jobId` in ~9 ms | No batch exists yet, so the row defaults to `preparing` — the state a job with no batches is actually in, and the state the sweep looks for |
| 2 | **Batching** | A 125 ms sweep claims a job with a Postgres advisory lock, then walks the filter keyset-style, 1,000 ids per page | Each page commits its batch **and** the cursor that follows it in one transaction, so a crash resumes instead of restarting — and the advisory lock makes "one batching pass per job" something Postgres enforces, not a property of there being one replica |
| 3 | **Relay** | A 125 ms timer publishes unpublished batch rows to RabbitMQ | A row is marked published only *after* the broker confirms, so a crash in between re-publishes rather than loses the batch |
| 4 | **Drain** | 12 consumers take a per-batch advisory lock, claim the batch `pending → running` by compare-and-set, then apply it in one transaction: move the records, insert one transition each, write back the counts | Those two database guards are what turn at-least-once delivery into effectively-once — see below. A duplicate batch is harmless; a lost one hangs the job for ever |
| 5 | **Status** | `GET /bulk-moves/:id` — progress, batches by state, failures, dead letters | A dead-lettered message goes to a dead-letter queue and **the rest of the job keeps draining** |

**A record is moved only if it survives all six checks, in this order:**

| # | check | if it fires |
|---|---|---|
| 1 | the record still exists | **fail** — deleted since the batch was built |
| 2 | it is **not already in the target stage** | count as **moved** |
| 3 | `record.stage_decided_at <= job.snapshot_at` | **skip** — somebody decided after this job was submitted |
| 4 | it is still in a stage the filter names | **skip** — it left scope |
| 5 | its current stage has a permitted transition to the target | **fail** — no permitted move |
| 6 | — | **move it**, and insert one transition |

Check 2 must stay ahead of check 3, and the reason is the mirror image of the
obvious one. A person who moves a record *to* the target has stamped it with a
time newer than the job's, so testing the clock first would report that as a
skip — and under-report a job that in fact achieved its intent. Check 2 also
makes a retry idempotent: a record the previous attempt already moved is counted
as moved, not as a failure, so a batch that dies halfway does not turn every
record it finished into a failure on the second attempt.

Two guards sit in front of all of that, and they cover different accidents:

- **A per-batch advisory lock**, taken first. If another consumer already holds
  this batch, the second one is refused outright and returns without touching a
  row. This is the only thing that catches a *concurrent* duplicate, and it has
  to come first — two consumers holding the same message both read `pending`, so
  both would pass a status check.
- **A `pending → running` compare-and-set**, taken second. A batch that already
  committed is `completed` and matches nothing, so a redelivery that arrives
  *after* the first attempt finished claims no rows and does nothing.

Both are in the database, not in process memory, because a second replica has to
be excluded too. Neither is a unique index: there is deliberately no constraint
on `(job_id, opportunity_id)`, and `0001_schema.sql` records why one is not
needed and what would have to change to bring it back.

## Design decisions

**Submission returns before the work starts.** `POST` writes the job row and
returns in single-digit milliseconds. The walk that turns a filter into batches
runs afterwards in `SnapshotBuilder`, resumed from a cursor if it dies. At
500,000 records the old inline walk was super-linear — 0.56 s at 50,000 became
32.06 s at 500,000 — because it accumulated every batch's ids in memory first.
Streaming fixes the shape, not just the constant.

**A job row is 'preparing' by default, not 'pending'.** A job is created before
its batches exist, so that is the state it is actually in, and it is the state
the sweep looks for. The opposite default produced a silent hang: a job inserted
without an explicit status sat in 'pending' with no batches, nothing to publish,
and nothing to notice it had stalled. Getting it wrong now means a job gets
built that should not have been, which is recoverable.

**Batches hold record ids, not one row per record.** The job writes 50 batch rows
carrying 1,000 uuids each, not 50,000 item rows. The old shape was 81% of the
time to the `201` and needed four indexes and three composite foreign keys per
record. What it bought back — the ability to skip a record someone moved by hand
— is kept, by re-reading live state in the worker rather than by storing a
membership table.

**`stage_decided_at` is a logical clock, not a timestamp of change.** A person's
edit stamps the wall clock. A bulk job stamps **its own submission time**. The
worker's rule is then `skip if record.stage_decided_at > job.snapshot_at`, which
compares *when jobs were submitted* rather than when rows were written — so the
newest job wins no matter which one finishes first. Writing the wall clock would
make it last-writer-wins, and an older job still draining would beat a newer one.
Two properties fall out: a person always wins, at either side of the submission;
and a job never skips its own work, because `J.at > J.at` is false, so a retry is
never blocked by its first attempt.

It needs **no synchronised clocks**, because every timestamp in the comparison
comes from the one Postgres — the job's `snapshot_at` is `now()` at insert, the
trigger's `now()` is the same clock, and the worker reads its stamp from its own
row rather than from its own system time.

**One batching pass per job, enforced by the database.** A job's pages are keyset-
paginated, so page N+1's cursor is page N's last row and a job's walk cannot be
parallelised at all. Different jobs are independent, so `SnapshotBuilder` takes a
non-blocking advisory lock per job before walking it. Without the lock a second
replica is *correct* and wasteful — every page read and written twice, scaling
with replica count. The lock makes "one walker per job" something Postgres
enforces rather than a property of there happening to be one replica.

**Dispatch is an outbox, so the batch rows and the intent to send them commit
together.** A row is marked published only after the broker confirms, so a crash
between the two re-publishes rather than loses. Delivery is at-least-once, and
the worker's two database guards — a per-batch advisory lock, then a
`pending → running` compare-and-set — make processing effectively-once.
Duplicate batches are harmless; a lost one would hang the job at pending for
ever.

**A correlation id is minted at the edge and carried on every log line.** One
`--> / <--` pair per request in every service, and the job's id stored on the job
row so the batching can be traced back to the request that caused it. Validated
against `^[A-Za-z0-9._:-]{1,64}$` at both the edge and the service, because a
caller-supplied id ends up in log lines and a newline would let a client forge
one.

## Left for later

**Redis — considered, added, then removed.** The state is either durable or
per-process, with nothing in between. Job status and cursors must survive a
restart, so they belong in Postgres; the correlation id is request-scoped and
belongs in `AsyncLocalStorage`, which needs no infrastructure. Caching the
snapshot batching would make it *less* correct, not faster: the cursor and the batch
data commit in one transaction, and splitting them across two stores turns an
atomic operation into a distributed one. It would earn its place for leader
election at high replica counts, or for a read path `EXPLAIN` shows hammering
Postgres. Neither is true yet.

**`opportunity.version` — the one concurrency gap left open.** `stage_decided_at`
covers job-versus-record. Two people editing the same deal is the other half, and
it is not covered: the second save silently wins. The fix is a
`version integer` with `WHERE version = $expected` on the single-record move,
rejecting the **later** save with a 409. The bulk job must never bump it — a job
moving 1,000 records would bump 1,000 versions and hand every user with one of
them open a 409 about a field they never edited — so the two mechanisms have to
stay independent. Not built: there is no UI, so it is only observable when two
humans edit one record at once.

**No rate limit on submit.** Nothing bounds how many jobs one tenant can create,
and with async submission that is now cheap to spam. Would be a Postgres
counter before a new dependency.

**Small jobs are bound by the poll intervals, not by their data.** A job waits a
tick before the first batch exists, then a tick before any of it is published. At
the original 250 ms, a filter matching 5,050 records settled in 390–470 ms and one
matching 5,065 in 386–407 ms — indistinguishable, and both carrying roughly half a
second of pure waiting. Halving both ticks to 125 ms took those to **335–341 ms**
and **275–336 ms**, a consistent ~100 ms across three runs, and the 50,000-record
job from 1.82–2.72 s to 1.52–2.37 s.

That is a mitigation, not a fix. The wait is now ~125 ms of expectation rather
than ~250 ms, and it is still there: a tick is a tick, and halving the interval
halves the wait rather than removing it. Removing it means kicking the builder
from `POST /bulk-moves` and kicking the relay after each page commits, leaving the
timers as the recovery path for a crashed replica. Not built.

It was not free. Queue wait roughly doubled — mean 116–280 ms against 21–144 ms —
because publishing twice as often delivers a heavier stream to twelve consumers.
The job a caller waits for got faster while internal queueing got worse.

## Exploring it

There is no UI, so two commands cover it. Both need the stack up.

**CLI — call the API by hand.** Use it to try a filter, move an opportunity, submit a bulk
move, or read a transition history.

```bash
npm run cli                                       # interactive menu
npm run cli -- opportunities --workspace=<uuid> --outcome=won --limit=20
npm run cli -- bulk-move --workspace=<uuid> --to=<stageId> --outcome=won
npm run cli -- job-status --workspace=<uuid> --id=<jobId>
```

A workspace id is required. `npm run seed` prints them; there is no endpoint that lists
workspaces, because a caller must not be able to enumerate other tenants.

**Dump — write the database to a file.** Use it to check by eye that a bulk move or a
filter touched exactly the rows you expected.

```bash
npm run dump                                      # every row -> db-state.txt
npm run cli -- dump-db --sample=100               # capped, for the 500k dataset
npm run cli -- dump-db --out=before.txt           # then --out=after.txt and diff
```

It reads Postgres directly, not the API, so it reflects what is actually stored.

**Watch a job run.**

```bash
docker compose logs -f worker-service     # one line per batch: wait, work, pool pressure
docker compose logs -f transition-service # batching, per page, and the relay
```

Service names, not the `bsm-` container names — `docker compose logs` takes the
former and fails on the latter.

## Tests

| suite | what it protects |
|---|---|
| `safety` | one test per safety mechanism — the ones that go red if a guard, constraint or trigger clause is removed |
| `endpoints` | every route and its failure paths |
| `happyflow` | the whole brief at full scale: one 50,000 record job end to end through the broker |

No test asserts a timing. Wall-clock assertions fail on a loaded machine and
pass on a fast one; the numbers live in `BENCHMARKS.md` and the measurement lives
in `benchmark.ts`.

Several safety tests are written to **fail when the thing they protect is
removed** — the `stage_decided_at` trigger's two `WHEN` clauses, batching's
advisory claim, the worker's compare-and-swap. That is checked, not assumed.
