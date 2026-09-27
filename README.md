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
| Jest + ts-jest | 215 tests across 19 suites |
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
| `npm run test` | 215 tests, in band |
| `npm run test:safety` / `:endpoints` / `:happyflow` | one project |
| `npm run bench` | the 50,000 record benchmark — see `BENCHMARKS.md` |
| `npx tsx test/happyflow/filter-bench.ts` | filter selectivity against the build |
| `npm run lint` / `npm run format` | |

## The services

**Ten containers. Six of them are application services, all started together.**

| container | role | port |
|---|---|---|
| `workspace-service` | tenants | 3001 |
| `user-service` | owners | 3002 |
| `stage-service` | pipelines and permitted moves | 3003 |
| `opportunity-service` | deals, their stages, the list and filter | 3004 |
| `transition-service` | **bulk jobs**: submit, build the snapshot, status | 3005 |
| `worker-service` | applies batches; **no port** — it only consumes | — |
| `nginx` | the edge; fronts transition-service only | 8080 |
| `postgres` | the datastore | 5432 |
| `rabbitmq` | batch dispatch | 5672, UI 15672 |
| `migrate` | applies migrations, then exits | — |

All six application services come up at once, plus the two datastores and the
edge; `migrate` runs to completion before any of them start. Every service has
its own connection pool (`PG_POOL_MAX`, per service rather than shared), and the
`workers × PG_POOL_MAX` product is kept under Postgres' 100-connection limit.

Only **transition-service** sits behind nginx. The other four keep direct ports,
because the brief asks for the service boundaries rather than a gateway. Tenant
scoping is a required `X-Workspace-Id` header.

## A bulk move, end to end

```
POST /bulk-moves ──► transition-service writes one job row, returns 201 + jobId
                     (~9 ms; no batch exists yet)

SnapshotBuilder      a 250 ms sweep finds jobs in 'preparing' and walks each
                     filter into batches of 1,000, committing a batch and the
                     cursor that follows it in one transaction

OutboxRelay          a 250 ms timer publishes unpublished batch rows to RabbitMQ

BatchWorker          12 consumers, one channel each, apply batches

GET  /bulk-moves/:id progress, batches by state, failures, dead letters
```

A submitted job is dispatched in batches of 1,000 records. Each batch applies in
one transaction, and the worker re-reads each record's live stage before moving
it, so a record someone edited by hand in the meantime is left alone.

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

**One builder per job, enforced by the database.** A job's pages are keyset-
paginated, so page N+1's cursor is page N's last row and a job's walk cannot be
parallelised at all. Different jobs are independent, so `SnapshotBuilder` takes a
non-blocking advisory lock per job before walking it. Without the lock a second
replica is *correct* and wasteful — every page read and written twice, scaling
with replica count. The lock makes "one walker per job" something Postgres
enforces rather than a property of there happening to be one replica.

**Dispatch is an outbox, so the batch rows and the intent to send them commit
together.** A row is marked published only after the broker confirms, so a crash
between the two re-publishes rather than loses. Delivery is at-least-once; the
worker's `pending → running` claim is a compare-and-set, which makes processing
effectively-once. Duplicate batches are harmless; a lost one would hang the job
at pending for ever.

**Twelve consumers, because it was measured.** Per-batch time is flat at 0.41 s
on 4 consumers and 0.44 s on 12, while throughput rises 2.8×. The batches do not
get slower, more run at once. It is a tuning value matched to this host's 12
cores and would need re-measuring elsewhere.

**A correlation id is minted at the edge and carried on every log line.** One
`--> / <--` pair per request in every service, and the job's id stored on the job
row so the build can be traced back to the request that caused it. Validated
against `^[A-Za-z0-9._:-]{1,64}$` at both the edge and the service, because a
caller-supplied id ends up in log lines and a newline would let a client forge
one.

## Left for later

**Redis — considered, added, then removed.** The state is either durable or
per-process, with nothing in between. Job status and cursors must survive a
restart, so they belong in Postgres; the correlation id is request-scoped and
belongs in `AsyncLocalStorage`, which needs no infrastructure. Caching the
snapshot build would make it *less* correct, not faster: the cursor and the batch
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

**nginx does not log the correlation id.** It mints one and forwards it, but has
no `log_format`, so the hop that *starts* every trace is the one hop missing from
it. Two lines.

**The build is the critical path.** It is ~90% of a job's wall clock on one
thread, and it is within 1% of the drain's length — so shortening either alone
moves the total very little, because the other immediately becomes the
constraint. Its per-page cost has not yet been separated from contention with the
workers running alongside it.

**The drain residue is unattributed.** Removing 50,000 row inserts should have
made the drain faster; it got slower. Pool starvation is ruled out by
measurement — 7–9 ms mean wait against 422–565 ms of work, with `waitingCount` 0
throughout. The residue is inside the per-batch Postgres work, and the two
candidates are untested. See `BENCHMARKS.md`.

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
docker compose logs -f bsm-worker     # one line per batch: pool wait, work time, slot
docker compose logs -f bsm-transition # the build, per page, and the relay
```

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
removed** — the `stage_decided_at` trigger's two `WHEN` clauses, the build's
advisory claim, the worker's compare-and-swap. That is checked, not assumed.
