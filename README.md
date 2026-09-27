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

**Ten containers, all started together. Six are application services, single-process each — the concurrency is inside them, not in how many there are.**

| container | role | port | kept up at once |
|---|---|---|---|
| `workspace-service` | tenants | 3001 | 1 process, **DB pool of 10** |
| `user-service` | owners | 3002 | 1 process, **DB pool of 10** |
| `stage-service` | pipelines and permitted moves | 3003 | 1 process, **DB pool of 10** |
| `opportunity-service` | deals, their stages, the list and filter | 3004 | 1 process, **DB pool of 10** |
| `transition-service` | **bulk jobs**: submit, batching, status | 3005 | 1 process, **DB pool of 10**, **2 background loops** |
| `worker-service` | applies batches; **no port** — it only consumes | — | 1 process, **12 consumers**, **DB pool of 12** |
| `nginx` | the edge; fronts transition-service only | 8080 | 1 |
| `postgres` | the datastore | 5432 | 1 |
| `rabbitmq` | batch dispatch | 5672, UI 15672 | 1 exchange, **5 queues** |
| `migrate` | applies migrations, then exits | — | runs once at startup |

**The three loops that do the work:**

| loop | where | every | up to | parallelism |
|---|---|---|---|---|
| `SnapshotBuilder` | transition-service | 125 ms | 25 jobs claimed | **one job at a time**, one walker per job |
| `OutboxRelay` | transition-service | 125 ms | 50 unpublished rows | one at a time |
| `BatchWorker` | worker-service | message-driven | — | **12 slots**, one channel each |

**Batching cannot be widened** — a page's keyset cursor is the previous page's last row — but different jobs can run in parallel, which is what the advisory lock is for. Twelve workers is the widest thing here, matched to this host's 12 cores.

**"DB pool of N" is a ceiling on Postgres connections** for that process (`PG_POOL_MAX` becomes `pg`'s `max`), opened on demand — the idle stack held 8 while this was written. Five pools of 10 plus the worker's 12 is **62 against Postgres' 100** `max_connections`, which is why the worker gets 12 and the rest 10. The ceiling is per process, so it multiplies by replica count: three worker replicas put the stack at 86, and a fourth breaches the limit.

**Five queues: one work, one dead-letter, three retry** — one per backoff step (1 s, 5 s, 30 s), because a queue carries one TTL. The backoff waits in the broker, not in a consumer, so a retrying batch holds no worker slot.

Only **transition-service** sits behind nginx; the other four keep direct ports, since the brief asks for the boundaries rather than a gateway. Tenant scoping is a required `X-Workspace-Id` header.

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

**Check 2 must stay ahead of check 3** — the mirror of the obvious order. A person who moves a record *to* the target has stamped it newer than the job, so testing the clock first would report a skip and under-count a job that achieved its intent. Check 2 also makes a retry idempotent, so a batch that dies halfway does not turn every record it finished into a failure.

**Two guards sit in front of all six checks, and they cover different accidents:**

- **A per-batch advisory lock, taken first.** Two consumers holding the same message both read `pending`, so only the lock catches a *concurrent* duplicate; the second is refused without touching a row.
- **A `pending → running` compare-and-set, taken second.** A batch that already committed is `completed` and matches nothing, so a redelivery after the fact claims no rows.

Both are in the database, not process memory, so a second replica is excluded too. Neither is a unique index — `0001_schema.sql` records why one is not needed on `(job_id, opportunity_id)`.

## Design decisions

**Submission returns before the work starts.** The walk that turns a filter into batches runs in `SnapshotBuilder` after the response, resumed from a cursor, because the old inline walk was super-linear — 0.56 s at 50,000 became 32.06 s at 500,000.

**A job row is 'preparing' by default, not 'pending'.** A job exists before its batches do, so that is the state it is actually in; the opposite default produced a silent hang.

**Batches hold record ids, not one row per record.** 50 rows of 1,000 uuids instead of 50,000 item rows, which were 81% of the time to the `201`.

**`stage_decided_at` is a logical clock, not a timestamp of change.** A person stamps the wall clock, a job stamps its own submission time, so the newest job wins however long the older one takes.

**It needs no synchronised clocks,** because every timestamp in that comparison comes from the one Postgres — the job's `snapshot_at` and the trigger's `now()` are the same clock.

**One batching pass per job, enforced by the database.** A page's keyset cursor is the previous page's last row, so a job cannot be walked twice at once; the advisory lock makes that Postgres's rule rather than a property of there being one replica.

**Dispatch is an outbox, so the batch rows and the intent to send them commit together,** which makes delivery at-least-once and lets the worker's two database guards make it effectively-once.

**A correlation id is minted at the edge and carried on every log line,** validated against `^[A-Za-z0-9._:-]{1,64}$` because a caller-supplied id ends up in log lines and a newline would let a client forge one.

## Left for later

**Redis — considered, added, then removed.** Every piece of state is either durable in Postgres or per-process, so there is nothing a cache could hold that is safe to cache.

**`opportunity.version` — the one concurrency gap left open.** `stage_decided_at` covers job-versus-record; two people editing the same deal is the other half, and the second save silently wins.

**No rate limit on submit.** Nothing bounds how many jobs one tenant can create, and async submission made that cheap to spam.

**Small jobs are bound by the poll intervals, not by their data.** Two ticks sit between submitting and the first batch running, so a 5,000-record job still carries ~125 ms of pure waiting.

**That floor is mitigated, not removed.** Halving both ticks to 125 ms took ~100 ms off every small job and doubled queue wait; removing the wait needs a kick from `POST /bulk-moves`, not a shorter timer.

**Progress is reported in batches, not records.** The endpoint returns how many batch rows settled and how many records failed, but not how many records moved — that number is in the database and is not exposed.

## Exploring it

There is no UI, so two commands cover it. Both need the stack up.

**CLI — call the API by hand.** Use it to try a filter, move an opportunity, submit a bulk
move, or read a transition history.

```bash
npm run cli                                       # interactive menu
npm run cli -- opportunities --workspace=<uuid> --outcome=won --limit=20
npm run cli -- bulk-move --workspace=<uuid> --to=<stageId> --outcome=won
npm run cli -- job-status --workspace=<uuid> --id=<jobId>
npm run cli -- job-watch  --workspace=<uuid> --id=<jobId>
```

A workspace id is required. `npm run seed` prints them; there is no endpoint that lists
workspaces, because a caller must not be able to enumerate other tenants.

**`job-watch` polls every 2 seconds until the job settles** — in place on a terminal, appended when piped, so it reads live and in a log. `--interval=ms` and `--timeout=ms` override the defaults; a timeout gives up watching **without cancelling the job**, since there is no cancel endpoint.

```bash
     0.1s  running     21/50 batches  11 running  18 pending    -
     2.1s  completed   50/50 batches  0 running  0 pending  23.9 batches/s

  completed in 2.1s  50 batch(es), 50000 matched, 0 failed
```

Progress is in batches, not records, because the API exposes no records-moved count — see *Left for later*.

**Dump — write the database to a file,** to check by eye that a bulk move or a filter touched exactly the rows you expected. It reads Postgres directly, so it reflects what is actually stored.

```bash
npm run dump                                      # every row -> db-state.txt
npm run cli -- dump-db --sample=100               # capped, for the 500k dataset
npm run cli -- dump-db --out=before.txt           # then --out=after.txt and diff
```

**Watch a job run.** Service names, not the `bsm-` container names.

```bash
docker compose logs -f worker-service     # one line per batch: wait, work, pool pressure
docker compose logs -f transition-service # batching, per page, and the relay
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
removed** — the `stage_decided_at` trigger's two `WHEN` clauses, batching's
advisory claim, the worker's compare-and-swap. That is checked, not assumed.
