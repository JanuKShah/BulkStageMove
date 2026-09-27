# BulkStageMove

Bulk stage move for Opportunities, SDE-3 backend take-home.

| | |
|---|---|
| **[`DESIGN.md`](DESIGN.md)** | chunking and the cursor, idempotency, concurrency on one record, snapshot vs live, isolation, what breaks at 10× |
| **[`BENCHMARKS.md`](BENCHMARKS.md)** | the numbers, the hardware, the method |
| **[`TESTSTRATEGY.md`](TESTSTRATEGY.md)** | what each test protects and why it exists |

## The problem

A user selects a filter, stage, owner, status, value range, date range, and
moves up to **50,000** matching opportunities to a different stage in one action.
The API returns immediately with a job handle. The work happens in the
background. The user polls for progress.

At volume it is a background job that must be **idempotent**, **resumable**, and
polite to everything running alongside it, and it has to stay correct while
individual users are editing the same records by hand.

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
- `opportunity.stage_decided_at` is a **logical clock**, see `DESIGN.md` section 9.

## Demo

https://github.com/user-attachments/assets/e0f60d7f-9ea3-43ff-a536-e6a75f86b6f6

## Technology

| | |
|---|---|
| Node.js 22 | runtime |
| TypeScript 5.9 | `strict`, `noUncheckedIndexedAccess` |
| NestJS 11 + Express | HTTP framework |
| PostgreSQL 16 | the only datastore, no cache, no second store |
| `pg` 8 | driver, raw SQL, no ORM |
| RabbitMQ 4 + `amqplib` 2 | batch dispatch between services |
| nginx | the edge, mints the correlation id |
| Jest + ts-jest | 253 tests across 24 suites |
| tsx | runs the migrate, seed, cli and benchmark scripts |
| ESLint + Prettier | lint and format |
| Docker Compose | 10 containers |

## Running it

```bash
npm run verify     # up + seed + dump + test, the one command
```

`npm run up` is `docker compose up -d --build --wait`, it starts everything, runs
migrations, and blocks until healthy. `npm run down` tears the stack down and
**deletes the database volume**.

| command | |
|---|---|
| `npm run up` / `npm run down` | start / stop, with a fresh volume |
| `npm run seed` | seed a workspace and print its id |
| `npm run seed:large` | the 500,000 record dataset |
| `npm run test` | 253 tests, in band |
| `npm run test:safety` / `:endpoints` / `:happyflow` | one project |
| `npm run bench` | the 50,000 record benchmark, see `BENCHMARKS.md` |
| `npx tsx test/happyflow/filter-bench.ts` | filter selectivity against batching |
| `npm run lint` / `npm run format` | |

## The services

**Ten containers, all started together. Six are application services, single-process each, the concurrency is inside them, not in how many there are.**

| container | role | port | kept up at once |
|---|---|---|---|
| `workspace-service` | tenants | 3001 | 1 process, **DB pool of 10** |
| `user-service` | owners | 3002 | 1 process, **DB pool of 10** |
| `stage-service` | pipelines and permitted moves | 3003 | 1 process, **DB pool of 10** |
| `opportunity-service` | deals, their stages, the list and filter | 3004 | 1 process, **DB pool of 10** |
| `transition-service` | **bulk jobs**: submit, batching, status | 3005 | 1 process, **DB pool of 10**, **2 background loops** |
| `worker-service` | applies batches; **no port**, it only consumes | n/a | 1 process, **12 consumers**, **DB pool of 12** |
| `nginx` | the edge; fronts transition-service only | 18080 | 1 |
| `postgres` | the datastore | 5432 | 1 |
| `rabbitmq` | batch dispatch | 5672, UI 15672 | 1 exchange, **1 queue** |
| `migrate` | applies migrations, then exits | n/a | runs once at startup |

**The three loops that do the work:**

| loop | where | every | up to | parallelism |
|---|---|---|---|---|
| `SnapshotBuilder` | transition-service | 125 ms | 25 jobs claimed | **one job at a time**, one walker per job |
| `OutboxRelay` | transition-service | 125 ms | 50 unpublished rows | one at a time |
| `BatchWorker` | worker-service | message-driven | n/a | **12 slots**, one channel each |

**"DB pool of N" is a ceiling on Postgres connections** for that process (`PG_POOL_MAX` becomes `pg`'s `max`), opened on demand, the idle stack held 8 while this was written. Five pools of 10 plus the worker's 12 is **62 against Postgres' 100** `max_connections`, which is why the worker gets 12 and the rest 10. The ceiling is per process, so it multiplies by replica count: three worker replicas put the stack at 86, and a fourth breaches the limit.

**One exchange, one queue.** No dead letter queue: a batch that runs out of attempts
is marked `failed` in Postgres and the message is acked, so `POST
/bulk-moves/:id/retry-failed` can put it back. The reasoning is in `DESIGN.md` section 9.

Tenant scoping is a required `X-Workspace-Id` header.

## What works, what does not

**Built and measured.** Async submit returning in ~13 ms; a resumable keyset walk
that commits each batch with the cursor after it; batches of 1,000 dispatched over
an outbox to 12 consumers; `stage_decided_at` so a person's edit is never
overwritten and the newest job wins; composite-FK tenant isolation; 253 tests; a
50,000-record job in **1.40 s**, and 500,000 in 58.71 s. A CLI with a job watcher
that polls to completion, and a database dump for checking a move by eye.

**Deliberately not built.** Redis, every piece of state is already durable in
Postgres or per-process. `opportunity.version`, the one real concurrency gap, not
observable without a UI. A gateway, websockets, or one service per noun. An
event-driven *relay* kick, submit kicks the sweep inline, but a committed batch
still waits out the relay's next tick.

## Exploring it

There is no UI, so two commands cover it. Both need the stack up.

**CLI, call the API by hand.** Use it to try a filter, move an opportunity, submit a bulk
move, or read a transition history.

Run bare, it lists the workspaces in the database so you can pick one by number, writes `db-state.txt`, and opens the menu.

```bash
npm run cli
npm run cli -- bulk-move --workspace=<uuid> --to=<stageId> --stageId=<uuid> --outcome=won
```

Every menu action has a command equivalent, and the CLI carries the full list of
them, so nothing here has to be repeated to be discoverable. Run one that does not
exist and it prints the list back.

**Dump, write the database to a file,** to check by eye that a bulk move or a filter touched exactly the rows you expected. It reads Postgres directly, so it reflects what is actually stored.

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

**253 tests across 24 suites**, see **[`TESTSTRATEGY.md`](TESTSTRATEGY.md)** for what each one covers and why.

| command | |
|---|---|
| `npm test` | all 253, in band |
| `npm run test:unit` | pure functions, no database needed |
| `npm run test:safety` | the ones that go red when a guard is removed |
| `npm run test:endpoints` / `:happyflow` | routes and failure paths / one 50,000 record job end to end |
