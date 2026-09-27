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
- `opportunity.stage_decided_at` is a **logical clock**: a person's edit stamps the wall clock, a bulk job stamps its own submission time, and a rename or value edit changes nothing.
  A job skips any record whose clock is newer than its submission, so a manual edit is never overwritten and the newest job wins however long the older one takes.
  It covers job-versus-record only, and needs no synchronised clocks — every timestamp in the comparison comes from the one Postgres, never from a worker's own clock.
  Two people editing the same record is the gap it deliberately does not cover: there is no `version` column, so the second save silently wins.
  The natural fix is a `version integer` counter with `WHERE version = $expected` on the single-record move, rejecting the **later** save with a 409.
  The bulk job must never bump it — a job moving 1,000 records would bump 1,000 versions and hand every user with one of them open a 409 about a field they never edited — so the two mechanisms stay independent.

## Technology

| | |
|---|---|
| Node.js 22 | runtime |
| TypeScript 5.9 | `strict`, `noUncheckedIndexedAccess` |
| NestJS 11 + Express | HTTP framework |
| PostgreSQL 16 | datastore |
| `pg` 8 | driver, raw SQL, no ORM |
| RabbitMQ 4 + `amqplib` 2 | batch dispatch between services |
| Jest + ts-jest | tests |
| tsx | runs the migrate and seed scripts |
| ESLint + Prettier | lint and format |
| Docker Compose | 5 services + Postgres + RabbitMQ |

## Project setup

```bash
npm run verify     # up + seed + dump + test, the one command
```

`npm run up` is `docker compose up -d --build --wait` — it starts the services, runs
migrations, and blocks until all are healthy. Use `docker compose up -d --build` directly
if you prefer; it just returns before the healthchecks pass.

Ports `3001` workspace, `3002` user, `3003` stage, `3004` opportunity, `3005` bulk jobs.
`5672` RabbitMQ, `15672` its management UI. Tenant scoping via `X-Workspace-Id`.

A submitted job is dispatched to RabbitMQ in batches of 1,000 and applied by the
worker service, which registers **4 consumers**, each on its own channel so batches
are processed in parallel. Set `RABBITMQ_CONSUMER_CONCURRENCY` to change it;
`RABBITMQ_PREFETCH` (default 1) caps batches in flight per consumer.

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


