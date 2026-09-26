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

## Technology

| | |
|---|---|
| Node.js 22 | runtime |
| TypeScript 5.9 | `strict`, `noUncheckedIndexedAccess` |
| NestJS 11 + Express | HTTP framework |
| PostgreSQL 16 | datastore |
| `pg` 8 | driver, raw SQL, no ORM |
| Jest + ts-jest | tests |
| tsx | runs the migrate and seed scripts |
| ESLint + Prettier | lint and format |
| Docker Compose | 4 services + Postgres |

## Project setup

```bash
npm run verify     # up + seed + dump + test, the one command
```

`npm run up` is `docker compose up -d --build --wait` — it starts the 4 services, runs
migrations, and blocks until all are healthy. Use `docker compose up -d --build` directly
if you prefer; it just returns before the healthchecks pass.

Ports `3001` workspace, `3002` user, `3003` stage, `3004` opportunity. Tenant scoping via
`X-Workspace-Id`.

## Exploring it

There is no UI, so two commands cover it. Both need the stack up.

**CLI — call the API by hand.** Use it to try a filter, move an opportunity, or read a
transition history.

```bash
npm run cli                                       # interactive menu
npm run cli -- opportunities --workspace=<uuid> --outcome=won --limit=20
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


