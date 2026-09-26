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
docker compose up -d --build
npm run migrate
npm run seed
npm test
```

Ports `3001` workspace, `3002` user, `3003` stage, `3004` opportunity. Tenant scoping via `X-Workspace-Id`.

