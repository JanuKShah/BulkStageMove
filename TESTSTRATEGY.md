# Test Strategy

The test suite contains 253 tests across 24 suites, divided into four projects:

| Project | Files | Tests | Purpose |
|---|---|---|---|
| unit | 2 | 17 | Fast checks for pure functions without a database |
| safety | 13 | 132 | Concurrency, race conditions, and data safety guards |
| endpoints | 8 | 94 | HTTP API contracts, status codes, and input validation |
| happyflow | 1 | 10 | End-to-end 50,000-record run through RabbitMQ and PostgreSQL |

To run everything: npm test. To inspect the core safety logic, run npm run test:safety.

## 1. Handling At-Least-Once Delivery and Retries

Because we only mark an outbox batch as published after the broker acknowledges it, network blips can cause duplicate deliveries. Duplicates are normal traffic, not rare anomalies.

* *Concurrent worker claims:* If two workers pick up the same batch at the same time, both see the status as pending. Only a session-scoped PostgreSQL advisory lock stops the second worker.
* *Redelivered batches:* If a message is redelivered after a batch has already finished, the worker recognizes the completed status and does nothing.
* *Idempotent record moves:* Retrying a batch never inserts duplicate transitions. Records already at the target stage count as successfully moved, not failed.
* *Delta counters:* Bulk job counters use deltas rather than total sums or naive increments. This prevents retried batches from inflating the total moved count.
* *Idempotency races:* Multiple concurrent API submissions using the same idempotency key create exactly one job. Callers that lose the race receive the existing job ID rather than an internal server error.
* *Key reuse validation:* Reusing an idempotency key with a different filter or target stage is rejected with HTTP 409 Conflict.

## 2. Keyset Walking and Safe Resumption

Building batches for a single job cannot be parallelized because each page needs the cursor from the previous page. Multiple worker nodes must not duplicate this work.

* *Single builder per job:* Builders use advisory locks per job. A second replica skips any job currently being processed.
* *Resuming from failures:* The keyset cursor (created_at, id) commits in the exact same transaction as the batch itself. If a node crashes mid-walk, the next worker resumes from that cursor without skipping or duplicating records.
* *State defaults:* A new job starts in preparing status so the system knows batches are still being generated, avoiding false timeouts.

## 3. Concurrency Between Users and Background Jobs

When a user edits an opportunity while a bulk job is running, the human edit must take priority. We use a logical clock (stage_decided_at) instead of a wall clock to track decision order.

* *Human edits win:* If a user moves an opportunity after the job was submitted, the bulk worker skips that record and reports the shortfall.
* *Submission order matters:* An older job yielding to a newer job depends on submission time (snapshot_at), not completion time.
* *Check order in worker:* The worker checks whether a record is already at the target stage before checking the clock. This ensures that records already moved by users or earlier attempts are counted as moved rather than skipped.
* *Atomic updates:* The stage update and transition audit insertion run in a single SQL CTE. If a record is edited during the statement execution, the batch rolls back and retries cleanly.

## 4. Logical Clock Trigger Mechanics

* *Automatic updates:* A PostgreSQL trigger automatically updates stage_decided_at to the current time whenever a user changes a stage.
* *Trigger bypass for bulk jobs:* The trigger only fires if the writer leaves stage_decided_at alone. The bulk worker explicitly sets it to the job's snapshot_at, allowing the job to preserve its logical timestamp.
* *Non-stage updates:* Renaming an opportunity or updating other fields leaves the logical clock untouched, preventing accidental drops from running jobs.

## 5. Filter Edge Cases and Safety

* *Empty match safety:* If a filter targets an outcome that maps to no stages (such as lost in a pipeline without a lost stage), the query must match zero records. It must never accidentally match the whole workspace.
* *Explicit validation:* Empty stage arrays, invalid date ranges, or minimum values higher than maximum values are rejected immediately.
* *Records leaving scope:* If an opportunity moves out of the filtered stage before its batch is processed, the worker skips it and records it separately from clock-based skips.

## 6. Tenant Isolation

We verify tenant isolation at the database layer rather than relying on application code discipline alone.

* *Composite foreign keys:* Every table includes workspace_id, and all cross-table references use composite keys like (id, workspace_id). Records cannot reference data from another workspace.
* *No global enumeration:* There is no endpoint that lists all workspaces.
* *Cross-workspace protection:* API requests cannot read, move, or transition opportunities belonging to another workspace.

## 7. Full-Scale Verification

* *Exact partitioning:* Across 50,000 opportunities, the system creates 50 batches of 1,000 records each with zero overlap and zero omitted records.
* *One transition per record:* All 50,000 records reach the target stage with exactly one transition row each.
* *Attributable batches:* Batches commit in 50 distinct transactions, producing 50 distinct audit timestamps for 50,000 records.
