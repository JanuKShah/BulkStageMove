# Test strategy

216 tests, 19 suites, in three projects. `npm test` runs all; `npm run test:safety`
is the one to read first.

| project | files | tests | scope |
|---|---|---|---|
| `safety` | 11 | 119 | the mechanisms below |
| `endpoints` | 7 | 87 | routes, failure paths, tenant scoping |
| `happyflow` | 1 | 10 | one 50,000-record job through the broker |

## Delivery is at-least-once, so duplicates are the normal case

A batch row is marked published only after the broker confirms, so a crash between
the two re-publishes. Duplicates are therefore expected traffic, not an edge case,
and the naive fix — read the status, act on it — is wrong in a way that is
invisible until two consumers collide.

- **Two workers claiming one batch concurrently, where the status predicate cannot help** — both read `pending`, so only the per-batch advisory lock stops the second. **[non-vacuous]**
- A redelivered message finds a `completed` batch, claims nothing, and does no work.
- A redelivered message that finds a batch still `running` re-takes it — and one worker is ever inside it, which is what makes re-taking safe.
- A retry writes no second transition for a record it already moved, and there is no unique index that would have caught it.
- A retry counts records already at the target as **moved, not failed** — otherwise every record a half-finished batch completed becomes a failure on the second attempt.
- **Job counters take a delta rather than a sum.** A batch that legitimately settles twice reported 10 records for a 5-record job before this.
- N callers racing one `idempotencyKey` create exactly one job. **[non-vacuous]**
- A caller losing that race leaves no half-written job behind.
- The database refuses a duplicate key even with the service check removed. **[non-vacuous]**
- The same key with a different filter, or a different target stage, is rejected — otherwise a client retrying a *changed* request silently reuses the old job.
- A lost batch must hang the job at pending, not complete it. Losing a batch is the one failure worse than duplicating one.

## A job's walk cannot be parallelised, and a second replica must not duplicate it

Page N+1's keyset cursor is page N's last row, so one job is inherently serial.
That is fine — but it means a second builder is pure waste, and the default of "it's
idempotent, it'll be fine" is wrong in a way that scales with replica count.

- **Two builders on one job leave exactly one set of batches.** **[non-vacuous]**
- A builder skips a job another builder is already walking.
- One job held by a builder does not block a *different* job from building — the claim is per job, not global.
- The claim is released when the walk finishes, and also when it throws.
- A job no longer `preparing` is not re-walked.
- A partial cursor **resumes** rather than restarting — the batch and the cursor commit in one transaction, so the cursor can never claim progress the data does not have.
- A `preparing` job is never left marked `completed`, which is what the wrong default for that column produced: a silent hang with no batches and nothing to notice it.

## Two actors, one record: the ordering problem

`stage_decided_at` is a logical clock rather than a timestamp of change, because
the naive version — "did anyone touch this row since I started?" — loses to a
slower older job. Every row here is a case where the wrong answer is silent.

- **A person moves a record after submission; the job leaves it alone.** **[non-vacuous]**
- Left alone even when the person moved it *within* the filtered stage — the clock, not the stage list, is what protects it.
- **Not** left alone when the person moved it *before* submission.
- **An older job defers to a newer one** that already moved the record, even if the older job is still draining.
- **A newer job still moves a record an older job already moved** — the mirror case, and the one that proves the comparison is on submission order and not completion order.
- A rename does not remove a record from an in-flight job; nor does a value edit. Both are the same class of bug: any write that touches the row must not look like a stage decision.
- A job write is stamped with the **job's** time, not the write's, or an older job could overwrite a newer one.
- **Check order is load-bearing** — already-at-target is tested *before* the clock. A person who moves a record *to* the target has stamped it newer, so testing the clock first reports a skip and under-counts a job that achieved its intent. **[non-vacuous]**
- A job that skipped records still settles rather than hanging.
- A job over records a person edited afterwards reports the shortfall, rather than quietly reporting success.

## The clock trigger, and the clause that lets a job opt out

- A stage change stamps the wall clock. **[non-vacuous]**
- The trigger **stands down when the writer sets the clock itself** — without that clause a bulk job would stamp 1,000 rows with the wall clock and defeat the entire design. **[non-vacuous]**
- A name change, a value change, and an `updated_at`-only change each leave the clock alone.
- A null clock is rejected, so a record cannot be made permanently undecidable.
- A record created after a job was submitted still reads as newer than it.

## Filters: the bug this suite was written to catch

A filter naming an outcome that resolves to **no** stage is the dangerous case. It
reads as a filter, stores as an empty object, and a job with no stage filter
matches **the entire workspace**. Measured live before the fix: `outcome=lost`
against a workspace with no lost stage moved all 10 seeded records.

- **A filter naming an outcome with no stage matches NOTHING.** **[non-vacuous]**
- **An explicitly empty stage list is refused, not read as no filter** — the same hazard through a second door. **[non-vacuous]**
- **A filter naming no stages at all still matches the whole workspace** — the control for the two above, without which "always add the clause" would also pass.
- The stored filter is the one that will be executed, not a re-derived one.
- An explicit `null` is rejected rather than treated as no bound; an empty string is treated as absent.
- `minValue` above `maxValue`, and `createdFrom` after `createdTo`, are rejected rather than silently matching nothing.
- A record that **leaves the filtered stage** is skipped, and reported separately from one left alone for the clock — two different events that both read as "skipped".
- A record left alone for scope is tested with the clock aged forward, so the scope check is what is actually under test and not shadowed by the clock.
- `limit` is capped rather than trusted.

## Tenant isolation, checked where a mistake is silent

Not "does it return 403" — whether a row can be *reached* by another tenant's id.

- **No endpoint enumerates every workspace.** **[non-vacuous]** A caller must not be able to discover other tenants by asking; the CLI reads the database directly instead, for exactly this reason.
- The same stage name is allowed in two workspaces — the guard cannot be "name is unique".
- An opportunity owned by another workspace's user is refused, as is one pointing at another workspace's stage.
- A transition rule spanning two workspaces is refused.
- A batch whose job belongs to another workspace is refused.
- Another workspace cannot read a job or its transitions.
- Deleting a job **keeps** its transition and clears `job_id` — an audit trail that vanishes on delete is not an audit trail.

## The correlation id is caller-influenced, so it is untrusted input

- A newline in a caller-supplied id is rejected, because the id lands in log lines and a newline would let a client forge one.
- It is validated at the edge **and again at the service**, because the edge is not the only way in.
- A handler and a `ServiceClient` call it makes share one id, so a trace does not fork at the first hop.
- The id is stored on the job row, so the build traces back to the request that caused it.

## Full scale, where the shape breaks rather than the logic

- **Every record is in exactly one batch, with no overlap.** **[non-vacuous]** The old design stored one row per record; a record in two batches would be moved and counted twice, and cardinality would be summed once per id.
- Every matching record is snapshotted, not just the first page.
- 50 batches of 1,000, none retried, 50,000 records at the target, 0 failed.
- One attributable transition per record — and **50 distinct timestamps for 50,000 records**, because a batch applies in one transaction. Any per-record percentile is the batch distribution restated.
- A job that matches nothing **completes** rather than hanging at pending.
- A batch that exhausted its attempts is surfaced with its reason; a partly-applied batch is **not** reported as dead-lettered.
- A healthy job reports no dead-lettered batches.
