# Benchmarks

50,000 opportunities, one job, `npm run bench`. No test asserts these numbers;
correctness at scale is asserted in `test/happyflow/bulk-happy-flow.spec.ts`.

Measured on Windows 11, 12 logical cores, 15.7 GB, Node v24.21.0, PostgreSQL
16.15, RabbitMQ 4.3.6 — one Compose project, all services sharing one Postgres.
Not a portable SLA.

## The job

| 50,000 records | item table, 4 consumers | batches, 4 consumers | **batches, 12 consumers** |
|---|---|---|---|
| Drain — submit to settled | 0.52 s | 5.18 s | **1.83 s** |
| **End-to-end** | **8.92 s** | **7.47 s** | **2.39 s** |
| End-to-end rate | 5,605/sec | 6,693/sec | **20,921/sec** |

**Submission is deliberately not a column**, because it does not vary with consumer
count and putting it in one implied a cause that cannot exist. `submit()` walks the
filter before it writes any batch rows, so no batch exists to consume and the
workers sit idle throughout — the only moving parts are the transition service and
Postgres.

| | submission |
|---|---|
| item table, 4 consumers | 8.40 s |
| batches, any consumer count | **0.56 – 2.30 s** |

The 4× spread in that range is this host, not the code: six runs at 4 consumers
gave 0.58, 1.59, 1.75, 2.16, 2.21 and 2.30 s. Treat any single submission figure
as a sample, not a result.

The middle column is the same code as the right one, at the worker setting this
change was measured against. The left column is a **single run** and is the
weakest number here.

## Why 12 consumers

Consumer count was measured, not guessed. The per-batch time is the evidence: 0.41 s
at 4 consumers, 0.44 s at 12. Flat, while throughput rises 2.8×. Linear scaling —
the batches do not get slower, more run at once. Each does 1,000 opportunity
updates and 1,000 transition inserts, so it is compute-bound, not queue-bound, and
matching consumers to cores is the lever.

One queue, not one per worker: consumers on a single queue already share work
round-robin, so extra queues only add a second thing to keep balanced.

`RABBITMQ_CONSUMER_CONCURRENCY` is a tuning value, not a constant, and would need
re-measuring on other hardware.

## What the job writes

The item table wrote 50,000 rows per job, four indexes and three composite foreign
keys each. Measured inside a chunk, that insert was **81%** of the time to the
`201`. It now writes 50 batch rows holding the same ids as arrays.

There is **no per-record latency distribution** to report, and there never was: a
batch applies in one transaction, so its 1,000 transitions share one `created_at`.
The benchmark counts the distinct timestamps directly — 50 for 50,000 records.
Any percentile over the expanded set is the batch distribution restated.

## The drain is still 3.5× slower than before

Removing 50,000 row inserts should have made the drain faster. At 4 consumers it
went 0.52 s → 5.18 s. At the shipped 12 it is 1.83 s, so most of the gap is
concurrency — but that residue appeared at *constant* consumer count, so it is not
a parallelism problem. Three extra round trips per batch and contention on the
shared `bulk_job` row are the candidates; neither is measured. **Unidentified.**

## Beyond the brief

At 500,000 the job still completes cleanly — 500,000 moved, 0 failed, 57.34 s
end-to-end. **Submission breaks first:** 0.56 s at 50,000 became 32.06 s at 500,000,
sixteen times the time for ten times the data. `submit()` accumulates every batch's
ids before writing any, so 500,000 uuid strings are resident at once — roughly
50–100 MB, and it will not hold at 5,000,000. It needs to stream.

## Tests

| suite | tests | time |
|---|---|---|
| `safety` | 61 | ~14 s |
| `endpoints` | 86 | ~14 s |
| `happyflow` | 10 | 11 s |
| **all three** | **157** | passing |

## Known limits

- The 3.5× drain residue is unexplained.
- Concurrency is tuned to this host's 12 cores.
- An earlier version of this file claimed a chunk-size win of 1.21× that was really
  1.04×. The false figure is in the message of pushed commit `8f2052b`.
- The two harnesses that measured the old shape were deleted with it rather than
  repaired; they would have produced numbers about a design that no longer exists.
