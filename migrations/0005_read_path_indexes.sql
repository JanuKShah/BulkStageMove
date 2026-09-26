-- 0005_read_path_indexes.sql
--
-- Indexes for the read paths that are hot at the brief's scale. Each one is here
-- because EXPLAIN showed a sequential scan or an in-memory sort on a query that
-- runs per page, per batch or per poll - not because the table looks big.
--
-- The 50,000 record benchmark is what justifies these. At the seeded 15,000 rows
-- most of them are neutral; at 500,000 opportunities and 50,000 transitions in a
-- single job they are the difference between sorting the whole table per page and
-- reading exactly one page's worth.
--
-- Measured on the seeded dataset, opportunity list returning 1,000 of 10,000
-- matching rows:
--   before  Seq Scan, 10,000 rows examined, top-N heapsort, 1,974 buffers
--   after   Index Scan, 1,000 rows examined, no sort, 120 buffers
--
-- and bulk-moves/:id/transitions returning 50 of 8,314:
--   before  Seq Scan, 8,314 rows examined, top-N heapsort, 163 buffers
--   after   Index Scan, 50 rows examined, no sort, 54 buffers
--
-- Column order follows the query, not the table. Every one of these is a
-- keyset-paginated read, so the index has to supply the sort order as well as
-- the filter or the planner still sorts.

-- The opportunity list endpoint: WHERE workspace_id = $1 AND (created_at, id) > ...
-- ORDER BY created_at, id. Without this the page query sorts every row in the
-- workspace to return one page, which is the whole table at 500,000 rows.
CREATE INDEX opportunity_workspace_created_id_idx
    ON opportunity (workspace_id, created_at, id);

-- GET /opportunities/:id/transitions: WHERE opportunity_id = $1 AND
-- workspace_id = $2 ORDER BY created_at. opportunity_transition had no index at
-- all beyond its primary key, so this was a full scan of the table for one
-- record's history.
CREATE INDEX opportunity_transition_opportunity_created_idx
    ON opportunity_transition (opportunity_id, created_at);

-- GET /bulk-moves/:id/transitions: WHERE job_id = $1 AND workspace_id = $2
-- keyset on (created_at, id). A 50,000 record job writes 50,000 transition rows,
-- so this is the endpoint most likely to be polled while a long job runs.
CREATE INDEX opportunity_transition_job_created_id_idx
    ON opportunity_transition (job_id, created_at, id);

-- The worker's claim, and the status endpoint's rollup, both against the batch
-- rows. WHERE job_id = $1 AND batch_no = $2 AND status IN ('pending','running').
--
-- The predicate is IN ('pending','running') rather than = 'pending' on purpose.
-- 'running' has to be re-claimable so a redelivered message can re-take a batch
-- whose previous attempt rolled back, and a partial index only applies when the
-- query predicate implies the index predicate - an index on = 'pending' would
-- sit unused while the planner fell back to scanning on job_id alone. That
-- mistake was made once already and is what this index exists to correct.
--
-- There is no separate pending-only index. The status endpoint aggregates over a
-- whole job's batches, which a partial index on a single status cannot serve;
-- it was measured at zero scans before the per-item table was removed.
CREATE INDEX bulk_job_outbox_claimable_idx
    ON bulk_job_outbox (job_id, batch_no)
    WHERE status IN ('pending', 'running');
