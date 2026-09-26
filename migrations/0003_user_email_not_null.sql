-- 0003_user_email_not_null.sql
--
-- A workspace user has to be reachable, so email becomes mandatory. SET NOT NULL
-- rather than a backfill: fabricating addresses would put invented data in a
-- load-bearing column, and Postgres refusing the statement is the right outcome.
-- It also gives UNIQUE (workspace_id, email) one meaning - NULLs are distinct, so
-- a nullable email let nameless users share a workspace unjudged by the
-- constraint.
--
-- Still open: the constraint compares the raw string, so Alice@x.com and
-- alice@x.com are two accounts. Every seeded address already satisfies
-- lower(trim(email)) = email, so a functional unique index can be added later.

ALTER TABLE app_user ALTER COLUMN email SET NOT NULL;
