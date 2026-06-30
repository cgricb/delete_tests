-- Visibility Map (VM) -> Index-Only-Scan (IOS) regression demo.
-- Shows how churn flips heap pages "not all-visible", forcing IOS heap fetches,
-- and how VACUUM restores all-visible so IOS goes fast again.
-- Run: psql -f vm_regression.sql   (captured output is at the bottom of this file)

CREATE EXTENSION IF NOT EXISTS pg_visibility;

DROP TABLE IF EXISTS vm_demo;
CREATE TABLE vm_demo (
  id   bigserial PRIMARY KEY,
  k    int NOT NULL,
  pad  text NOT NULL
);

-- Big enough to span many heap pages
INSERT INTO vm_demo(k, pad)
SELECT (g % 100000), repeat('x', 200)
FROM generate_series(1, 1500000) AS g;

CREATE INDEX vm_demo_k_idx ON vm_demo (k);
ANALYZE vm_demo;

-- Freeze + set all-visible so an Index Only Scan needs zero heap fetches.
VACUUM (ANALYZE, FREEZE) vm_demo;

-- Force the index path so we measure IOS, not a seq scan.
SET enable_seqscan = off;

\echo '== IOS while 100% all-visible (expect Heap Fetches: 0) =='
EXPLAIN (ANALYZE, BUFFERS)
SELECT k FROM vm_demo
WHERE k BETWEEN 1000 AND 90000
ORDER BY k
LIMIT 50000;

\echo '== VM coverage before churn =='
SELECT count(*) AS heap_pages,
       sum((all_visible)::int) AS all_visible_pages,
       round(100.0 * sum((all_visible)::int) / count(*), 2) AS all_visible_pct
FROM pg_visibility_map('vm_demo');

-- Even a no-op "SET pad = pad" creates new row versions (Postgres does not
-- elide no-op UPDATEs) and clears all-visible on every touched page; with
-- fillfactor 100 many of these updates are NOT HOT, so the table also grows.
UPDATE vm_demo SET pad = pad WHERE k % 10 = 0;

\echo '== IOS after churn (expect large Heap Fetches + buffers) =='
EXPLAIN (ANALYZE, BUFFERS)
SELECT k FROM vm_demo
WHERE k BETWEEN 1000 AND 90000
ORDER BY k
LIMIT 50000;

\echo '== VM coverage after churn (expect ~0% all-visible) =='
SELECT count(*) AS heap_pages,
       sum((all_visible)::int) AS all_visible_pages,
       round(100.0 * sum((all_visible)::int) / count(*), 2) AS all_visible_pct
FROM pg_visibility_map('vm_demo');

VACUUM (ANALYZE) vm_demo;

\echo '== VM coverage after VACUUM (expect 100% all-visible again) =='
SELECT count(*) AS heap_pages,
       sum((all_visible)::int) AS all_visible_pages,
       round(100.0 * sum((all_visible)::int) / count(*), 2) AS all_visible_pct
FROM pg_visibility_map('vm_demo');

RESET enable_seqscan;

-- ============================================================
-- ACTUAL OUTPUT (PostgreSQL 17.10 on aarch64-apple-darwin)
-- ============================================================
-- == IOS while 100% all-visible (Heap Fetches: 0) ==
--  Limit (actual time=0.024..3.100 rows=50000)
--    Buffers: shared hit=6667 read=56
--    ->  Index Only Scan using vm_demo_k_idx ...
--          Heap Fetches: 0
--  Execution Time: 3.965 ms
--
-- == VM coverage before churn ==
--  heap_pages | all_visible_pages | all_visible_pct
--       45455 |             45455 |          100.00
--
-- UPDATE 150000
--
-- == IOS after churn ==
--  Limit (actual time=0.035..9.037 rows=50000)
--    Buffers: shared hit=61733          <- ~9x the buffers vs 6723 before
--    ->  Index Only Scan using vm_demo_k_idx ...
--          Heap Fetches: 55010          <- VM no longer all-visible => heap visits
--  Execution Time: 9.933 ms
--
-- == VM coverage after churn ==
--  heap_pages | all_visible_pages | all_visible_pct
--       50000 |                 0 |            0.00
--
-- == VM coverage after VACUUM ==
--  heap_pages | all_visible_pages | all_visible_pct
--       50000 |             50000 |          100.00   <- IOS goes fast again
