CREATE EXTENSION IF NOT EXISTS pg_visibility;

DROP TABLE IF EXISTS vm_demo;
CREATE TABLE vm_demo (
  id   bigserial PRIMARY KEY,
  k    int NOT NULL,
  pad  text NOT NULL
);

-- Make it big enough to have many heap pages
INSERT INTO vm_demo(k, pad)
SELECT (g % 100000),
       repeat('x', 200)
FROM generate_series(1, 1500000) AS g;

CREATE INDEX vm_demo_k_idx ON vm_demo (k);
ANALYZE vm_demo;
CREATE EXTENSION
NOTICE:  table "vm_demo" does not exist, skipping
DROP TABLE
CREATE TABLE


INSERT 0 1500000
CREATE INDEX
ANALYZE
postgres=#
postgres=#
postgres=# VACUUM (ANALYZE, FREEZE) vm_demo;
VACUUM
postgres=#
postgres=#
postgres=#
postgres=# SET enable_seqscan = off;

EXPLAIN (ANALYZE, BUFFERS)
SELECT k
FROM vm_demo
WHERE k BETWEEN 1000 AND 90000
ORDER BY k
LIMIT 50000;
SET
                                                                   QUERY PLAN
-------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=0.43..1213.32 rows=50000 width=4) (actual time=0.140..14.500 rows=50000 loops=1)
   Buffers: shared hit=6669 read=54
   ->  Index Only Scan using vm_demo_k_idx on vm_demo  (cost=0.43..32429.27 rows=1336842 width=4) (actual time=0.137..10.176 rows=50000 loops=1)
         Index Cond: ((k >= 1000) AND (k <= 90000))
         Heap Fetches: 0
         Buffers: shared hit=6669 read=54
 Planning:
   Buffers: shared hit=27 read=3
 Planning Time: 1.205 ms
 Execution Time: 17.765 ms
(10 rows)

postgres=#
postgres=#
postgres=#
postgres=#
postgres=# SELECT
  count(*) AS heap_pages,
  sum((all_visible)::int) AS all_visible_pages,
  round(100.0 * sum((all_visible)::int) / count(*), 2) AS all_visible_pct
FROM pg_visibility_map('vm_demo');
 heap_pages | all_visible_pages | all_visible_pct
------------+-------------------+-----------------
      45455 |             45455 |          100.00
(1 row)

postgres=#
postgres=#
postgres=#
postgres=# -- Update 10% of keys -> lots of heap pages become not-all-visible
UPDATE vm_demo
SET pad = pad
WHERE k % 10 = 0;

UPDATE 150000
postgres=#
postgres=#
postgres=#
postgres=# EXPLAIN (ANALYZE, BUFFERS)
SELECT k
FROM vm_demo
WHERE k BETWEEN 1000 AND 90000
ORDER BY k
LIMIT 50000;
                                                                   QUERY PLAN
-------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=0.43..1810.53 rows=50000 width=4) (actual time=0.188..39.660 rows=50000 loops=1)
   Buffers: shared hit=60381 read=1352 dirtied=1391
   ->  Index Only Scan using vm_demo_k_idx on vm_demo  (cost=0.43..53235.94 rows=1470512 width=4) (actual time=0.187..36.926 rows=50000 loops=1)
         Index Cond: ((k >= 1000) AND (k <= 90000))
         Heap Fetches: 55010
         Buffers: shared hit=60381 read=1352 dirtied=1391
 Planning:
   Buffers: shared hit=9 read=12 dirtied=13
 Planning Time: 0.453 ms
 Execution Time: 41.421 ms
(10 rows)

postgres=# SELECT
  count(*) AS heap_pages,
  sum((all_visible)::int) AS all_visible_pages,
  round(100.0 * sum((all_visible)::int) / count(*), 2) AS all_visible_pct
FROM pg_visibility_map('vm_demo');
 heap_pages | all_visible_pages | all_visible_pct
------------+-------------------+-----------------
      50000 |                 0 |            0.00
(1 row)

postgres=# VACUUM (ANALYZE) vm_demo;
VACUUM
postgres=# SELECT
  count(*) AS heap_pages,
  sum((all_visible)::int) AS all_visible_pages,
  round(100.0 * sum((all_visible)::int) / count(*), 2) AS all_visible_pct
FROM pg_visibility_map('vm_demo');
 heap_pages | all_visible_pages | all_visible_pct
------------+-------------------+-----------------
      50000 |             50000 |          100.00
(1 row)

postgres=#
