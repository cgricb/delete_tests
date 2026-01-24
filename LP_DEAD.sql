postgres=# CREATE EXTENSION IF NOT EXISTS pageinspect;

DROP SCHEMA IF EXISTS demo_lp CASCADE;
CREATE SCHEMA demo_lp;
SET search_path = demo_lp;

CREATE TABLE t (
  id  int PRIMARY KEY,
  pad text NOT NULL
);


INSERT INTO t
SELECT g, repeat('x', 500)
FROM generate_series(1, 2000) g;

VACUUM (ANALYZE) t;
CREATE EXTENSION
NOTICE:  schema "demo_lp" does not exist, skipping
DROP SCHEMA
CREATE SCHEMA
SET
CREATE TABLE
INSERT 0 2000
VACUUM
postgres=#
postgres=# SELECT id, ctid FROM t WHERE id IN (10, 11, 12);
 id |  ctid
----+--------
 10 | (0,10)
 11 | (0,11)
 12 | (0,12)
(3 rows)

postgres=# WITH x AS (
  SELECT (ctid::text::point)[0]::int AS blk
  FROM t
  WHERE id = 10
)
SELECT * FROM x;
 blk
-----
   0
(1 row)
postgres=#
postgres=# SELECT lp, lp_flags, t_xmin, t_xmax, t_ctid
FROM demo_batch.heap_page_items(
       demo_batch.get_raw_page('demo_lp.t', 0)
     )
ORDER BY lp;
 lp | lp_flags | t_xmin | t_xmax | t_ctid
----+----------+--------+--------+--------
  1 |        1 |    791 |      0 | (0,1)
  2 |        1 |    791 |      0 | (0,2)
  3 |        1 |    791 |      0 | (0,3)
  4 |        1 |    791 |      0 | (0,4)
  5 |        1 |    791 |      0 | (0,5)
  6 |        1 |    791 |      0 | (0,6)
  7 |        1 |    791 |      0 | (0,7)
  8 |        1 |    791 |      0 | (0,8)
  9 |        1 |    791 |      0 | (0,9)
 10 |        1 |    791 |      0 | (0,10)
 11 |        1 |    791 |      0 | (0,11)
 12 |        1 |    791 |      0 | (0,12)
 13 |        1 |    791 |      0 | (0,13)
 14 |        1 |    791 |      0 | (0,14)
 15 |        1 |    791 |      0 | (0,15)
(15 rows)

postgres=#
postgres=#
postgres=#
postgres=# DELETE FROM t WHERE id IN (10, 11, 12);
DELETE 3
postgres=#
postgres=#
postgres=# SELECT lp, lp_flags, t_xmin, t_xmax, t_ctid
FROM demo_batch.heap_page_items(
       demo_batch.get_raw_page('demo_lp.t', 0)
     )
ORDER BY lp;
 lp | lp_flags | t_xmin | t_xmax | t_ctid
----+----------+--------+--------+--------
  1 |        1 |    791 |      0 | (0,1)
  2 |        1 |    791 |      0 | (0,2)
  3 |        1 |    791 |      0 | (0,3)
  4 |        1 |    791 |      0 | (0,4)
  5 |        1 |    791 |      0 | (0,5)
  6 |        1 |    791 |      0 | (0,6)
  7 |        1 |    791 |      0 | (0,7)
  8 |        1 |    791 |      0 | (0,8)
  9 |        1 |    791 |      0 | (0,9)
 10 |        1 |    791 |    795 | (0,10)
 11 |        1 |    791 |    795 | (0,11)
 12 |        1 |    791 |    795 | (0,12)
 13 |        1 |    791 |      0 | (0,13)
 14 |        1 |    791 |      0 | (0,14)
 15 |        1 |    791 |      0 | (0,15)
(15 rows)

postgres=# commit;
WARNING:  there is no transaction in progress
COMMIT
postgres=# VACUUM (VERBOSE, ANALYZE, DISABLE_PAGE_SKIPPING) t;
INFO:  aggressively vacuuming "postgres.demo_lp.t"
INFO:  finished vacuuming "postgres.demo_lp.t": index scans: 0
pages: 0 removed, 134 remain, 134 scanned (100.00% of total)
tuples: 3 removed, 1997 remain, 0 are dead but not yet removable
removable cutoff: 796, which was 0 XIDs old when operation ended
frozen: 0 pages from table (0.00% of total) had 0 tuples frozen
index scan bypassed: 1 pages from table (0.75% of total) have 3 dead item identifiers
avg read rate: 0.000 MB/s, avg write rate: 4.770 MB/s
buffer usage: 277 hits, 0 misses, 1 dirtied
WAL usage: 2 records, 1 full page images, 3253 bytes
system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s
INFO:  aggressively vacuuming "postgres.pg_toast.pg_toast_16525"
INFO:  finished vacuuming "postgres.pg_toast.pg_toast_16525": index scans: 0
pages: 0 removed, 0 remain, 0 scanned (100.00% of total)
tuples: 0 removed, 0 remain, 0 are dead but not yet removable
removable cutoff: 796, which was 0 XIDs old when operation ended
new relfrozenxid: 796, which is 4 XIDs ahead of previous value
frozen: 0 pages from table (100.00% of total) had 0 tuples frozen
index scan not needed: 0 pages from table (100.00% of total) had 0 dead item identifiers removed
avg read rate: 0.000 MB/s, avg write rate: 41.778 MB/s
buffer usage: 6 hits, 0 misses, 1 dirtied
WAL usage: 1 records, 1 full page images, 7901 bytes
system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s
INFO:  analyzing "demo_lp.t"
INFO:  "t": scanned 134 of 134 pages, containing 1997 live rows and 3 dead rows; 1997 rows in sample, 1997 estimated total rows
VACUUM
postgres=# SELECT lp, lp_flags, t_xmin, t_xmax, t_ctid
FROM demo_batch.heap_page_items(
       demo_batch.get_raw_page('demo_lp.t', 0)
     )
ORDER BY lp;
 lp | lp_flags | t_xmin | t_xmax | t_ctid
----+----------+--------+--------+--------
  1 |        1 |    791 |      0 | (0,1)
  2 |        1 |    791 |      0 | (0,2)
  3 |        1 |    791 |      0 | (0,3)
  4 |        1 |    791 |      0 | (0,4)
  5 |        1 |    791 |      0 | (0,5)
  6 |        1 |    791 |      0 | (0,6)
  7 |        1 |    791 |      0 | (0,7)
  8 |        1 |    791 |      0 | (0,8)
  9 |        1 |    791 |      0 | (0,9)
 10 |        3 |        |        |
 11 |        3 |        |        |
 12 |        3 |        |        |
 13 |        1 |    791 |      0 | (0,13)
 14 |        1 |    791 |      0 | (0,14)
 15 |        1 |    791 |      0 | (0,15)
(15 rows)
