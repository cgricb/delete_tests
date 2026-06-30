-- LP_DEAD ("known dead") index hint-bit demo using pageinspect.
-- After deleting rows, the btree still points at the now-dead heap tuples.
-- An index scan that visits those dead tuples marks the index entries LP_DEAD
-- (kill_prune) so later scans can skip them without touching the heap.
--
-- NOTE: the original mixed an unqualified `CREATE EXTENSION pageinspect` with
-- `demo_batch.bt_page_*` calls, which only worked if the extension happened to
-- live in schema demo_batch. This version installs pageinspect in the current
-- schema and calls its functions unqualified, so it is reproducible as-is.
-- Run: psql -f LP_DEAD.sql   (captured output is at the bottom of this file)

CREATE EXTENSION IF NOT EXISTS pageinspect;

DROP TABLE IF EXISTS public.lpdead_demo;
CREATE TABLE public.lpdead_demo (
  id  bigserial PRIMARY KEY,
  k   int NOT NULL,
  pad text NOT NULL
) WITH (autovacuum_enabled = off);     -- keep vacuum from clearing things under us

CREATE INDEX lpdead_demo_k_idx ON public.lpdead_demo(k);

INSERT INTO public.lpdead_demo(k, pad)
SELECT g, repeat('x', 200)
FROM generate_series(1, 300000) g;
ANALYZE public.lpdead_demo;

-- Delete ~10% of rows. Index still references the dead heap tuples.
DELETE FROM public.lpdead_demo WHERE k % 10 = 0;   -- ~30000 rows

-- Force a plain index scan so the executor visits the dead heap tuples and
-- marks the matching index items LP_DEAD via kill_prune.
SET enable_seqscan   = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.lpdead_demo WHERE k BETWEEN 1 AND 300000;
RESET enable_seqscan;
RESET enable_bitmapscan;

\echo '== Leaf pages carrying the most LP_DEAD items =='
WITH idx AS (
  SELECT 'public.lpdead_demo_k_idx'::regclass::text AS i_txt
),
sz AS (
  SELECT i_txt, (pg_relation_size(i_txt::regclass)/8192 - 1)::int AS maxblk
  FROM idx
)
SELECT blkno,
       (bt_page_stats(i_txt, blkno)).live_items AS live,
       (bt_page_stats(i_txt, blkno)).dead_items AS dead
FROM sz, LATERAL generate_series(1, maxblk) AS blkno
WHERE (bt_page_stats(i_txt, blkno)).type = 'l'
  AND (bt_page_stats(i_txt, blkno)).dead_items > 0
ORDER BY dead DESC
LIMIT 20;

-- Pick the first leaf block that has LP_DEAD items, then list those items.
WITH idx AS (
  SELECT 'public.lpdead_demo_k_idx'::regclass::text AS i_txt
),
sz AS (
  SELECT i_txt, (pg_relation_size(i_txt::regclass)/8192 - 1)::int AS maxblk
  FROM idx
)
SELECT min(blkno) AS deadblk
FROM sz, LATERAL generate_series(1, maxblk) AS blkno
WHERE (bt_page_stats(i_txt, blkno)).type = 'l'
  AND (bt_page_stats(i_txt, blkno)).dead_items > 0
\gset

\echo '== LP_DEAD index items on the first such leaf page =='
SELECT itemoffset, dead, htid, itemlen, data
FROM bt_page_items('public.lpdead_demo_k_idx'::text, :deadblk)
WHERE dead IS TRUE
LIMIT 20;

-- ============================================================
-- ACTUAL OUTPUT (PostgreSQL 17.10 on aarch64-apple-darwin)
-- ============================================================
-- DELETE 30000
-- (forced index scan returns count = 270000, and sets LP_DEAD bits)
--
-- == Leaf pages carrying the most LP_DEAD items ==
--  blkno | live | dead
-- -------+------+------
--     28 |  330 |   37
--     33 |  330 |   37
--     25 |  330 |   37
--    ... (every leaf page that held a deleted key now has dead_items=37)
--
-- == LP_DEAD index items on the first such leaf page ==
--  itemoffset | dead |  htid  | itemlen |          data
-- ------------+------+--------+---------+-------------------------
--          11 | t    | (0,10) |      16 | 0a 00 00 00 00 00 00 00
--          21 | t    | (0,20) |      16 | 14 00 00 00 00 00 00 00
--          31 | t    | (0,30) |      16 | 1e 00 00 00 00 00 00 00
--         ... (dead='t' => future scans skip these without visiting the heap)
--
-- Note: LP_DEAD is set on the PRIMARY by scans that observe the dead heap
-- tuples. A hot standby cannot set these bits from its own queries the same way
-- (it must honor the primary's horizon and its own older snapshots), so an
-- identical index scan can do more heap fetches on a replica than on the
-- primary until the primary's cleanup is replayed.
