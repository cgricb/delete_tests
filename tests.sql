-- Reproducible SQL benchmark for PostgreSQL deletion techniques.
-- Regenerated on PostgreSQL 17.10 on aarch64-apple-darwin (Apple Silicon laptop).
-- Original recorded environment: PostgreSQL 17.7, full_page_writes=on, max_wal_size=20G.
-- This regenerated run used full_page_writes=off and max_wal_size=64GB so the WAL
-- metric isolates intrinsic record WAL (see the note above section 7); actual
-- results are pasted at the very bottom of this file.
--
-- Scale is configurable (defaults reproduce the original 10M-row run); override:
--   psql -v join_keys=50000 -v base_rows=500000 -v batch=50000 -v sleep=0 -f tests.sql
--
-- What it measures per method:
--   active_seconds  - work time (DELETE + COMMIT); pg_sleep throttle EXCLUDED
--   wall_seconds    - total wall time INCLUDING the pg_sleep throttle
--   wal_bytes_delta - cluster WAL LSN advance during the op; with full_page_writes
--                     off + large max_wal_size this approximates intrinsic record WAL
--   dead_tuples_after - accurate post-op dead tuples via pgstattuple (full scan)

\if :{?join_keys}
\else
  \set join_keys 1000000
\endif
\if :{?base_rows}
\else
  \set base_rows 10000000
\endif
\if :{?batch}
\else
  \set batch 200000
\endif
\if :{?sleep}
\else
  \set sleep 1
\endif

CREATE EXTENSION IF NOT EXISTS pgstattuple;

DROP TABLE IF EXISTS batchbalance_base CASCADE;
DROP TABLE IF EXISTS join_table CASCADE;

CREATE TABLE join_table (
  join_column bigint PRIMARY KEY,
  column_to_delete boolean NOT NULL
);

-- join keys, 30% marked for deletion
INSERT INTO join_table
SELECT gs, (random() < 0.30)
FROM generate_series(1, :join_keys) gs;

CREATE TABLE batchbalance_base (
  batchid bigint PRIMARY KEY,
  join_column bigint NOT NULL,
  closeddate timestamptz NOT NULL,
  preopeningbalance int NOT NULL,
  preopeningauthbalance int NOT NULL,
  preopeningblockedbalance int NOT NULL,
  payload bytea NOT NULL
);

-- Load base rows
INSERT INTO batchbalance_base
SELECT
  gs AS batchid,
  1 + (random()*(:join_keys - 1))::bigint,
  now() - ((random()*120)::int * interval '1 day'),
  CASE WHEN random() < 0.85 THEN 0 ELSE (random()*100)::int END,
  CASE WHEN random() < 0.85 THEN 0 ELSE (random()*100)::int END,
  CASE WHEN random() < 0.85 THEN 0 ELSE (random()*100)::int END,
  decode(repeat('ab', 200), 'hex')  -- ~200 bytes; tune
FROM generate_series(1, :base_rows) gs;

-- Indexes (important for fairness)
CREATE INDEX batchbalance_base_closeddate_idx ON batchbalance_base (closeddate);
CREATE INDEX batchbalance_base_join_idx      ON batchbalance_base (join_column);

VACUUM (ANALYZE) join_table;
VACUUM (ANALYZE) batchbalance_base;

-- ============================================================
-- 0) Results table
-- ============================================================
DROP TABLE IF EXISTS public.delete_bench_results;

CREATE TABLE public.delete_bench_results (
  run_ts            timestamptz DEFAULT now(),
  method            text NOT NULL,
  batch_size        int,
  sleep_seconds     int,
  rows_deleted      bigint NOT NULL,
  active_seconds    numeric NOT NULL,   -- DELETE + COMMIT only (no sleep)
  wall_seconds      numeric NOT NULL,   -- includes pg_sleep throttle
  wal_bytes_delta   numeric NOT NULL,
  wal_lsn_before    pg_lsn NOT NULL,
  wal_lsn_after     pg_lsn NOT NULL,
  dead_tuples_after bigint NOT NULL
);

-- ============================================================
-- 1) Helpers
-- ============================================================
-- Accurate dead-tuple count (pg_stat_user_tables.n_dead_tup lags and is
-- updated asynchronously; pgstattuple does a full scan but is exact).
CREATE OR REPLACE FUNCTION public.bench_dead_tuples() RETURNS bigint
LANGUAGE sql AS $$
  SELECT (pgstattuple('public.batchbalance')).dead_tuple_count::bigint
$$;

-- ============================================================
-- 2) Restore procedure (NO VACUUM inside; run VACUUM outside)
--    Assumes batchbalance_base exists and is loaded once.
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_restore;

CREATE OR REPLACE PROCEDURE public.bench_restore()
LANGUAGE plpgsql
AS $$
BEGIN
  DROP TABLE IF EXISTS public.batchbalance;

  CREATE TABLE public.batchbalance AS
  TABLE public.batchbalance_base;

  CREATE INDEX batchbalance_closeddate_idx ON public.batchbalance (closeddate);
  CREATE INDEX batchbalance_join_idx      ON public.batchbalance (join_column);
  ALTER TABLE public.batchbalance ADD PRIMARY KEY (batchid);

  -- NOTE: VACUUM cannot be executed inside stored code.
  -- After calling bench_restore(), do:
  --   VACUUM (ANALYZE) public.batchbalance;
END $$;

-- ============================================================
-- 3) Method A: Single-shot DELETE
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_delete_singleshot;

CREATE OR REPLACE PROCEDURE public.bench_delete_singleshot()
LANGUAGE plpgsql
AS $$
DECLARE
  v_start timestamptz;
  v_deleted bigint;
  v_elapsed numeric;
  lsn0 pg_lsn;
  lsn1 pg_lsn;
  wal_delta numeric;
BEGIN
  lsn0 := pg_current_wal_lsn();
  v_start := clock_timestamp();

  WITH deleted AS (
    DELETE FROM public.batchbalance b
    USING public.join_table j
    WHERE b.join_column = j.join_column
      AND j.column_to_delete = true
      AND b.closeddate < now() - interval '2 months'
      AND b.preopeningbalance = 0
      AND b.preopeningauthbalance = 0
      AND b.preopeningblockedbalance = 0
    RETURNING 1
  )
  SELECT count(*) INTO v_deleted FROM deleted;

  v_elapsed := EXTRACT(epoch FROM clock_timestamp() - v_start);
  lsn1 := pg_current_wal_lsn();
  wal_delta := pg_wal_lsn_diff(lsn1, lsn0);

  -- single statement, no throttle: active == wall
  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,active_seconds,wall_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('single-shot delete', NULL, NULL, v_deleted, v_elapsed, v_elapsed, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 4) Method B: Keyset batching (commits per batch)
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_delete_keyset;

CREATE OR REPLACE PROCEDURE public.bench_delete_keyset(p_batch int, p_sleep int)
LANGUAGE plpgsql
AS $$
DECLARE
  v_last bigint := 0;
  v_rows int;
  v_total bigint := 0;
  v_active numeric := 0;
  v_wall numeric;
  v_iter_start timestamptz;
  v_wall_start timestamptz;
  lsn0 pg_lsn;
  lsn1 pg_lsn;
  wal_delta numeric;
  v_next_last bigint;
BEGIN
  lsn0 := pg_current_wal_lsn();
  v_wall_start := clock_timestamp();

  LOOP
    v_iter_start := clock_timestamp();

    WITH batch AS (
      SELECT b.batchid
      FROM public.batchbalance b
      JOIN public.join_table j ON b.join_column = j.join_column
      WHERE j.column_to_delete = true
        AND b.closeddate < now() - interval '2 months'
        AND b.preopeningbalance = 0
        AND b.preopeningauthbalance = 0
        AND b.preopeningblockedbalance = 0
        AND b.batchid > v_last
      ORDER BY b.batchid
      LIMIT p_batch
    ),
    del AS (
      DELETE FROM public.batchbalance b
      USING batch x
      WHERE b.batchid = x.batchid
      RETURNING b.batchid
    )
    SELECT count(*), max(batchid)
    INTO v_rows, v_next_last
    FROM del;

    IF v_rows = 0 THEN
      v_active := v_active + EXTRACT(epoch FROM clock_timestamp() - v_iter_start);
      EXIT;
    END IF;

    v_total := v_total + v_rows;
    v_last := COALESCE(v_next_last, v_last);

    COMMIT;
    -- count DELETE + COMMIT as active work; the throttle below is excluded
    v_active := v_active + EXTRACT(epoch FROM clock_timestamp() - v_iter_start);
    PERFORM pg_sleep(p_sleep);
  END LOOP;

  v_wall := EXTRACT(epoch FROM clock_timestamp() - v_wall_start);
  lsn1 := pg_current_wal_lsn();
  wal_delta := pg_wal_lsn_diff(lsn1, lsn0);

  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,active_seconds,wall_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('keyset batching', p_batch, p_sleep, v_total, v_active, v_wall, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 5) Method C: CTID batching
--    FIX vs original: a high-water ctid cursor (b.ctid > v_last_ctid) makes the
--    scan walk the heap forward in physical order instead of re-scanning and
--    re-sorting all matching rows every iteration (the original had no cursor,
--    which was quadratic-ish and produced a misleadingly slow result).
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_delete_ctid;

CREATE OR REPLACE PROCEDURE public.bench_delete_ctid(p_batch int, p_sleep int)
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows int;
  v_total bigint := 0;
  v_active numeric := 0;
  v_wall numeric;
  v_iter_start timestamptz;
  v_wall_start timestamptz;
  lsn0 pg_lsn;
  lsn1 pg_lsn;
  wal_delta numeric;
  v_last_ctid tid := '(0,0)';
  v_next_last tid;
BEGIN
  lsn0 := pg_current_wal_lsn();
  v_wall_start := clock_timestamp();

  LOOP
    v_iter_start := clock_timestamp();

    WITH batch AS (
      SELECT b.ctid AS tid
      FROM public.batchbalance b
      JOIN public.join_table j ON b.join_column = j.join_column
      WHERE j.column_to_delete = true
        AND b.closeddate < now() - interval '2 months'
        AND b.preopeningbalance = 0
        AND b.preopeningauthbalance = 0
        AND b.preopeningblockedbalance = 0
        AND b.ctid > v_last_ctid
      ORDER BY b.ctid
      LIMIT p_batch
    ),
    del AS (
      DELETE FROM public.batchbalance b
      USING batch x
      WHERE b.ctid = x.tid
      RETURNING b.ctid
    )
    SELECT count(*), max(ctid) INTO v_rows, v_next_last FROM del;

    IF v_rows = 0 THEN
      v_active := v_active + EXTRACT(epoch FROM clock_timestamp() - v_iter_start);
      EXIT;
    END IF;

    v_total := v_total + v_rows;
    v_last_ctid := COALESCE(v_next_last, v_last_ctid);

    COMMIT;
    v_active := v_active + EXTRACT(epoch FROM clock_timestamp() - v_iter_start);
    PERFORM pg_sleep(p_sleep);
  END LOOP;

  v_wall := EXTRACT(epoch FROM clock_timestamp() - v_wall_start);
  lsn1 := pg_current_wal_lsn();
  wal_delta := pg_wal_lsn_diff(lsn1, lsn0);

  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,active_seconds,wall_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('ctid batching', p_batch, p_sleep, v_total, v_active, v_wall, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 6) Method D: Staged keys -> join delete
--    Stage victims once into UNLOGGED del_ids (no WAL for the staging table),
--    then batch-delete by PK join (cheap), avoiding re-running the multi-table
--    predicate join every batch.
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_delete_staged;

CREATE OR REPLACE PROCEDURE public.bench_delete_staged(p_batch int, p_sleep int)
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows int;
  v_total bigint := 0;
  v_active numeric := 0;
  v_wall numeric;
  v_iter_start timestamptz;
  v_wall_start timestamptz;
  lsn0 pg_lsn;
  lsn1 pg_lsn;
  wal_delta numeric;
BEGIN
  -- Stage keys (unlogged) - staging itself does NOT generate WAL like logged tables do.
  DROP TABLE IF EXISTS public.del_ids;
  CREATE UNLOGGED TABLE public.del_ids(batchid bigint PRIMARY KEY);

  INSERT INTO public.del_ids
  SELECT b.batchid
  FROM public.batchbalance b
  JOIN public.join_table j ON b.join_column = j.join_column
  WHERE j.column_to_delete = true
    AND b.closeddate < now() - interval '2 months'
    AND b.preopeningbalance = 0
    AND b.preopeningauthbalance = 0
    AND b.preopeningblockedbalance = 0;

  ANALYZE public.del_ids;

  lsn0 := pg_current_wal_lsn();
  v_wall_start := clock_timestamp();

  LOOP
    v_iter_start := clock_timestamp();

    WITH batch AS (
      SELECT batchid
      FROM public.del_ids
      ORDER BY batchid
      LIMIT p_batch
    ),
    deleted AS (
      DELETE FROM public.batchbalance b
      USING batch x
      WHERE b.batchid = x.batchid
      RETURNING b.batchid
    ),
    gone AS (
      DELETE FROM public.del_ids d
      USING deleted x
      WHERE d.batchid = x.batchid
      RETURNING 1
    )
    SELECT count(*) INTO v_rows FROM gone;

    IF v_rows = 0 THEN
      v_active := v_active + EXTRACT(epoch FROM clock_timestamp() - v_iter_start);
      EXIT;
    END IF;

    v_total := v_total + v_rows;

    COMMIT;
    v_active := v_active + EXTRACT(epoch FROM clock_timestamp() - v_iter_start);
    PERFORM pg_sleep(p_sleep);
  END LOOP;

  v_wall := EXTRACT(epoch FROM clock_timestamp() - v_wall_start);
  lsn1 := pg_current_wal_lsn();
  wal_delta := pg_wal_lsn_diff(lsn1, lsn0);

  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,active_seconds,wall_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('staged keys', p_batch, p_sleep, v_total, v_active, v_wall, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 7) Run all methods (each starts from a freshly restored, vacuumed table)
-- ============================================================
-- WAL is captured as a cluster-wide LSN diff, so checkpoint-driven full-page
-- images (FPIs) would otherwise pollute it - and an FPI storm landing in one
-- method's window is a pure artifact, not that method's cost. Two safeguards:
--   1) full_page_writes=off  -> WAL is record-only and checkpoint-independent
--   2) large max_wal_size    -> no size-triggered checkpoint fires mid-suite
-- (FPI volume is method-independent anyway: it depends on pages touched, not on
-- how you batch.) The CHECKPOINT below drains WAL once so all methods start from
-- the same baseline.
--
-- Recommended run (each ALTER SYSTEM must be its own statement - ALTER SYSTEM
-- cannot run inside a multi-statement transaction block):
--   ALTER SYSTEM SET full_page_writes = off;
--   ALTER SYSTEM SET max_wal_size     = '64GB';
--   SELECT pg_reload_conf();
CHECKPOINT;

CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_singleshot();

CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_keyset(:batch, :sleep);

CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_ctid(:batch, :sleep);

CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_staged(:batch, :sleep);

-- ============================================================
-- 8) Report
-- ============================================================
SELECT method, batch_size, rows_deleted,
       round(active_seconds,2) AS active_sec,
       round(wall_seconds,2)   AS wall_sec,
       round(wal_bytes_delta/1024/1024/1024, 3) AS wal_gb,
       dead_tuples_after
FROM public.delete_bench_results
ORDER BY run_ts DESC;

-- ============================================================
-- INTERPRETATION
-- ============================================================
-- * active_sec is the honest work metric: single-shot has no throttle, so its
--   active==wall. For batched methods, wall_sec - active_sec is pure pg_sleep
--   throttle and must NOT be read as "slowness".
-- * Single-shot wins raw active time - expected, and not the point. Batching
--   trades throughput for bounded lock-hold time, replication lag and WAL burst
--   rate. Those operational benefits (and per-batch COMMIT fsync / sync-replica
--   ack costs, the real thing batching trades against) are NOT measured here.
-- * dead_tuples_after is the most revealing column and is NOT equal: single-shot
--   leaves ALL deleted rows dead (one transaction, nothing reclaimed until a
--   later VACUUM), whereas the batched methods leave far fewer. Committing each
--   batch advances the xmin horizon, so subsequent batches' page accesses
--   opportunistically prune (heap_page_prune) the now-dead tuples mid-run. This
--   is a concrete bloat-control benefit of batch+commit, even before autovacuum.
--   ctid batching prunes less than keyset/staged because its forward-only heap
--   walk never revisits earlier pages, so more dead tuples linger.
-- * ctid batching is now bounded by the high-water ctid cursor (was quadratic).
-- * staged keys pays the predicate-join cost once, then deletes via cheap PK
--   join - the best active time among the batched methods.
-- * Caveats: single laptop run (no repetitions/median); macOS fsync/checkpoint
--   behavior is not production-representative; WAL is a cluster LSN diff measured
--   with full_page_writes=off to isolate record WAL (production FPW=on adds the
--   same method-independent full-page-image cost on top).

-- ============================================================
-- ACTUAL RESULTS (regenerated)
-- ============================================================
-- PostgreSQL 17.10 on aarch64-apple-darwin (Apple Silicon laptop)
-- Settings: full_page_writes=off, max_wal_size=64GB (see note above section 7)
-- Scale: join_keys=1,000,000  base_rows=10,000,000  batch=200000  sleep=1
--
--        method       | batch_size | rows_deleted | active_sec | wall_sec | wal_gb | dead_tuples_after
-- -------------------+------------+--------------+------------+----------+--------+-------------------
--  staged keys        |     200000 |       914459 |       6.00 |    11.00 |  0.064 |            114460
--  ctid batching      |     200000 |       914459 |      10.79 |    15.80 |  0.060 |            314455
--  keyset batching    |     200000 |       914459 |      10.04 |    15.04 |  0.064 |            114459
--  single-shot delete |            |       914459 |       2.00 |     2.00 |  0.047 |            914459
--
-- Reading it:
--  - single-shot is fastest (2.0s) and lowest WAL, but leaves all 914,459 rows
--    dead until a later VACUUM.
--  - batched methods cost more active time but, by committing per batch, let
--    mid-run pruning reclaim most dead tuples (staged/keyset down to ~114k).
--  - wall_sec - active_sec is exactly the 5 x 1s pg_sleep throttle (5 batches).
--  - ctid batching is no longer pathological (10.8s) now that it uses a
--    high-water ctid cursor instead of re-scanning every iteration.
