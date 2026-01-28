PostgreSQL 17.7 on aarch64-apple-darwin21.6.0
full_page_writes=on
max_wal_size=20G

DROP TABLE IF EXISTS batchbalance_base CASCADE;
DROP TABLE IF EXISTS join_table CASCADE;

CREATE TABLE join_table (
  join_column bigint PRIMARY KEY,
  column_to_delete boolean NOT NULL
);

-- 1M join keys, 30% marked for deletion
INSERT INTO join_table
SELECT gs, (random() < 0.30)
FROM generate_series(1, 1000000) gs;

CREATE TABLE batchbalance_base (
  batchid bigint PRIMARY KEY,
  join_column bigint NOT NULL,
  closeddate timestamptz NOT NULL,
  preopeningbalance int NOT NULL,
  preopeningauthbalance int NOT NULL,
  preopeningblockedbalance int NOT NULL,
  payload bytea NOT NULL
);

-- Load 10M rows
INSERT INTO batchbalance_base
SELECT
  gs AS batchid,
  1 + (random()*999999)::bigint,
  now() - ((random()*120)::int * interval '1 day'),
  CASE WHEN random() < 0.85 THEN 0 ELSE (random()*100)::int END,
  CASE WHEN random() < 0.85 THEN 0 ELSE (random()*100)::int END,
  CASE WHEN random() < 0.85 THEN 0 ELSE (random()*100)::int END,
  decode(repeat('ab', 200), 'hex')  -- ~200 bytes; tune
FROM generate_series(1, 10000000) gs;

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
  elapsed_seconds   numeric NOT NULL,
  wal_bytes_delta   numeric NOT NULL,
  wal_lsn_before    pg_lsn NOT NULL,
  wal_lsn_after     pg_lsn NOT NULL,
  dead_tuples_after bigint NOT NULL
);

-- ============================================================
-- 1) Helpers
-- ============================================================
CREATE OR REPLACE FUNCTION public.bench_dead_tuples() RETURNS bigint
LANGUAGE sql AS $$
  SELECT COALESCE(n_dead_tup, 0)
  FROM pg_stat_user_tables
  WHERE relname = 'batchbalance'
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

  -- Recreate indexes (adjust to match your real schema/indexes)
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

  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,elapsed_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('single-shot delete', NULL, NULL, v_deleted, v_elapsed, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 4) Method B: Keyset batching
--     - commits per batch
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_delete_keyset;

CREATE OR REPLACE PROCEDURE public.bench_delete_keyset(p_batch int, p_sleep int)
LANGUAGE plpgsql
AS $$
DECLARE
  v_last bigint := 0;
  v_rows int;
  v_total bigint := 0;

  v_start timestamptz;
  v_elapsed numeric;

  lsn0 pg_lsn;
  lsn1 pg_lsn;
  wal_delta numeric;

  v_next_last bigint;
BEGIN
  lsn0 := pg_current_wal_lsn();
  v_start := clock_timestamp();

  LOOP
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

    EXIT WHEN v_rows = 0;

    v_total := v_total + v_rows;
    v_last := COALESCE(v_next_last, v_last);

    COMMIT;
    PERFORM pg_sleep(p_sleep);
    -- next statements run in a new implicit transaction
  END LOOP;

  v_elapsed := EXTRACT(epoch FROM clock_timestamp() - v_start);
  lsn1 := pg_current_wal_lsn();
  wal_delta := pg_wal_lsn_diff(lsn1, lsn0);

  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,elapsed_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('keyset batching', p_batch, p_sleep, v_total, v_elapsed, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 5) Method C: CTID batching
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_delete_ctid;

CREATE OR REPLACE PROCEDURE public.bench_delete_ctid(p_batch int, p_sleep int)
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows int;
  v_total bigint := 0;

  v_start timestamptz;
  v_elapsed numeric;

  lsn0 pg_lsn;
  lsn1 pg_lsn;
  wal_delta numeric;
BEGIN
  lsn0 := pg_current_wal_lsn();
  v_start := clock_timestamp();

  LOOP
    WITH batch AS (
      SELECT b.ctid
      FROM public.batchbalance b
      JOIN public.join_table j ON b.join_column = j.join_column
      WHERE j.column_to_delete = true
        AND b.closeddate < now() - interval '2 months'
        AND b.preopeningbalance = 0
        AND b.preopeningauthbalance = 0
        AND b.preopeningblockedbalance = 0
      ORDER BY b.closeddate, b.batchid
      LIMIT p_batch
    ),
    del AS (
      DELETE FROM public.batchbalance b
      USING batch x
      WHERE b.ctid = x.ctid
      RETURNING 1
    )
    SELECT count(*) INTO v_rows FROM del;

    EXIT WHEN v_rows = 0;

    v_total := v_total + v_rows;

    COMMIT;
    PERFORM pg_sleep(p_sleep);
  END LOOP;

  v_elapsed := EXTRACT(epoch FROM clock_timestamp() - v_start);
  lsn1 := pg_current_wal_lsn();
  wal_delta := pg_wal_lsn_diff(lsn1, lsn0);

  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,elapsed_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('ctid batching', p_batch, p_sleep, v_total, v_elapsed, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 6) Method D: Staged keys -> join delete
--     - Stage victims once into UNLOGGED table del_ids
-- ============================================================
DROP PROCEDURE IF EXISTS public.bench_delete_staged;

CREATE OR REPLACE PROCEDURE public.bench_delete_staged(p_batch int, p_sleep int)
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows int;
  v_total bigint := 0;

  v_start timestamptz;
  v_elapsed numeric;

  lsn0 pg_lsn;
  lsn1 pg_lsn;
  wal_delta numeric;
BEGIN
  -- Stage keys (unlogged) - this staging itself does NOT generate WAL like logged tables do.
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
  v_start := clock_timestamp();

  LOOP
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

    EXIT WHEN v_rows = 0;

    v_total := v_total + v_rows;

    COMMIT;
    PERFORM pg_sleep(p_sleep);
  END LOOP;

  v_elapsed := EXTRACT(epoch FROM clock_timestamp() - v_start);
  lsn1 := pg_current_wal_lsn();
  wal_delta := pg_wal_lsn_diff(lsn1, lsn0);

  INSERT INTO public.delete_bench_results
    (method,batch_size,sleep_seconds,rows_deleted,elapsed_seconds,wal_bytes_delta,wal_lsn_before,wal_lsn_after,dead_tuples_after)
  VALUES
    ('staged keys', p_batch, p_sleep, v_total, v_elapsed, wal_delta, lsn0, lsn1, public.bench_dead_tuples());
END $$;

-- ============================================================
-- 7) Reporting query
-- ============================================================
-- Example:
-- SELECT method, batch_size, rows_deleted,
--        round(elapsed_seconds,2) AS sec,
--        round(wal_bytes_delta/1024/1024/1024, 3) AS wal_gb
-- FROM public.delete_bench_results
-- ORDER BY run_ts DESC;



CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_singleshot();

CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_keyset(200000, 1);

CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_ctid(200000, 1);

CALL public.bench_restore();
VACUUM (ANALYZE) public.batchbalance;
CALL public.bench_delete_staged(200000, 1);


SELECT method, batch_size, rows_deleted, round(elapsed_seconds,2) AS sec, round(wal_bytes_delta/1024/1024/1024, 3) AS wal_gb, dead_tuples_after FROM public.delete_bench_results ORDER BY run_ts DESC;


postgres=# SELECT method, batch_size, rows_deleted, round(elapsed_seconds,2) AS sec, round(wal_bytes_delta/1024/1024/1024, 3) AS wal_gb FROM public.delete_bench_results ORDER BY run_ts DESC;
       method       | batch_size | rows_deleted |  sec   | wal_gb
--------------------+------------+--------------+--------+--------
 staged keys        |     200000 |      1072613 |  35.72 |  0.087
 ctid batching      |     200000 |      1072613 | 114.33 |  0.109
 keyset batching    |     200000 |      1072613 |  44.03 |  0.087
 single-shot delete |            |      1072613 |   8.83 |  0.071
(4 rows)
