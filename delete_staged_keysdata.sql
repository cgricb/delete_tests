CREATE OR REPLACE PROCEDURE public.delete_target_from_source()
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_size        int := 1000000;
    v_batch_num         int := 0;
    v_rows_updated      int;
    v_total_updated     bigint := 0;

    v_start_ts          timestamptz;
    v_end_ts            timestamptz;
    v_elapsed           interval;

    v_cutoff_date       date := DATE '2020-04-01';

    v_candidate_count   bigint;
BEGIN
    ----------------------------------------------------------------------
    -- 0) Preflight (share-safe)
    ----------------------------------------------------------------------
    SELECT count(*)
      INTO v_candidate_count
    FROM public.target_table t
    WHERE t.delete_col_1 IS NULL
      AND t.created_at > v_cutoff_date;

    RAISE NOTICE 'Preflight: candidate rows = % (cutoff=%)', v_candidate_count, v_cutoff_date;

    ----------------------------------------------------------------------
    -- 1) TEMPORARY autovacuum tuning for the target table (PERSISTENT!)
    --    These are table reloptions (ALTER TABLE ... SET), not session SETs.
    --    They will remain until reset, so we RESET them at the end.
    ----------------------------------------------------------------------
    ALTER TABLE public.target_table
      SET (
        -- more frequent vacuum/analyze during heavy churn
        autovacuum_vacuum_scale_factor  = 0.01,
        autovacuum_analyze_scale_factor = 0.02,

        -- don’t wait for tiny tables; for large tables this matters less,
        -- but thresholds help avoid “too sparse” trigger behavior
        autovacuum_vacuum_threshold     = 50000,
        autovacuum_analyze_threshold    = 50000,

        -- let autovacuum work a bit harder per cycle very optional based on the resources
        --autovacuum_vacuum_cost_limit    = 2000,
        -- autovacuum_vacuum_cost_delay    = 2
      );

    RAISE NOTICE 'Autovacuum reloptions applied to target_table';

    ----------------------------------------------------------------------
    -- 2) Build temp driving table (stable batching)
    ----------------------------------------------------------------------
    CREATE TEMP TABLE IF NOT EXISTS tmp_drive (
        id   bigserial PRIMARY KEY,
        txid bigint NOT NULL
    ) ON COMMIT DROP;

    TRUNCATE tmp_drive;

    INSERT INTO tmp_drive (txid)
    SELECT t.fk_txid
    FROM public.target_table t
    WHERE t.delete_col_1 IS NULL
      AND t.created_at > v_cutoff_date;

    RAISE NOTICE 'Temp drive table populated: % rows', (SELECT count(*) FROM tmp_drive);

    ----------------------------------------------------------------------
    -- 3) Batched update loop
    ----------------------------------------------------------------------
    LOOP
        v_start_ts := clock_timestamp();

        UPDATE public.target_table tgt
        SET
            delete_col_1 = src.src_col_a,
            delete_col_2 = src.src_col_b,
            delete_col_3 = src.src_col_c,
            delete_col_4 = src.src_col_d
        FROM public.source_table src
        JOIN tmp_drive d
          ON src.txid = d.txid
         AND d.id >  v_batch_num * v_batch_size
         AND d.id <= (v_batch_num + 1) * v_batch_size
        WHERE src.txid = tgt.fk_txid
          AND tgt.created_at > v_cutoff_date
          AND tgt.delete_col_1 IS NULL;

        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

        v_end_ts := clock_timestamp();
        v_elapsed := v_end_ts - v_start_ts;

        v_total_updated := v_total_updated + v_rows_updated;

        RAISE NOTICE 'Batch % updated: % rows affected, time taken: %',
            v_batch_num, v_rows_updated, v_elapsed;

        EXIT WHEN v_rows_updated = 0;

        v_batch_num := v_batch_num + 1;
        PERFORM pg_sleep(1);
    END LOOP;

    RAISE NOTICE 'All updates completed: % rows affected in total', v_total_updated;

    ----------------------------------------------------------------------
    -- 4) Reset autovacuum reloptions back to defaults
    ----------------------------------------------------------------------
    ALTER TABLE public.target_table
      RESET (
        autovacuum_vacuum_scale_factor,
        autovacuum_analyze_scale_factor,
        autovacuum_vacuum_threshold,
        autovacuum_analyze_threshold,
        autovacuum_vacuum_cost_limit,
        autovacuum_vacuum_cost_delay
      );

    RAISE NOTICE 'Autovacuum reloptions reset to defaults';

END;
$$;
