DO
$cleanup$
DECLARE
    v_batch_size                int := 200000;
    v_batch_counter             int := 1;
    v_av_threshold              int := v_batch_size * 10;
    v_sleep_between_batches     int := 5;

    v_last_key_deleted          bigint;
    v_num_rows_deleted          int;
BEGIN
    -- Temporarily tune autovacuum for aggressive churn on the target table.
    -- NOTE: ALTER TABLE ... SET is persistent until RESET is executed.
    EXECUTE format($sql$
        ALTER TABLE public.target_table SET (
            autovacuum_vacuum_scale_factor  = 0.0,
            autovacuum_vacuum_threshold     = %s,
            autovacuum_analyze_scale_factor = 0.0,
            autovacuum_analyze_threshold    = %s
        )
    $sql$, v_av_threshold, v_av_threshold);

    COMMIT;

    RAISE NOTICE 'Will clean up target_table';
    CHECKPOINT;

    WHILE TRUE LOOP
        WITH keys_to_delete AS (
            SELECT
                t.pk_id
            FROM public.target_table t
            WHERE
                t.closed_at < now() - interval '2 months'
                AND t.metric_a = 0
                AND t.metric_b = 0
                AND t.metric_c = 0
            LIMIT v_batch_size
        ),
        deleted AS (
            DELETE FROM public.target_table tt
            USING keys_to_delete k
            WHERE tt.pk_id = k.pk_id
            RETURNING tt.pk_id
        )
        SELECT
            max(pk_id),
            count(*)
        INTO
            v_last_key_deleted,
            v_num_rows_deleted
        FROM deleted;

        COMMIT;

        IF v_num_rows_deleted > 0 THEN
            RAISE NOTICE '%: deleted % rows up to key %',
                v_batch_counter, v_num_rows_deleted, v_last_key_deleted;

            IF v_batch_counter % 50 = 0 THEN
                RAISE NOTICE 'Running checkpoint';
                CHECKPOINT;
            END IF;

            v_batch_counter := v_batch_counter + 1;

            RAISE NOTICE 'Waiting % seconds before next batch', v_sleep_between_batches;
            PERFORM pg_sleep(v_sleep_between_batches);
        ELSE
            RAISE NOTICE 'Nothing deleted in last batch, exiting';

            ALTER TABLE public.target_table RESET (
                autovacuum_vacuum_scale_factor,
                autovacuum_vacuum_threshold,
                autovacuum_analyze_scale_factor,
                autovacuum_analyze_threshold
            );

            EXIT;
        END IF;
    END LOOP;
END
$cleanup$;
