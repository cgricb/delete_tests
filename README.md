# PostgreSQL deletion techniques: benchmark + patterns

Reproducible SQL for comparing large `DELETE`/`UPDATE` strategies in PostgreSQL,
plus two production-ready batching patterns and two diagnostic demos.
Results below were regenerated on PostgreSQL 17.10 (Apple Silicon).

## Files

| File | Purpose |
|------|---------|
| `tests.sql` | Benchmark harness comparing four deletion strategies on a 10M-row table. |
| `batched_delete_with_cte.sql` | Production pattern: batched `DELETE` with controlled autovacuum. |
| `delete_staged_keysdata.sql` | Production pattern: batched `UPDATE` driven by a staged key table. |
| `vm_regression.sql` | Demo: Visibility Map churn breaking Index-Only Scans. |
| `LP_DEAD.sql` | Demo: LP_DEAD ("known dead") index hint bits via `pageinspect`. |

## Benchmark (`tests.sql`)

Builds a 1M-key join table and a 10M-row target table (~200-byte payload, PK + 2
secondary indexes), then deletes the same ~9% of rows four different ways, each
time from an identical, freshly restored and vacuumed table.

Methods:
- **single-shot** `DELETE` (one statement)
- **keyset batching** (commit per batch, high-water PK cursor)
- **ctid batching** (commit per batch, high-water `ctid` cursor)
- **staged keys** (stage victims into an `UNLOGGED` table once, then delete by PK join)

Measured per method:
- `active_sec` - work only (`DELETE` + `COMMIT`); the `pg_sleep` throttle is **excluded**
- `wall_sec` - total wall time **including** the throttle
- `wal_gb` - WAL generated (cluster LSN diff)
- `dead_tuples_after` - exact post-op dead tuples via `pgstattuple`

### Run

```sh
psql -f tests.sql                                                                   # default 10M-row scale
psql -v join_keys=50000 -v base_rows=500000 -v batch=50000 -v sleep=0 -f tests.sql  # quick check
```

For trustworthy WAL numbers (avoids checkpoint full-page-image noise polluting the
cluster LSN diff). Each `ALTER SYSTEM` must be its own statement:

```sql
ALTER SYSTEM SET full_page_writes = off;
ALTER SYSTEM SET max_wal_size     = '64GB';
SELECT pg_reload_conf();
```

### Results (PostgreSQL 17.10, 10M rows, batch 200k, sleep 1s)

| method | active_s | wall_s | wal_gb | dead_tuples_after |
|--------|---------:|-------:|-------:|------------------:|
| single-shot delete | 2.00 | 2.00 | 0.047 | 914,459 |
| staged keys        | 6.00 | 11.00 | 0.064 | 114,460 |
| keyset batching    | 10.04 | 15.04 | 0.064 | 114,459 |
| ctid batching      | 10.79 | 15.80 | 0.060 | 314,455 |

Takeaways:
- Single-shot wins raw throughput - but that is not the point of batching.
- `wall - active` is exactly the `pg_sleep` throttle (5 batches x 1s); never read it as "slowness".
- `dead_tuples_after` is the revealing column: single-shot leaves **all** deleted rows
  dead until a later `VACUUM`, whereas batched methods commit per batch - advancing the
  xmin horizon so subsequent batches opportunistically prune most dead tuples mid-run.
  (ctid prunes less because its forward-only walk never revisits earlier pages.)
- Batching trades throughput for bounded lock-hold time, replication lag, and WAL burst
  rate - operational properties, not speed.

## Production patterns

`batched_delete_with_cte.sql` (batched `DELETE`) and `delete_staged_keysdata.sql`
(batched `UPDATE` from a staged key table) show how to run large DML at scale while
controlling vacuum behavior. Both:
- commit per batch (release locks, flush WAL incrementally, and advance the xmin horizon
  so autovacuum can actually reclaim the dead tuples the DML creates);
- temporarily tune the target table's autovacuum reloptions, then `RESET` them;
- use `SET LOCAL lock_timeout` to fail fast instead of queueing behind a blocking lock.

Note: because they `COMMIT` per batch they cannot wrap the loop in a PL/pgSQL `EXCEPTION`
handler, so if they abort mid-run the reloptions stay applied and must be reset manually -
the exact `ALTER TABLE ... RESET` is documented at the top of each file.

## Diagnostic demos

- `vm_regression.sql` - freezes a table to 100% all-visible (Index-Only Scan with
  `Heap Fetches: 0`), churns 10% of rows so all-visible drops to 0% (heap fetches jump to
  ~55k, buffers ~9x), then `VACUUM` restores it. Shows why churn silently regresses
  Index-Only Scans. Even a no-op `SET pad = pad` creates new row versions and clears
  all-visible.
- `LP_DEAD.sql` - deletes ~10% of rows, then an index scan marks the dead index entries
  LP_DEAD (`pageinspect` shows `dead='t'`). A hot standby cannot set these bits from its
  own queries the same way, so the same scan can do more heap fetches on a replica until
  the primary's cleanup is replayed.

## Caveats

Single-laptop runs (no repetitions/median); macOS fsync/checkpoint behavior is not
production-representative; WAL is measured with `full_page_writes=off` to isolate
intrinsic record WAL (production `FPW=on` adds the same method-independent full-page-image
cost). Per-batch `COMMIT` fsync and `synchronous_commit`/sync-replica ack costs - the real
things batching trades against - are not measured here.
