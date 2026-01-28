Reproducible SQL benchmarks for PostgreSQL deletion techniques.

- Main script: `tests.sql`
- What it measures: runtime, WAL delta/volume, dead tuples (and optionally replica lag)

Run:
- `psql -f tests.sql`

There are also examples about different DML based deletion techniques that you can use in production.


batched_delete_with_cte.sql and delete_staged_keysdata.sql are the examples how you can perform UPDATE/DELETEs at scale with controlling vacuum behavior.

vm_regression and LP_DEAD files are mostly tests to show impact of visibility map on Index Only Scans and LP_DEAD hint bit impact on the queries that runs on master and replica differently.
